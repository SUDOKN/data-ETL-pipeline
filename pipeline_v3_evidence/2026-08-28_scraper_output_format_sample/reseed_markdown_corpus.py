#!/usr/bin/env python
"""Re-scrape the 20 golden-corpus manufacturers in markdown_v1 and publish them.

Part of the 2026-08-28 innerText -> Markdown cutover (see README.md in this
folder, and the rendering contract in scraper/utils/html_to_markdown.py).

Per subject it mirrors the production scrape bot's flow
(`new_scrape_queue_bot.get_valid_scraped_file` + the existing-manufacturer
branch), minus the extract-queue push — this reseeds texts, it does not
trigger extraction:

  1. SCRAPE  ScraperService(output_format="markdown") from https://<etld1>,
             production settings (5 browsers, depth 5). One service instance
             is reused for every subject: ChromeDriverManager pkills orphaned
             chrome/chromedriver on construction, so a fresh service per
             subject would kill the previous one's browsers.
  2. LOCAL   write the deduped text to
             apps/data_etl_app/tests/test_stages/sample_scraped_markdowns/<etld1>.txt
             (sibling of sample_scraped_texts/, same .txt convention so any
             existing glob keeps working). Written even when the scrape is
             invalid — a partial artifact is still worth inspecting.
  3. S3      ScrapedMfgFile.upload_to_s3_and_create -> a NEW VERSION of
             s3://<SCRAPED_TEXT_BUCKET>/<etld1>.txt carrying the new
             `text_format=markdown_v1` object tag. Old versions are untouched;
             reverting = deleting the new versions. The upload REFUSES an
             invalid scrape (timed out / <30 tokens / <80% success) — that
             guard is the code's, and this script does not bypass it.
  4. POINTER Mongo `manufacturer.scraped_text_file_version_id` (+
             scraped_text_file_num_tokens) -> the new version. This step is
             what actually makes extraction read the markdown text: extraction
             downloads the version NAMED BY MONGO, not S3's latest
             (`new_extract_queue_bot.py`), so an S3 upload alone would change
             nothing downstream. Every previous pointer is recorded in
             revert_pointers.json BEFORE the write, and `--revert` restores
             them.

Usage:
    python reseed_markdown_corpus.py                     # all 20, resumable
    python reseed_markdown_corpus.py --subjects a.com,b.com
    python reseed_markdown_corpus.py --no-upload --no-pointer   # local only
    python reseed_markdown_corpus.py --revert            # restore Mongo pointers
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
for _pkg in ("pure_utils", "infra", "core", "llm_providers", "scraper"):
    sys.path.insert(0, str(REPO / "packages" / _pkg / "src"))
sys.path.insert(0, str(REPO / "apps" / "data_etl_app" / "src"))

from pure_utils.env_util import load_env  # noqa: E402

load_env(["SCRAPED_TEXT_BUCKET", "MONGO_DB_URI", "CHROME_PROFILE_TMPDIR"])

from data_etl_app.db_models.manufacturer import Manufacturer  # noqa: E402
from data_etl_app.models.s3.scraped_mfg_file import ScrapedMfgFile  # noqa: E402
from infra.models.queue_items.to_scrape_item import Batch  # noqa: E402
from infra.utils.aws.clients import (  # noqa: E402
    AWSClientName,
    cleanup_aws_clients,
    initialize_aws_clients,
)
from infra.utils.db_clients.mongo_client import init_db  # noqa: E402
from llm_providers.models.llm_model import GPT_5_2  # noqa: E402  (what the bot counts tokens with)
from scraper.services.url_scraper_service import ScraperService  # noqa: E402

PINNED = REPO / "apps/data_etl_app/tests/test_stages/sample_scraped_texts"
OUT_DIR = REPO / "apps/data_etl_app/tests/test_stages/sample_scraped_markdowns"
EVIDENCE = Path(__file__).resolve().parent
RECORD_PATH = EVIDENCE / "reseed_run_record.json"
REVERT_PATH = EVIDENCE / "revert_pointers.json"

BATCH_TITLE = "markdown_v1_cutover_reseed_2026_08_28"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("reseed")
logging.getLogger("selenium").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)


def subjects_from_pinned() -> list[str]:
    return sorted(p.stem for p in PINNED.glob("*.txt"))


def load_json(path: Path) -> dict:
    return json.loads(path.read_text()) if path.exists() else {}


def save_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, indent=1, sort_keys=True))


async def revert(record_only: bool = False) -> None:
    """Restore every Mongo pointer recorded in revert_pointers.json."""
    pointers = load_json(REVERT_PATH)
    if not pointers:
        logger.error("nothing to revert: %s is empty or missing", REVERT_PATH)
        return
    client = await init_db([Manufacturer])
    try:
        for subject, prev in sorted(pointers.items()):
            mfg = await Manufacturer.find_one(Manufacturer.etld1 == subject)
            if mfg is None:
                logger.warning("%s: not in mongo, skipped", subject)
                continue
            logger.info(
                "%s: pointer %s -> %s (restored)",
                subject, mfg.scraped_text_file_version_id, prev["version_id"],
            )
            if not record_only:
                mfg.scraped_text_file_version_id = prev["version_id"]
                mfg.scraped_text_file_num_tokens = prev["num_tokens"]
                mfg.updated_at = datetime.now(timezone.utc)
                await mfg.save()
    finally:
        await client.close()


async def upload_from_local(args: argparse.Namespace) -> None:
    """Publish ALREADY-SCRAPED local corpus files (steps 3-4) without touching
    the sites again: the run record holds every stat a ScrapingResult needs, so
    the scrape and the publish are independent halves. Used when the crawl ran
    under narrower permissions than the S3/Mongo writes."""
    from scraper.models.scraping_result import ScrapingResult

    record = load_json(RECORD_PATH)
    reverts = load_json(REVERT_PATH)
    subjects = (
        [s.strip() for s in args.subjects.split(",") if s.strip()]
        if args.subjects
        else sorted(record)
    )
    await initialize_aws_clients(AWSClientName.SCRAPED_TEXT_S3)
    mongo_client = await init_db([Manufacturer]) if args.pointer else None
    batch = Batch(title=BATCH_TITLE, timestamp=datetime.now(timezone.utc))
    try:
        for subject in subjects:
            entry = record.get(subject)
            path = OUT_DIR / f"{subject}.txt"
            if entry is None or not path.exists():
                logger.warning("%s: no local scrape to publish, skipped", subject)
                continue
            if entry.get("status") == "ok" and not args.force:
                logger.info("%s: already published, skipped", subject)
                continue
            if not entry.get("valid"):
                logger.warning("%s: recorded scrape was invalid, not publishing", subject)
                continue
            # A hand-set veto for a scrape that PASSES ScrapingResult.is_valid()
            # but must not reach S3/Mongo. is_valid only asks for >30 tokens,
            # >80% success and no timeout — sterlingmfg.net (2026-08-29) is a
            # lapsed domain now serving a 92-token GoDaddy parking page, which
            # clears every one of those bars and would overwrite the subject's
            # real text (its content moved to eptam.com years ago; the pinned
            # legacy file is full of eptam.com URLs). Silent corpus destruction
            # is worse than a gap, so publishing is refused until a human
            # re-points or drops the subject.
            if entry.get("publish_blocked"):
                logger.warning(
                    "%s: publish BLOCKED (%s)", subject, entry["publish_blocked"]
                )
                continue

            manifest_path = OUT_DIR / f"{subject}.manifest.json"
            result = ScrapingResult(
                content=path.read_text(),
                errors=[],
                urls_scraped=entry["urls_scraped"],
                urls_failed=entry["urls_failed"],
                urls_discovered=entry["urls_discovered"],
                total_time_taken=entry.get("total_time_taken", entry.get("elapsed_s", 0.0)),
                timed_out=entry["timed_out"],
                llm_model=GPT_5_2,
                final_landing_etld1=entry.get("final_landing_etld1") or subject,
                text_format=entry["text_format"],
                # the sidecar rides along; upload_to_s3_and_create stamps the
                # text version id into it
                manifest=(
                    json.loads(manifest_path.read_text()) if manifest_path.exists() else None
                ),
            )
            scraped_file = await ScrapedMfgFile.upload_to_s3_and_create(batch, result, subject)
            entry.update(
                s3_version_id=scraped_file.s3_version_id,
                s3_text_format=scraped_file.text_format,
            )
            logger.info("%s: uploaded s3 version %s", subject, scraped_file.s3_version_id)

            if args.pointer:
                mfg = await Manufacturer.find_one(Manufacturer.etld1 == subject)
                if mfg is None:
                    entry["status"] = "uploaded_no_mongo_record"
                    logger.warning("%s: no manufacturer in mongo; pointer not moved", subject)
                else:
                    reverts.setdefault(
                        subject,
                        {
                            "version_id": mfg.scraped_text_file_version_id,
                            "num_tokens": mfg.scraped_text_file_num_tokens,
                            "recorded_at": datetime.now(timezone.utc).isoformat(),
                        },
                    )
                    save_json(REVERT_PATH, reverts)
                    entry["previous_pointer"] = reverts[subject]["version_id"]
                    mfg.scraped_text_file_num_tokens = scraped_file.num_tokens
                    mfg.scraped_text_file_version_id = scraped_file.s3_version_id
                    mfg.updated_at = datetime.now(timezone.utc)
                    await mfg.save()
                    logger.info(
                        "%s: mongo pointer %s -> %s",
                        subject, entry["previous_pointer"], scraped_file.s3_version_id,
                    )
                    entry["status"] = "ok"
            else:
                entry["status"] = "uploaded_no_pointer"
            record[subject] = entry
            save_json(RECORD_PATH, record)
    finally:
        if mongo_client is not None:
            await mongo_client.close()
        await cleanup_aws_clients()


async def run(args: argparse.Namespace) -> None:
    subjects = (
        [s.strip() for s in args.subjects.split(",") if s.strip()]
        if args.subjects
        else subjects_from_pinned()
    )
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    record = load_json(RECORD_PATH)
    reverts = load_json(REVERT_PATH)

    if args.upload or args.pointer:
        await initialize_aws_clients(AWSClientName.SCRAPED_TEXT_S3)
    mongo_client = await init_db([Manufacturer]) if args.pointer else None

    batch = Batch(title=BATCH_TITLE, timestamp=datetime.now(timezone.utc))
    scraper = ScraperService(
        max_concurrent_browsers=args.browsers,
        max_depth=args.depth,
        scrape_timeout=args.timeout,
        output_format="markdown",
        soft_token_cutoff=args.token_cutoff or None,
    )

    try:
        for i, subject in enumerate(subjects, 1):
            done = record.get(subject, {})
            if done.get("status") == "ok" and not args.force:
                logger.info("[%d/%d] %s: already done, skipping", i, len(subjects), subject)
                continue

            logger.info("[%d/%d] %s: scraping ...", i, len(subjects), subject)
            entry: dict = {"subject": subject, "started_at": datetime.now(timezone.utc).isoformat()}
            t0 = time.monotonic()
            # `--start-url` exists for subjects whose HTTPS is unusable by the
            # PRE-FLIGHT redirect resolver: `get_final_landing_url` verifies
            # certificates (requests), while the browser is launched with
            # --ignore-certificate-errors. austinelectricservices.com serves a
            # cert invalid for its own hostname (apex and www), so the scrape
            # dies before a browser starts even though Chrome would render the
            # site; http:// resolves normally. Recorded in the run entry so the
            # corpus says which subject was reached over a non-default URL.
            start_url = args.start_url or f"https://{subject}"
            if args.start_url:
                entry["start_url_override"] = start_url
            try:
                result = scraper.scrape(start_url, GPT_5_2)
            except Exception as e:
                entry.update(status="scrape_error", error=f"{type(e).__name__}: {e}")
                record[subject] = entry
                save_json(RECORD_PATH, record)
                logger.error("%s: scrape raised %s", subject, e, exc_info=True)
                continue

            entry.update(
                elapsed_s=round(time.monotonic() - t0, 1),
                urls_scraped=result.urls_scraped,
                urls_failed=result.urls_failed,
                urls_discovered=result.urls_discovered,
                success_rate=round(result.success_rate, 4),
                timed_out=result.timed_out,
                num_tokens=result.num_tokens,
                chars=len(result.content),
                text_format=result.text_format,
                # every field --upload-from-local needs to rebuild a
                # ScrapingResult without re-crawling the site
                final_landing_etld1=result.final_landing_etld1,
                total_time_taken=result.total_time_taken,
                valid=result.is_valid(),
            )

            # 2. local corpus file (always — a partial text is still useful),
            #    plus the provenance manifest sidecar (scrape_manifest.py)
            (OUT_DIR / f"{subject}.txt").write_text(result.content)
            if result.manifest is not None:
                (OUT_DIR / f"{subject}.manifest.json").write_text(
                    json.dumps(result.manifest, indent=1, sort_keys=True)
                )
            logger.info(
                "%s: %d pages, %d tokens, %.0f%% success%s -> local file",
                subject, result.urls_scraped, result.num_tokens,
                100 * result.success_rate, " (TIMED OUT)" if result.timed_out else "",
            )

            if not result.is_valid():
                entry["status"] = "invalid_not_uploaded"
                record[subject] = entry
                save_json(RECORD_PATH, record)
                logger.warning(
                    "%s: scrape invalid (timed_out=%s success=%.2f tokens=%d) — "
                    "S3 upload skipped by the model's own guard",
                    subject, result.timed_out, result.success_rate, result.num_tokens,
                )
                continue

            # 3. S3: a new version, tagged text_format=markdown_v1
            if args.upload:
                scraped_file = await ScrapedMfgFile.upload_to_s3_and_create(
                    batch, result, subject
                )
                entry.update(
                    s3_version_id=scraped_file.s3_version_id,
                    s3_text_format=scraped_file.text_format,
                )
                logger.info("%s: uploaded s3 version %s", subject, scraped_file.s3_version_id)
            else:
                scraped_file = None

            # 4. Mongo pointer (what extraction actually reads) — previous value
            #    recorded BEFORE the write so --revert can always undo it
            if args.pointer and scraped_file is not None:
                mfg = await Manufacturer.find_one(Manufacturer.etld1 == subject)
                if mfg is None:
                    entry["status"] = "uploaded_no_mongo_record"
                    logger.warning("%s: no manufacturer in mongo; pointer not moved", subject)
                else:
                    reverts.setdefault(
                        subject,
                        {
                            "version_id": mfg.scraped_text_file_version_id,
                            "num_tokens": mfg.scraped_text_file_num_tokens,
                            "recorded_at": datetime.now(timezone.utc).isoformat(),
                        },
                    )
                    save_json(REVERT_PATH, reverts)
                    entry["previous_pointer"] = reverts[subject]["version_id"]
                    mfg.scraped_text_file_num_tokens = scraped_file.num_tokens
                    mfg.scraped_text_file_version_id = scraped_file.s3_version_id
                    mfg.updated_at = datetime.now(timezone.utc)
                    await mfg.save()
                    logger.info(
                        "%s: mongo pointer %s -> %s",
                        subject, entry["previous_pointer"], scraped_file.s3_version_id,
                    )
                    entry["status"] = "ok"
            else:
                entry["status"] = "ok" if args.upload else "local_only"

            record[subject] = entry
            save_json(RECORD_PATH, record)
    finally:
        scraper._cleanup_all_drivers()
        if mongo_client is not None:
            await mongo_client.close()
        if args.upload or args.pointer:
            await cleanup_aws_clients()

    ok = sum(1 for e in record.values() if e.get("status") in ("ok", "local_only"))
    logger.info("done: %d/%d subjects ok; record at %s", ok, len(subjects), RECORD_PATH)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--subjects", help="comma-separated etld1s (default: all pinned)")
    p.add_argument("--browsers", type=int, default=5)
    p.add_argument("--depth", type=int, default=5)
    p.add_argument("--timeout", type=int, default=90, help="per-subject minutes")
    p.add_argument(
        "--token-cutoff", type=int, default=100_000,
        help="soft estimated-token cutoff per subject (0 = crawl everything; "
        "default 100k, user decision 2026-09-03)",
    )
    p.add_argument("--no-upload", dest="upload", action="store_false", help="skip S3")
    p.add_argument("--no-pointer", dest="pointer", action="store_false", help="skip mongo")
    p.add_argument("--force", action="store_true", help="redo subjects already recorded ok")
    p.add_argument(
        "--start-url",
        help="override the https://<etld1> start URL (use with a single --subjects value)",
    )
    p.add_argument("--revert", action="store_true", help="restore mongo pointers and exit")
    p.add_argument("--dry-revert", action="store_true", help="show what --revert would do")
    p.add_argument(
        "--upload-from-local",
        action="store_true",
        help="publish already-scraped local files to S3 (+mongo) without re-crawling",
    )
    args = p.parse_args()

    if args.revert or args.dry_revert:
        asyncio.run(revert(record_only=args.dry_revert))
        return
    if args.upload_from_local:
        asyncio.run(upload_from_local(args))
        return
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
