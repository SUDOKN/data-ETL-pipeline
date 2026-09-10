#!/usr/bin/env python
"""Paired A/B capture: both renderings of the SAME page load.

WHY THIS AND NOT A CONVERTER (measured 2026-08-29). Converting the stored
legacy corpus to Markdown cannot produce a valid A/B, because the legacy text
is a lossy projection that already threw the structure away:

  - Table HEADER ASSOCIATION is gone. innerText tab-separates cells, so DATA
    rows survive ("Min\t0.18\t0.25\t..."), but on howcogroup.com the header
    cells were laid out as blocks and became one line EACH ("Fe" alone on a
    line). Nothing in the text says 0.18 is the carbon column. A converter
    would emit a confident, unlabeled table — worse than emitting none.
  - Heading LEVEL, list NESTING, colspan/rowspan: a heading and a list item
    are both just lines.
  - Alt text and closed-<details> content are ABSENT, not flattened (verified:
    med-tekinc's two image captions appear once in markdown, zero times in
    legacy).

So a converted corpus would measure the converter's guesses, not the format.

The other tempting comparison — the pinned legacy corpus vs the 2026-08-29
markdown corpus — is confounded by SITE DRIFT (taylordunn 415 pages now vs 31
pinned). Any delta mixes format change with site change.

This harness removes both confounds: for every page it takes ONE navigation
and reads both renderings off the same loaded DOM — `html_to_markdown` on
`outerHTML` (the production path, byte-identical, via super()) and
`document.body.innerText` immediately after (the legacy path). Both documents
are then assembled with the same page-block envelope, in the same URL order,
and passed through the same `deduplicate_scraped_content`, so the ONLY
difference between arm A and arm B is the rendering.

Usage:
    python paired_capture_ab.py --subjects med-tekinc.com
    python paired_capture_ab.py                      # all pinned subjects
Outputs (under ab_pairs/): <subject>.markdown.txt, <subject>.legacy.txt,
and ab_summary.json with per-subject token/structure counts.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import threading
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
for _pkg in ("pure_utils", "infra", "core", "llm_providers", "scraper"):
    sys.path.insert(0, str(REPO / "packages" / _pkg / "src"))

from pure_utils.env_util import load_env  # noqa: E402

load_env(["CHROME_PROFILE_TMPDIR"])

import litellm  # noqa: E402

from llm_providers.models.llm_model import GPT_5_2  # noqa: E402
from scraper.services.url_scraper_service import ScraperService  # noqa: E402
from scraper.utils.dedup_util import deduplicate_scraped_content  # noqa: E402

PINNED = REPO / "apps/data_etl_app/tests/test_stages/sample_scraped_texts"
OUT = Path(__file__).resolve().parent / "ab_pairs"
SEP = "#" * 50

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("ab")
for noisy in ("selenium", "urllib3", "scraper.utils.selenium.driver_factory",
              "scraper.services.url_scraper_service", "scraper.utils.dedup_util"):
    logging.getLogger(noisy).setLevel(logging.WARNING)


class PairedScraperService(ScraperService):
    """ScraperService that also keeps the legacy innerText of every page.

    The markdown arm is produced by the UNMODIFIED production method (super()),
    so arm A is exactly what the scraper ships. The legacy arm is read from the
    same already-loaded DOM right after it returns — no second navigation, so
    the two arms cannot disagree about what the page was.
    """

    def __init__(self, **kwargs):
        super().__init__(output_format="markdown", **kwargs)
        self.legacy_by_url: dict[str, str] = {}
        self._legacy_lock = threading.Lock()

    def _extract_text_with_fallback(self, driver, url: str) -> str:
        markdown = super()._extract_text_with_fallback(driver, url)
        try:
            legacy = driver.execute_script(
                "return document.body ? document.body.innerText : ''"
            ) or ""
        except Exception:
            legacy = ""
        with self._legacy_lock:
            self.legacy_by_url[url] = legacy.strip()
        return markdown


def urls_in_order(document: str) -> list[str]:
    """The page URLs of a combined document, in block order."""
    return re.findall(rf"^{SEP}\n(\S+)$", document, flags=re.M)


def counts(text: str) -> dict:
    heads = bullets = numbered = pipes = islands = tabs = 0
    for line in text.split("\n"):
        stripped = line.lstrip()
        if re.match(r"^#{1,6} \S", line):
            heads += 1
        if stripped.startswith("- "):
            bullets += 1
        if re.match(r"^\d+\. \S", stripped):
            numbered += 1
        if line.startswith("|"):
            pipes += 1
        if line.startswith("<table>"):
            islands += 1
        if "\t" in line:
            tabs += 1
    return {
        "headings": heads, "bullets": bullets, "numbered": numbered,
        "table_rows": pipes, "islands": islands, "tab_lines": tabs,
        "tokens": litellm.token_counter(model=GPT_5_2.name, text=text),
        "chars": len(text),
    }


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--subjects", help="comma-separated etld1s (default: all pinned)")
    p.add_argument("--browsers", type=int, default=5)
    p.add_argument("--depth", type=int, default=5)
    p.add_argument("--timeout", type=int, default=90)
    args = p.parse_args()

    subjects = (
        [s.strip() for s in args.subjects.split(",") if s.strip()]
        if args.subjects
        else sorted(q.stem for q in PINNED.glob("*.txt"))
    )
    OUT.mkdir(parents=True, exist_ok=True)
    summary_path = OUT / "ab_summary.json"
    summary = json.loads(summary_path.read_text()) if summary_path.exists() else {}

    for i, subject in enumerate(subjects, 1):
        logger.info("[%d/%d] %s: paired capture ...", i, len(subjects), subject)
        scraper = PairedScraperService(
            max_concurrent_browsers=args.browsers,
            max_depth=args.depth,
            scrape_timeout=args.timeout,
        )
        t0 = time.monotonic()
        result = scraper.scrape(f"https://{subject}", GPT_5_2)
        markdown_doc = result.content

        # Arm B: same pages, same order, same envelope, same dedup — only the
        # rendering differs.
        legacy_doc = deduplicate_scraped_content(
            "".join(
                f"{SEP}\n{u}\n\n{scraper.legacy_by_url.get(u, '')}\n"
                for u in urls_in_order(markdown_doc)
            )
        )

        (OUT / f"{subject}.markdown.txt").write_text(markdown_doc)
        (OUT / f"{subject}.legacy.txt").write_text(legacy_doc)

        md_c, lg_c = counts(markdown_doc), counts(legacy_doc)
        summary[subject] = {
            "pages_scraped": result.urls_scraped,
            "pages_paired": len(scraper.legacy_by_url),
            "elapsed_s": round(time.monotonic() - t0, 1),
            "markdown": md_c,
            "legacy": lg_c,
            "token_ratio": round(md_c["tokens"] / max(lg_c["tokens"], 1), 4),
        }
        summary_path.write_text(json.dumps(summary, indent=1, sort_keys=True))
        logger.info(
            "%s: %d pages | tokens md=%d legacy=%d (%.3fx) | md headings=%d bullets=%d tables=%d",
            subject, result.urls_scraped, md_c["tokens"], lg_c["tokens"],
            summary[subject]["token_ratio"], md_c["headings"], md_c["bullets"], md_c["table_rows"],
        )
        scraper._cleanup_all_drivers()


if __name__ == "__main__":
    main()
