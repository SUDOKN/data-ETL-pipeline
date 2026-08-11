#!/usr/bin/env python3
"""
Render extraction prompts from their rule catalogs.

Catalogs are the source of truth. Rendered prompts are written to
knowledge/prompts/final_texts/assembled/, which is build output (gitignored).
Hand-written prompts with no catalog live alongside it in final_texts/static/.
Do not hand-edit a rendered prompt: edit its catalog and re-render.

Usage:
    python assemble_prompts.py render     # write final_texts/assembled/**.txt
    python assemble_prompts.py check      # fail if rendered output drifted
    python assemble_prompts.py publish    # upload to S3, record the version id
    python assemble_prompts.py adopt      # record version IDs of prompts already
                                          # in S3, without re-uploading

    ... [--only PROMPT_NAME] [--dry-run] [--force]

`check` is what catches a hand-edited prompt: it re-renders every catalog and
diffs against what is on disk, without writing anything.

`publish` uploads the rendered text and writes the returned S3 version ID back
into the catalog's `published` block. That block is excluded from the render
input, so recording it never changes what renders next time. It also stamps the
catalog version and the rendered digest onto the object as S3 metadata, which is
what `PromptService` verifies at init. Because metadata cannot be added to an
existing version, `--force` re-uploads prompts whose text has not changed.
"""

from __future__ import annotations

import argparse
import asyncio
import difflib
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import AsyncContextManager

from pure_utils.env_util import load_env

from data_etl_app.dependencies.env import PROMPT_SCRIPT_ENV
from data_etl_app.services.prompt_assembly_service import (
    ASSEMBLED_PROMPTS_DIR,
    CATALOG_DIR,
    PromptAssemblyError,
    assembled_prompt_path,
    load_all_catalogs,
    prompt_s3_key,
    render_prompt,
    rendered_sha256,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def _render_all(only: str | None) -> dict[str, tuple[str, str]]:
    """prompt_name -> (s3 key, rendered text)."""
    catalogs = load_all_catalogs()
    if only:
        if only not in catalogs:
            raise SystemExit(
                f"unknown prompt {only!r}; known: {', '.join(sorted(catalogs))}"
            )
        catalogs = {only: catalogs[only]}

    return {
        name: (prompt_s3_key(catalog), render_prompt(catalog))
        for name, catalog in sorted(catalogs.items())
    }


def cmd_render(args: argparse.Namespace) -> int:
    changed = 0
    for name, (s3_key, text) in _render_all(args.only).items():
        target = ASSEMBLED_PROMPTS_DIR / s3_key
        existing = target.read_text(encoding="utf-8") if target.exists() else None
        if existing == text:
            logger.info("unchanged  %s", s3_key)
            continue

        changed += 1
        if args.dry_run:
            logger.info("would write %s", s3_key)
            continue

        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text, encoding="utf-8")
        logger.info("wrote      %s", s3_key)

    logger.info("%d prompt(s) %s", changed, "would change" if args.dry_run else "written")
    return 0


def cmd_check(args: argparse.Namespace) -> int:
    drifted: list[str] = []
    for name, (s3_key, text) in _render_all(args.only).items():
        target = ASSEMBLED_PROMPTS_DIR / s3_key
        if not target.exists():
            logger.error("MISSING    %s (run `render`)", s3_key)
            drifted.append(name)
            continue

        on_disk = target.read_text(encoding="utf-8")
        if on_disk == text:
            continue

        drifted.append(name)
        logger.error("DRIFTED    %s", s3_key)
        diff = difflib.unified_diff(
            on_disk.splitlines(),
            text.splitlines(),
            fromfile=f"{s3_key} (on disk)",
            tofile=f"{s3_key} (from catalog)",
            lineterm="",
            n=1,
        )
        for line in list(diff)[:40]:
            print(f"    {line}")

    if drifted:
        logger.error(
            "%d prompt(s) differ from their catalogs: %s",
            len(drifted),
            ", ".join(drifted),
        )
        return 1

    logger.info("all prompts match their catalogs")
    return 0


def _prompt_s3() -> AsyncContextManager[None]:
    """The prompt S3 client is created explicitly at entrypoint startup; this
    script is its own entrypoint. Scoping it to the command closes the client's
    HTTP session before the event loop shuts down, which is what keeps aiohttp
    from complaining about an unclosed session at exit."""
    from infra.utils.aws.clients import AWSClientName, aws_clients

    return aws_clients(AWSClientName.PROMPT_RDF_S3)


async def _publish(only: str | None, dry_run: bool, force: bool) -> int:
    # Imported lazily so `render` and `check` need no AWS credentials.
    from infra.utils.aws.s3.prompt_s3_util import (
        CATALOG_VERSION_METADATA_KEY,
        RENDERED_SHA256_METADATA_KEY,
        upload_prompt,
    )

    published_at = datetime.now(timezone.utc)
    async with _prompt_s3():
        for name, (s3_key, text) in _render_all(only).items():
            catalog_path = CATALOG_DIR / f"{name}.json"
            catalog_raw = json.loads(catalog_path.read_text(encoding="utf-8"))
            digest = rendered_sha256(text)

            # --force exists for stamping: the objects published before provenance
            # metadata carry the right bytes and so pass this check, but there is
            # no way to add the stamp to an existing version.
            if not force and catalog_raw["published"].get("rendered_sha256") == digest:
                logger.info("up to date %s", s3_key)
                continue

            if dry_run:
                logger.info("would publish %s", s3_key)
                continue

            version_id = await upload_prompt(
                s3_key,
                text,
                metadata={
                    CATALOG_VERSION_METADATA_KEY: catalog_raw["catalog_version"],
                    RENDERED_SHA256_METADATA_KEY: digest,
                },
            )

            # Rewrite in place so the rest of the catalog keeps its key order and
            # formatting; only the published block moves.
            catalog_raw["published"] = {
                "s3_version_id": version_id,
                "rendered_sha256": digest,
                "uploaded_at": published_at.isoformat(),
            }
            catalog_path.write_text(
                json.dumps(catalog_raw, indent=2, ensure_ascii=False) + "\n",
                encoding="utf-8",
            )

            # Keep the local copy identical to what was just published.
            target = ASSEMBLED_PROMPTS_DIR / s3_key
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(text, encoding="utf-8")
            logger.info("published  %s  version=%s", s3_key, version_id)

    return 0


def cmd_publish(args: argparse.Namespace) -> int:
    load_env(PROMPT_SCRIPT_ENV)
    return asyncio.run(_publish(args.only, args.dry_run, args.force))


async def _adopt(only: str | None, dry_run: bool) -> int:
    """Record the version IDs of prompts already in S3, without re-uploading.

    For prompts uploaded out of band: the object is downloaded, checked to be
    byte-identical to what its catalog renders AND to carry the provenance stamp
    matching this catalog, and its live version ID written into `published`. A
    mismatch is refused rather than recorded, because pinning to a version whose
    bytes differ from the catalog would make the catalog lie.

    An unstamped object is refused too. Metadata cannot be added to an existing
    version, so adopting one would leave a pinned prompt that `PromptService` has
    no way to verify — exactly the case the stamp exists to rule out. `publish`
    is the way in: it re-uploads and stamps in one step.
    """
    from infra.utils.aws.s3.prompt_s3_util import download_prompt

    adopted_at = datetime.now(timezone.utc)
    mismatched: list[str] = []

    async with _prompt_s3():
        for name, (s3_key, text) in _render_all(only).items():
            catalog_path = CATALOG_DIR / f"{name}.json"
            catalog_raw = json.loads(catalog_path.read_text(encoding="utf-8"))
            digest = rendered_sha256(text)

            try:
                live = await download_prompt(s3_key)
            except Exception as exc:
                logger.error("MISSING    %s (%s)", s3_key, exc)
                mismatched.append(name)
                continue

            if live.text != text:
                logger.error(
                    "DIFFERS    %s — S3 content is not what this catalog renders; "
                    "run `publish` to overwrite it instead",
                    s3_key,
                )
                mismatched.append(name)
                continue

            if live.catalog_version is None or live.rendered_sha256 is None:
                logger.error(
                    "UNSTAMPED  %s — object carries no provenance metadata and one "
                    "cannot be added to an existing version; run `publish` instead",
                    s3_key,
                )
                mismatched.append(name)
                continue

            if (live.catalog_version, live.rendered_sha256) != (
                catalog_raw["catalog_version"],
                digest,
            ):
                logger.error(
                    "STAMP      %s — object is stamped %s/%s but this catalog is "
                    "%s/%s; run `publish` instead",
                    s3_key,
                    live.catalog_version,
                    live.rendered_sha256[:12],
                    catalog_raw["catalog_version"],
                    digest[:12],
                )
                mismatched.append(name)
                continue

            if dry_run:
                logger.info("would adopt %s  version=%s", s3_key, live.version_id)
                continue

            version_id = live.version_id
            catalog_raw["published"] = {
                "s3_version_id": version_id,
                "rendered_sha256": digest,
                "uploaded_at": adopted_at.isoformat(),
            }
            catalog_path.write_text(
                json.dumps(catalog_raw, indent=2, ensure_ascii=False) + "\n",
                encoding="utf-8",
            )
            logger.info("adopted    %s  version=%s", s3_key, version_id)

    if mismatched:
        logger.error("%d prompt(s) not adopted: %s", len(mismatched), ", ".join(mismatched))
        return 1
    return 0


def cmd_adopt(args: argparse.Namespace) -> int:
    load_env(PROMPT_SCRIPT_ENV)
    return asyncio.run(_adopt(args.only, args.dry_run))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command", choices=["render", "check", "publish", "adopt"], help="what to do"
    )
    parser.add_argument("--only", help="restrict to a single prompt name")
    parser.add_argument(
        "--dry-run", action="store_true", help="report what would change, write nothing"
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="publish: re-upload even when the rendered text is unchanged. Needed "
        "to stamp provenance onto prompts published before stamping existed, "
        "since S3 metadata cannot be added to an existing version.",
    )
    args = parser.parse_args()

    handlers = {
        "render": cmd_render,
        "check": cmd_check,
        "publish": cmd_publish,
        "adopt": cmd_adopt,
    }
    try:
        return handlers[args.command](args)
    except PromptAssemblyError as exc:
        logger.error("%s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
