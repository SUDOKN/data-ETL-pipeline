#!/usr/bin/env python3
"""
Render extraction prompts from their rule catalogs, and publish every prompt.

Catalogs are the source of truth for the prompts that have one. Rendered prompts
are written to knowledge/prompts/final_texts/assembled/, which is build output
(gitignored). Hand-written prompts with no catalog — search, recursive search,
relationship, and all single-stage — live alongside it in final_texts/static/.
Do not hand-edit a rendered prompt: edit its catalog and re-render.

Both kinds publish to the same flat S3 namespace and both are handled by every
command here. Only `render` and the rendering half of `check` are catalog-only,
because a static prompt has nothing to render from.

Usage:
    python assemble_prompts.py render     # write final_texts/assembled/**.txt
    python assemble_prompts.py check      # fail if what ships differs from disk
    python assemble_prompts.py publish    # upload to S3, record the version id
    python assemble_prompts.py adopt      # record version IDs of prompts already
                                          # in S3, without re-uploading

    ... [--only PROMPT_NAME] [--dry-run] [--force]

`check` catches the two ways on-disk truth stops being what runs: a hand-edited
rendered prompt (re-render every catalog, diff against disk) and a static prompt
edited but never published (hash it, compare against its recorded pin). The
second is the one that bit us on 2026-08-11 — `publish` covered catalogs only, so
edits to the relationship prompts never left the machine while the pipeline went
on reading the superseded S3 copy as `latest`.

`publish` uploads the text and records the returned S3 version ID: into the
catalog's `published` block for a catalog prompt, into
knowledge/prompts/static_prompt_pins.config.json for a static one. That block is
excluded from the render input, so recording it never changes what renders next
time. It also stamps the rendered digest (and, for catalog prompts, the catalog
version) onto the object as S3 metadata, which is what `PromptService` verifies
at init. Because metadata cannot be added to an existing version, `--force`
re-uploads prompts whose text has not changed.
"""

from __future__ import annotations

import argparse
import asyncio
import difflib
import json
import logging
import sys
from datetime import datetime, timezone
from typing import AsyncContextManager

from pure_utils.env_util import load_env

from data_etl_app.dependencies.env import PROMPT_SCRIPT_ENV
from data_etl_app.services.prompt_assembly_service import (
    ASSEMBLED_PROMPTS_DIR,
    CATALOG_DIR,
    STATIC_PIN_FILE,
    PromptAssemblyError,
    StaticPromptPin,
    load_all_catalogs,
    load_static_pins,
    load_static_prompts,
    prompt_s3_key,
    render_prompt,
    rendered_sha256,
    save_static_pins,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def _render_all(only: str | None) -> dict[str, tuple[str, str]]:
    """prompt_name -> (s3 key, rendered text). Catalog prompts only."""
    catalogs = load_all_catalogs()
    if only:
        if only not in catalogs:
            # Not an error yet: `only` may name a static prompt, and every caller
            # of this also consults _static_all. _reject_unknown_only is what
            # decides the name is unknown to both.
            return {}
        catalogs = {only: catalogs[only]}

    return {
        name: (prompt_s3_key(catalog), render_prompt(catalog))
        for name, catalog in sorted(catalogs.items())
    }


def _static_all(only: str | None) -> dict[str, tuple[str, str]]:
    """prompt_name -> (s3 key, text on disk). Hand-written prompts only."""
    statics = load_static_prompts()
    if only:
        return {only: statics[only]} if only in statics else {}
    return statics


def _reject_unknown_only(only: str | None) -> None:
    """`--only` naming neither a catalog nor a static prompt is a typo, and
    silently publishing nothing is the worst possible response to it."""
    if not only:
        return

    known = set(load_all_catalogs()) | set(load_static_prompts())
    if only not in known:
        raise SystemExit(f"unknown prompt {only!r}; known: {', '.join(sorted(known))}")


def cmd_render(args: argparse.Namespace) -> int:
    _reject_unknown_only(args.only)
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


def _check_static(only: str | None) -> list[str]:
    """Static prompts whose bytes on disk are not what is published.

    There is nothing to re-render for these, so the check is against the pin:
    edit a static prompt without publishing it and the pipeline keeps reading the
    old object, with nothing in the diff to say so.
    """
    pins = load_static_pins()
    stale: list[str] = []

    for name, (s3_key, text) in _static_all(only).items():
        pin = pins.get(name)
        if pin is None:
            logger.error("UNPINNED   %s (never published; run `publish`)", s3_key)
            stale.append(name)
            continue

        digest = rendered_sha256(text)
        if pin.rendered_sha256 != digest:
            logger.error(
                "UNPUBLISHED %s — on disk hashes to %s but the published version "
                "%s is %s; run `publish`",
                s3_key,
                digest[:12],
                pin.s3_version_id,
                pin.rendered_sha256[:12],
            )
            stale.append(name)

    if only is None:
        for name in sorted(set(pins) - set(load_static_prompts())):
            logger.error(
                "ORPHANED   %s — pinned but no longer on disk; delete its entry "
                "from %s",
                pins[name].s3_key,
                STATIC_PIN_FILE.name,
            )
            stale.append(name)

    return stale


def cmd_check(args: argparse.Namespace) -> int:
    _reject_unknown_only(args.only)
    drifted: list[str] = _check_static(args.only)
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
            "%d prompt(s) are not what is published: %s",
            len(drifted),
            ", ".join(drifted),
        )
        return 1

    logger.info("all prompts match their catalogs and their published versions")
    return 0


def _prompt_s3() -> AsyncContextManager[None]:
    """The prompt S3 client is created explicitly at entrypoint startup; this
    script is its own entrypoint. Scoping it to the command closes the client's
    HTTP session before the event loop shuts down, which is what keeps aiohttp
    from complaining about an unclosed session at exit."""
    from infra.utils.aws.clients import AWSClientName, aws_clients

    return aws_clients(AWSClientName.PROMPT_RDF_S3)


async def _publish_static(only: str | None, dry_run: bool, force: bool, published_at: datetime) -> None:
    """Upload the hand-written prompts and record what went where.

    Stamped with the rendered digest but no catalog version, because there is no
    catalog: the digest is the whole provenance a static prompt can carry, and it
    is enough to prove the bytes a run fetched are the bytes that were published.
    """
    from infra.utils.aws.s3.prompt_s3_util import (
        RENDERED_SHA256_METADATA_KEY,
        upload_prompt,
    )

    pins = load_static_pins()
    for name, (s3_key, text) in _static_all(only).items():
        digest = rendered_sha256(text)
        pin = pins.get(name)

        if not force and pin is not None and pin.rendered_sha256 == digest:
            logger.info("up to date %s", s3_key)
            continue

        if dry_run:
            logger.info("would publish %s", s3_key)
            continue

        version_id = await upload_prompt(
            s3_key,
            text,
            metadata={RENDERED_SHA256_METADATA_KEY: digest},
        )
        pins[name] = StaticPromptPin(
            s3_key=s3_key,
            s3_version_id=version_id,
            rendered_sha256=digest,
            uploaded_at=published_at.isoformat(),
        )
        # Written per prompt rather than once at the end: an upload that succeeds
        # and then loses its pin to a crash is unrecoverable without diffing S3
        # by hand, and re-writing a small file is cheap.
        save_static_pins(pins)
        logger.info("published  %s  version=%s", s3_key, version_id)


async def _publish(only: str | None, dry_run: bool, force: bool) -> int:
    # Imported lazily so `render` and `check` need no AWS credentials.
    from infra.utils.aws.s3.prompt_s3_util import (
        CATALOG_VERSION_METADATA_KEY,
        RENDERED_SHA256_METADATA_KEY,
        upload_prompt,
    )

    published_at = datetime.now(timezone.utc)
    async with _prompt_s3():
        await _publish_static(only, dry_run, force, published_at)
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
    _reject_unknown_only(args.only)
    load_env(PROMPT_SCRIPT_ENV)
    return asyncio.run(_publish(args.only, args.dry_run, args.force))


async def _adopt_static(
    only: str | None, dry_run: bool, adopted_at: datetime
) -> list[str]:
    """Record the live version IDs of hand-written prompts already in S3.

    Same contract as the catalog half — byte-identical to disk, stamped, and the
    stamp matching, or refused rather than recorded — with the catalog checks
    dropped for want of a catalog. The one check this half adds is the reverse:
    an object stamped with a catalog version means the key now belongs to a
    rendered prompt, and adopting it would pin a static prompt on top of a
    catalog's own record.

    Returns the names it refused.
    """
    from infra.utils.aws.s3.prompt_s3_util import download_prompt

    pins = load_static_pins()
    mismatched: list[str] = []

    for name, (s3_key, text) in _static_all(only).items():
        digest = rendered_sha256(text)

        try:
            live = await download_prompt(s3_key)
        except Exception as exc:
            logger.error("MISSING    %s (%s)", s3_key, exc)
            mismatched.append(name)
            continue

        if live.text != text:
            logger.error(
                "DIFFERS    %s — S3 content is not the file on disk; run `publish` "
                "to overwrite it instead",
                s3_key,
            )
            mismatched.append(name)
            continue

        if live.rendered_sha256 is None:
            logger.error(
                "UNSTAMPED  %s — object carries no provenance metadata and one "
                "cannot be added to an existing version; run `publish` instead",
                s3_key,
            )
            mismatched.append(name)
            continue

        if live.catalog_version is not None:
            logger.error(
                "CATALOGUED %s — object is stamped with catalog %s, so this key is "
                "published from a catalog now; delete the hand-written copy rather "
                "than pinning it",
                s3_key,
                live.catalog_version,
            )
            mismatched.append(name)
            continue

        if live.rendered_sha256 != digest:
            logger.error(
                "STAMP      %s — object is stamped %s but the file on disk hashes "
                "to %s; run `publish` instead",
                s3_key,
                live.rendered_sha256[:12],
                digest[:12],
            )
            mismatched.append(name)
            continue

        if dry_run:
            logger.info("would adopt %s  version=%s", s3_key, live.version_id)
            continue

        pins[name] = StaticPromptPin(
            s3_key=s3_key,
            s3_version_id=live.version_id,
            rendered_sha256=digest,
            uploaded_at=adopted_at.isoformat(),
        )
        # Per prompt, for the same reason `publish` writes per prompt: a pin lost
        # to a crash costs a hand-diff against S3 to recover.
        save_static_pins(pins)
        logger.info("adopted    %s  version=%s", s3_key, live.version_id)

    return mismatched


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
        mismatched.extend(await _adopt_static(only, dry_run, adopted_at))
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
    _reject_unknown_only(args.only)
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
