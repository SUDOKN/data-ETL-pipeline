"""The scrape MANIFEST: a per-scrape provenance fingerprint (2026-08-29, user
decision — part of the scraper proper, written on every scrape).

WHY. Without it, comparing a stored scrape against the live site later cannot
distinguish "the scraper skipped this" from "the site changed since" — the two
failure stories that matter read identically. The manifest records enough at
scrape time to tell them apart:

- PER-PAGE BODY HASHES, taken PRE-dedup at collection time. The dedup pass
  strips majority-voted header/footer lines from every page, so a post-dedup
  hash depends on which OTHER pages were crawled — measured 2026-08-29 on two
  crawls 11h apart: post-dedup page-hash stability was 0% on alecmfg purely
  because the dedup vote differed, while genuinely static sites measured
  98-100%. A page's fingerprint must depend only on that page's own rendered
  DOM. The converter is deterministic (same DOM -> same markdown -> same
  hash), so an unchanged page hashes identically across crawls.
- THE URL SETS: scraped, discovered-but-not-scraped, failed. A URL on the live
  site but absent from ``discovered`` = the crawl never saw it (scraper gap);
  in ``discovered`` but not scraped = seen and skipped (depth/extension/
  error); scraped with a different hash = the site changed.
- THE SITE'S OWN CLAIM: sitemap.xml's ``<lastmod>`` map when the site serves
  one — free, third-party, unreliable-but-honest evidence.
- Wall-clock scrape timestamps and the text format version.

The manifest is JSON, stored NEXT TO the text: locally as
``<etld1>.manifest.json`` beside a corpus file, in production as an S3 sidecar
object of the same name (its ``s3_text_version_id`` field names the text
version it describes — filled at upload). ``MANIFEST_VERSION`` is bumped when
this schema changes, same discipline as the text format version.

Everything in this module is pure (the one network call, the sitemap fetch,
takes an injectable fetcher and never raises).
"""

from __future__ import annotations

import hashlib
import logging
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Callable, Mapping, Optional, Sequence

logger = logging.getLogger(__name__)

# "2" (2026-08-29): added ``skipped_by_extension`` (links the crawl saw but
# filtered by SKIP_EXTENSIONS — previously they vanished without a trace; a
# site's certificate-PDF inventory now shows up here even with PDF fetching
# off), ``pdfs`` (per fetched PDF: binary sha256, bytes, extraction meta or
# error), and ``include_pdfs`` (the crawl flag that decided between the two).
MANIFEST_VERSION = "2"

# Cap on child sitemaps followed from a sitemap index — politeness and
# determinism over completeness (an index's first N children in document
# order; N children is plenty for SME manufacturer sites).
_SITEMAP_INDEX_CHILD_CAP = 10
_SITEMAP_TIMEOUT_S = 10


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8", errors="replace")).hexdigest()


def url_set_hash(urls: Sequence[str]) -> str:
    """Order-independent fingerprint of a URL set: the hash of the sorted,
    newline-joined URLs. Two crawls that saw the same pages agree on it
    regardless of crawl order."""
    return sha256_text("\n".join(sorted(set(urls))))


def parse_sitemap_xml(xml_text: str) -> tuple[dict[str, str], list[str]]:
    """``(loc -> lastmod, child_sitemap_urls)`` from one sitemap document.

    Handles both a urlset (returns its lastmod map) and a sitemapindex
    (returns its child sitemap locations). Namespace-agnostic; a URL without
    ``<lastmod>`` maps to ``""``. Malformed XML returns empty results — the
    manifest records absence, it never fails a scrape.
    """
    lastmod: dict[str, str] = {}
    children: list[str] = []
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return {}, []

    def local(tag: str) -> str:
        return tag.rsplit("}", 1)[-1].lower()

    for element in root:
        kind = local(element.tag)
        loc = mod = ""
        for child in element:
            if local(child.tag) == "loc":
                loc = (child.text or "").strip()
            elif local(child.tag) == "lastmod":
                mod = (child.text or "").strip()
        if not loc:
            continue
        if kind == "sitemap":
            children.append(loc)
        elif kind == "url":
            lastmod[loc] = mod
    return lastmod, children


def fetch_sitemap_lastmods(
    origin: str, fetcher: Optional[Callable[[str], str]] = None
) -> dict:
    """The site's ``sitemap.xml`` lastmod claims, as a manifest fragment:
    ``{"fetched": bool, "url": ..., "lastmod": {loc: lastmod}, "error": ...}``.

    *origin* is scheme+host (no trailing slash). *fetcher* maps URL -> body
    text and exists for tests; the default uses ``requests`` (already a
    scraper dependency via the redirect resolver). Never raises: any failure
    is recorded in ``error`` and the scrape proceeds.
    """
    sitemap_url = f"{origin.rstrip('/')}/sitemap.xml"

    def _default_fetcher(url: str) -> str:  # pragma: no cover - network default
        import requests

        response = requests.get(
            url, timeout=_SITEMAP_TIMEOUT_S, headers={"User-Agent": "sudokn-scraper"}
        )
        response.raise_for_status()
        return response.text

    fetch = fetcher if fetcher is not None else _default_fetcher

    try:
        lastmod, children = parse_sitemap_xml(fetch(sitemap_url))
        for child_url in children[:_SITEMAP_INDEX_CHILD_CAP]:
            try:
                child_lastmod, _ = parse_sitemap_xml(fetch(child_url))
                lastmod.update(child_lastmod)
            except Exception as e:
                logger.debug("child sitemap %s failed: %s", child_url, e)
        return {"fetched": True, "url": sitemap_url, "lastmod": lastmod, "error": None}
    except Exception as e:
        return {
            "fetched": False,
            "url": sitemap_url,
            "lastmod": {},
            "error": f"{type(e).__name__}: {e}",
        }


def assemble_manifest(
    *,
    text_format: str,
    start_url: str,
    final_landing_url: str,
    started_at: datetime,
    finished_at: Optional[datetime],
    pages: Mapping[str, dict],
    discovered: Sequence[str],
    failed: Mapping[str, str],
    sitemap: Optional[dict] = None,
    skipped_by_extension: Optional[Mapping[str, str]] = None,
    pdfs: Optional[Mapping[str, dict]] = None,
    include_pdfs: bool = False,
    soft_token_cutoff: Optional[int] = None,
    cutoff_reached: bool = False,
) -> dict:
    """The manifest dict (schema ``MANIFEST_VERSION``). Pure.

    *pages*: ``url -> {"sha256", "chars", "depth"}`` captured PRE-dedup by the
    worker. *discovered*: every URL the crawl ever saw (scraped or not).
    *failed*: ``url -> error_type``. *skipped_by_extension*: ``url -> ext``
    for links filtered by SKIP_EXTENSIONS (grouped by extension in the
    output). *pdfs*: per fetched-PDF meta when ``include_pdfs`` was on.
    ``s3_text_version_id`` starts None and is filled by the upload path so
    the sidecar names the text version it describes.
    """
    scraped_urls = sorted(pages)
    not_scraped = sorted(set(discovered) - set(pages) - set(failed))
    by_ext: dict[str, list[str]] = {}
    for url, ext in sorted((skipped_by_extension or {}).items()):
        by_ext.setdefault(ext, []).append(url)
    return {
        "manifest_version": MANIFEST_VERSION,
        "text_format": text_format,
        "start_url": start_url,
        "final_landing_url": final_landing_url,
        "scrape_started_at": started_at.astimezone(timezone.utc).isoformat(),
        "scrape_finished_at": (
            finished_at.astimezone(timezone.utc).isoformat() if finished_at else None
        ),
        "pages": {url: dict(pages[url]) for url in scraped_urls},
        "discovered_not_scraped": not_scraped,
        "failed": dict(sorted(failed.items())),
        "url_set_hashes": {
            "scraped": url_set_hash(scraped_urls),
            "discovered": url_set_hash(list(discovered)),
        },
        "sitemap": sitemap
        or {"fetched": False, "url": None, "lastmod": {}, "error": "not attempted"},
        "skipped_by_extension": by_ext,
        "include_pdfs": include_pdfs,
        # crawl-scope soft stop (2026-09-03): the configured estimated-token
        # cutoff (None = crawl everything) and whether this scrape hit it —
        # a cutoff_reached manifest describes a deliberately partial crawl.
        "soft_token_cutoff": soft_token_cutoff,
        "cutoff_reached": cutoff_reached,
        "pdfs": {url: dict(meta) for url, meta in sorted((pdfs or {}).items())},
        "s3_text_version_id": None,
    }
