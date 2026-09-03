"""PDF -> Markdown for linked site documents (2026-08-29, user decision:
built but FLAGGED OFF — ``ScraperService(include_pdfs=False)`` — until the
corpus-wide PDF inventory the manifest now records has been measured).

WHY. The crawl's link collector already finds PDF links; ``SKIP_EXTENSIONS``
silently dropped them. Measured on 22 cached corpus pages: 78 unique PDF
links, and they are CERTIFICATES — ISO 9001/14001, IATF 16949 — primary
evidence for conformity_attestations that the pipeline has never seen.

WHAT THIS RENDERS. A fetched PDF becomes one ordinary page block in the
scrape (separator / its URL / body), so dedup, chunking, page alignment and
the legal-page URL exclusion all apply to PDFs with no new code. The body is
the text layer per page (blank line between pages, no page markers), with
detected tables re-emitted as pipe tables and their regions excluded from the
prose text where the library allows. Rendering is deterministic: same PDF
bytes -> same markdown.

HONEST LIMITS (phase 1):

- SCANNED PDFs have no text layer — exactly the certificates this exists for
  are often image scans. They render to nothing; the manifest records
  ``text_layer: false`` so the gap is visible, and the PDF's URL alone
  ("ISO-9001-Certificate.pdf") is already weak evidence. OCR is deliberately
  deferred (heavy dependency, slow, and a measurement should justify it).
- Running per-page headers/footers inside a PDF repeat in the output; the
  corpus-level dedup does not see inside one block. Accepted for now.
- Catalogs run to hundreds of pages against a 2x20k-token chunk budget:
  extraction stops at ``PDF_MAX_PAGES`` / ``PDF_MAX_CHARS`` and the manifest
  records ``truncated: true``.

Caps (frozen with this module; changing them changes output shape for
enabled crawls, so treat edits like format-rule edits):
``PDF_MAX_BYTES`` 20MB download cap, ``PDF_MAX_PAGES`` 40, ``PDF_MAX_CHARS``
200k, ``MAX_PDFS_PER_SITE`` 50 (sorted URL order, deterministic).
"""

from __future__ import annotations

import io
import logging
import re
from typing import Callable, Optional

logger = logging.getLogger(__name__)

PDF_MAX_BYTES = 20 * 1024 * 1024
PDF_MAX_PAGES = 40
PDF_MAX_CHARS = 200_000
MAX_PDFS_PER_SITE = 50
_FETCH_TIMEOUT_S = 30

# Below this many extracted characters a PDF is treated as having no usable
# text layer (a scan): a page or two of OCR-less noise, not content.
_TEXT_LAYER_MIN_CHARS = 40


class PdfTooLargeError(Exception):
    pass


class NotAPdfError(Exception):
    pass


def looks_like_pdf(data: bytes) -> bool:
    return data[:5] == b"%PDF-"


def fetch_pdf(url: str, fetcher: Optional[Callable[[str], bytes]] = None) -> bytes:
    """The PDF's bytes, size-capped. *fetcher* exists for tests; the default
    streams via ``requests`` and aborts past ``PDF_MAX_BYTES``. Raises
    ``PdfTooLargeError`` / ``NotAPdfError`` / network errors — the caller
    records failures in the manifest, they never fail the scrape."""
    if fetcher is not None:
        data = fetcher(url)
        if len(data) > PDF_MAX_BYTES:
            raise PdfTooLargeError(f"{len(data)} bytes > {PDF_MAX_BYTES}")
    else:  # pragma: no cover - network default
        import requests

        chunks: list[bytes] = []
        size = 0
        with requests.get(
            url, timeout=_FETCH_TIMEOUT_S, stream=True,
            headers={"User-Agent": "sudokn-scraper"},
        ) as response:
            response.raise_for_status()
            for chunk in response.iter_content(chunk_size=65536):
                size += len(chunk)
                if size > PDF_MAX_BYTES:
                    raise PdfTooLargeError(f">{PDF_MAX_BYTES} bytes")
                chunks.append(chunk)
        data = b"".join(chunks)
    if not looks_like_pdf(data):
        raise NotAPdfError("missing %PDF- magic bytes")
    return data


def tables_to_pipes(rows: list[list[Optional[str]]]) -> str:
    """One extracted table (rows of cells, None for empty) as a GFM pipe
    table — same conventions as the HTML converter: first row is the header
    row, interior pipes escaped, whitespace collapsed."""
    cleaned = [
        [re.sub(r"\s+", " ", (cell or "")).strip().replace("|", "\\|") for cell in row]
        for row in rows
        if any((cell or "").strip() for cell in row)
    ]
    if not cleaned:
        return ""
    width = max(len(r) for r in cleaned)
    lines = ["| " + " | ".join(r + [""] * (width - len(r))) + " |" for r in cleaned]
    lines.insert(1, "|" + "---|" * width)
    return "\n".join(lines)


def pdf_to_markdown(data: bytes) -> tuple[str, dict]:
    """``(markdown_text, meta)`` for one PDF's bytes. Pure and deterministic.

    ``meta``: ``pages_total``, ``pages_rendered``, ``tables``, ``chars``,
    ``text_layer`` (False = scan, see module docstring), ``truncated``.
    """
    import pdfplumber

    parts: list[str] = []
    tables_found = 0
    truncated = False
    with pdfplumber.open(io.BytesIO(data)) as pdf:
        pages_total = len(pdf.pages)
        pages = pdf.pages[:PDF_MAX_PAGES]
        truncated = pages_total > PDF_MAX_PAGES
        for page in pages:
            try:
                page_tables = page.find_tables()
                prose_region = page
                for table in page_tables:
                    # exclude each table's region from the prose text so rows
                    # are not emitted twice; any failure falls back to the
                    # full page text
                    prose_region = prose_region.outside_bbox(table.bbox)
                text = prose_region.extract_text() or ""
                parts.append(text.strip())
                for table in page_tables:
                    rendered = tables_to_pipes(table.extract())
                    if rendered:
                        tables_found += 1
                        parts.append(rendered)
            except Exception:
                logger.debug("page fell back to plain text", exc_info=True)
                try:
                    parts.append((page.extract_text() or "").strip())
                except Exception:
                    parts.append("")
            if sum(len(p) for p in parts) > PDF_MAX_CHARS:
                truncated = True
                break

    text = "\n\n".join(p for p in parts if p)
    if len(text) > PDF_MAX_CHARS:
        text = text[:PDF_MAX_CHARS]
        truncated = True
    text = re.sub(r"[ \t]+\n", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    meta = {
        "pages_total": pages_total,
        "pages_rendered": min(pages_total, len(pages)),
        "tables": tables_found,
        "chars": len(text),
        "text_layer": len(text) >= _TEXT_LAYER_MIN_CHARS,
        "truncated": truncated,
    }
    return text, meta
