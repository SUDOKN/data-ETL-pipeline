"""Tests for pdf_to_markdown (2026-08-29): the flagged-off PDF pipeline.

Fixtures avoid new dev dependencies: the text-layer PDF is handcrafted
minimal PDF bytes (pdfminer's fallback parser scans objects without an xref),
and the "scanned certificate" is a Pillow image saved as PDF — a real PDF
with no text layer, exactly what an ISO-cert scan looks like to extraction.
"""

import io

import pytest

from scraper.utils.pdf_to_markdown import (
    MAX_PDFS_PER_SITE,
    PDF_MAX_BYTES,
    NotAPdfError,
    PdfTooLargeError,
    fetch_pdf,
    looks_like_pdf,
    pdf_to_markdown,
    tables_to_pipes,
)


def _text_pdf(
    text: str = "ISO 9001 certified precision machining and fabrication since 1974",
) -> bytes:
    stream = f"BT /F1 12 Tf 72 720 Td ({text}) Tj ET".encode()
    return (
        b"%PDF-1.4\n"
        b"1 0 obj << /Type /Catalog /Pages 2 0 R >> endobj\n"
        b"2 0 obj << /Type /Pages /Kids [3 0 R] /Count 1 >> endobj\n"
        b"3 0 obj << /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
        b"/Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >> endobj\n"
        b"4 0 obj << /Length " + str(len(stream)).encode() + b" >> stream\n"
        + stream + b"\nendstream endobj\n"
        b"5 0 obj << /Type /Font /Subtype /Type1 /BaseFont /Helvetica >> endobj\n"
        b"trailer << /Root 1 0 R >>\n"
    )


def _scanned_pdf() -> bytes:
    from PIL import Image

    buf = io.BytesIO()
    Image.new("RGB", (200, 100), "white").save(buf, format="PDF")
    return buf.getvalue()


class TestDetection:
    def test_magic_bytes(self):
        assert looks_like_pdf(b"%PDF-1.7 rest")
        assert not looks_like_pdf(b"<html><body>404</body></html>")

    def test_fetch_rejects_non_pdf(self):
        with pytest.raises(NotAPdfError):
            fetch_pdf("https://x.com/a.pdf", fetcher=lambda u: b"<html>not found</html>")

    def test_fetch_rejects_oversize(self):
        big = b"%PDF-" + b"0" * (PDF_MAX_BYTES + 1)
        with pytest.raises(PdfTooLargeError):
            fetch_pdf("https://x.com/a.pdf", fetcher=lambda u: big)

    def test_fetch_passes_valid_pdf_through(self):
        data = _text_pdf()
        assert fetch_pdf("https://x.com/a.pdf", fetcher=lambda u: data) == data


class TestRendering:
    def test_text_layer_extracted(self):
        text, meta = pdf_to_markdown(_text_pdf())
        assert "ISO 9001 certified precision machining" in text
        assert meta["text_layer"] is True
        assert meta["pages_total"] == 1
        assert meta["truncated"] is False

    def test_scanned_pdf_yields_no_text_and_says_so(self):
        text, meta = pdf_to_markdown(_scanned_pdf())
        assert text == ""
        assert meta["text_layer"] is False
        assert meta["pages_total"] == 1

    def test_deterministic(self):
        data = _text_pdf()
        assert pdf_to_markdown(data) == pdf_to_markdown(data)


class TestTables:
    def test_rows_render_as_pipes_with_header_rule(self):
        rendered = tables_to_pipes(
            [["Alloy", "Temper"], ["6061", "T6"], [None, "T651"]]
        )
        assert rendered.split("\n") == [
            "| Alloy | Temper |",
            "|---|---|",
            "| 6061 | T6 |",
            "|  | T651 |",
        ]

    def test_pipes_escaped_and_empty_rows_dropped(self):
        rendered = tables_to_pipes([["A|B"], [None], ["", ""]])
        assert "A\\|B" in rendered
        assert rendered.count("\n") == 1  # header + rule only

    def test_cap_constant_sane(self):
        assert 0 < MAX_PDFS_PER_SITE <= 100


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main([__file__, "-q"]))
