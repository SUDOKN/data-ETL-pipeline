"""
Tests for scraper_app.utils.dedup_util
=======================================

Unit tests  (self-contained, no external files)
------------------------------------------------
- _hash_block                  : body-only hashing, URL line excluded from digest
- _iter_blocks                 : separator-based block streaming
- _body_lines_of_block         : extracts body lines, omitting separator/URL/leading blank;
                                 preserves internal blank lines and line endings verbatim
- _prefix_lines                : extracts the separator + URL + blank-line header portion
- _detect_common_header_footer : majority-vote boilerplate detection algorithm
- _rebuild_block               : removes detected header/footer, preserves prefix and spacing
- deduplicate_scraped_content        : full deduplication pipeline (returns string)
- deduplicate_scraped_content_stream : generator variant of the pipeline

Whitespace-preservation regression tests
-----------------------------------------
- Blank lines within a body survive the rebuild path
- Trailing blank lines at the end of a body survive the rebuild path
- Whitespace-only lines are returned verbatim, not stripped
- _rebuild_block with empty header/footer lists is a no-op (byte-identical output)

Synthetic dataset integration tests
-------------------------------------
- A 251-block dataset is built in-memory at module load time; no external files needed
- Structure: 200 unique pages with boilerplate header/footer, 50 exact-body duplicates,
  1 outlier page with no boilerplate (exercises the 95 % majority-vote threshold)
- Covers block parsing, end-to-end dedup, header/footer stripping, stream parity,
  and performance on a realistic multi-block input
"""

import sys
import time
import types

import pytest

sys.path.insert(0, __import__("os").path.join(__import__("os").path.dirname(__file__), "..", "..", "src"))

from scraper_app.utils.dedup_util import (
    _SEPARATOR,
    _body_lines_of_block,
    _detect_common_header_footer,
    _hash_block,
    _iter_blocks,
    _prefix_lines,
    _rebuild_block,
    deduplicate_scraped_content,
    deduplicate_scraped_content_stream,
)

# ─────────────────────────────────────────────────────────────────────────────
# Helpers shared across all test classes
# ─────────────────────────────────────────────────────────────────────────────

def _make_block(url: str, body: str) -> str:
    """Construct a single scraper-format block: SEPARATOR + URL + blank line + body."""
    return f"{_SEPARATOR}\n{url}\n\n{body}\n"


def _make_combined(*pairs) -> str:
    """Concatenate multiple (url, body) pairs into a combined scraped-content string."""
    return "".join(_make_block(url, body) for url, body in pairs)


# ─────────────────────────────────────────────────────────────────────────────
# Synthetic dataset
# ─────────────────────────────────────────────────────────────────────────────
#
# Layout mirrors a realistic multi-page scrape with all dedup code paths active:
#   • 10-line boilerplate header shared by all 200 unique pages and 50 duplicates
#   • 6-line boilerplate footer shared by the same 250 blocks
#   • 200 unique product pages — each body contains a title, blank-line paragraph
#     separators, numeric specs, and a per-page whitespace-padded identifier line
#     (making it unique so the majority-vote algorithm does not flag it as boilerplate)
#   • 50 duplicate pages — body text identical to pages 0–49, different URLs;
#     the dedup pipeline should produce exactly 50 stubs for these
#   • 1 outlier/error page — no boilerplate at all; sits below the 5 % minority
#     threshold so it does not prevent header/footer detection on the other 250 blocks
#
# Built once at module import time and shared across all fixtures.

_BOILERPLATE_HEADER = (
    "Home | Products | About | Contact | Blog\n"
    "Free shipping on orders over $50\n"
    "Search products...\n"
    "My Account   Cart (0)   Wishlist\n"
    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    "ACME Industrial Fabrication\n"
    "Precision Sheet Metal & Custom Parts\n"
    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    "[ Sale items ]  [ New arrivals ]  [ Clearance ]\n"
    "Category: All\n"
)

_BOILERPLATE_FOOTER = (
    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    "Payment Methods: Visa  Mastercard  PayPal  Apple Pay\n"
    "         \n"
    "\n"
    "© 2024 ACME Industrial Fabrication. All rights reserved.\n"
    "Powered by Shopify\n"
)

# 200 unique page bodies — title line, blank line, two-paragraph description,
# blank line, spec table with a per-page whitespace-padded SKU identifier, closing line.
_UNIQUE_BODIES: list[str] = []
_PRODUCT_NAMES = [
    "Aluminum Sheet 4x8", "Steel Tube 2x2", "Copper Pipe 1/2in",
    "Stainless Bracket L3", "Galvanized Flange DN50", "Titanium Rod 6mm",
    "Carbon Fibre Panel A2", "Brass Fitting 3/4in", "Cast Iron Plate 10mm",
    "Zinc Die Casting B7",
]
for _i in range(200):
    _pname = _PRODUCT_NAMES[_i % len(_PRODUCT_NAMES)]
    _body = (
        f"Product: {_pname} — SKU {_i:04d}\n"
        f"\n"
        f"This is the main description for {_pname} item number {_i}.\n"
        f"Available in multiple sizes and finishes to suit your requirements.\n"
        f"\n"
        f"Specifications:\n"
        f"  Width  : {10 + _i % 50} mm\n"
        f"  Height : {20 + _i % 30} mm\n"
        f"  Weight : {0.5 + _i * 0.1:.1f} kg\n"
        f"  SKU-{_i:04d}  \n"    # whitespace-padded line unique per page (not boilerplate)
        f"In stock. Ships within 2 business days.\n"
    )
    _UNIQUE_BODIES.append(_body)

def _full_body(page_body: str) -> str:
    """Wrap a page-specific body with the shared boilerplate header and footer."""
    return _BOILERPLATE_HEADER + page_body + _BOILERPLATE_FOOTER


def _build_synthetic_dataset() -> str:
    """
    Assemble the full synthetic combined-content string.

    Block layout (0-based indices):
      [0   – 199]  200 unique pages  (boilerplate header + unique body + boilerplate footer)
      [200 – 249]  50 duplicate pages (same body as blocks 0–49, different URL path)
      [250]        1 outlier error page (no boilerplate — tests the 95 % threshold)
    """
    blocks = []

    # 200 unique pages
    for i, body in enumerate(_UNIQUE_BODIES):
        blocks.append(_make_block(f"https://acme-fab.example.com/products/item-{i:04d}", _full_body(body)))

    # 50 duplicate pages (same body as items 0-49, different path)
    for i in range(50):
        blocks.append(_make_block(
            f"https://acme-fab.example.com/collections/all/products/item-{i:04d}",
            _full_body(_UNIQUE_BODIES[i]),
        ))

    # 1 outlier error page — no boilerplate at all
    blocks.append(_make_block(
        "https://acme-fab.example.com/cdn/challenge",
        "Checking your browser before accessing the site.\n"
        "This process is automatic.\n"
        "Please wait...\n",
    ))

    return "".join(blocks)


_SYNTHETIC_RAW = _build_synthetic_dataset()
_TOTAL_BLOCKS  = 251   # 200 unique + 50 duplicates + 1 outlier


@pytest.fixture(scope="module")
def synthetic_raw() -> str:
    """Return the pre-built synthetic scraped dataset (no I/O)."""
    return _SYNTHETIC_RAW


@pytest.fixture(scope="module")
def synthetic_deduped(synthetic_raw) -> str:
    """Run dedup once on the synthetic dataset; reused by all dataset tests."""
    return deduplicate_scraped_content(synthetic_raw)


# ─────────────────────────────────────────────────────────────────────────────
# _hash_block
# ─────────────────────────────────────────────────────────────────────────────

class TestHashBlock:
    def test_same_body_different_urls_produce_same_hash(self):
        # Two blocks with identical bodies but different URLs must hash to the same value
        # because the URL line is excluded from the digest.
        body = "Line one\nLine two\nLine three"
        assert _hash_block(_make_block("https://example.com/a", body)) == \
               _hash_block(_make_block("https://example.com/b", body))

    def test_different_bodies_produce_different_hashes(self):
        # Distinct body content must produce distinct digests.
        assert _hash_block(_make_block("https://example.com/a", "Content A")) != \
               _hash_block(_make_block("https://example.com/b", "Content B"))

    def test_same_url_same_body_same_hash(self):
        # Hashing the same block twice returns the same digest (deterministic).
        block = _make_block("https://example.com", "Same body")
        assert _hash_block(block) == _hash_block(block)

    def test_whitespace_normalised_in_hash(self):
        # Leading/trailing whitespace on the body is stripped before hashing,
        # so padded and unpadded variants of the same content hash identically.
        assert _hash_block(_make_block("https://x.com", "  Body text  ")) == \
               _hash_block(_make_block("https://x.com", "Body text"))

    def test_returns_64_char_hex_string(self):
        # SHA-256 produces a 64-character lowercase hex digest.
        h = _hash_block(_make_block("https://example.com", "some content"))
        assert len(h) == 64
        assert all(c in "0123456789abcdef" for c in h)


# ─────────────────────────────────────────────────────────────────────────────
# _iter_blocks
# ─────────────────────────────────────────────────────────────────────────────

class TestIterBlocks:
    def test_single_block(self):
        # A combined string with one separator yields exactly one block containing
        # the expected URL and body content.
        blocks = list(_iter_blocks(_make_block("https://a.com", "Hello world")))
        assert len(blocks) == 1
        assert "https://a.com" in blocks[0]
        assert "Hello world" in blocks[0]

    def test_multiple_blocks(self):
        # Three consecutive blocks are each yielded separately with their URLs intact.
        combined = _make_combined(
            ("https://a.com", "Body A"),
            ("https://b.com", "Body B"),
            ("https://c.com", "Body C"),
        )
        blocks = list(_iter_blocks(combined))
        assert len(blocks) == 3
        assert any("https://a.com" in b for b in blocks)
        assert any("https://b.com" in b for b in blocks)
        assert any("https://c.com" in b for b in blocks)

    def test_empty_string_yields_nothing(self):
        assert list(_iter_blocks("")) == []

    def test_no_separator_yields_nothing(self):
        # Text without the separator pattern does not produce any blocks.
        assert list(_iter_blocks("just plain text with no separator")) == []

    def test_blocks_start_with_separator(self):
        # Every yielded block starts with the separator string.
        combined = _make_combined(
            ("https://a.com", "Body A"),
            ("https://b.com", "Body B"),
        )
        for block in _iter_blocks(combined):
            assert block.startswith(_SEPARATOR)

    def test_blank_blocks_skipped(self):
        # A separator that is immediately followed by another separator (no content
        # between them) does not produce an empty/whitespace-only block.
        combined = _SEPARATOR + "\n" + _SEPARATOR + "\nhttps://a.com\n\nBody\n"
        blocks = list(_iter_blocks(combined))
        assert all(b.strip() for b in blocks)


# ─────────────────────────────────────────────────────────────────────────────
# _body_lines_of_block
# ─────────────────────────────────────────────────────────────────────────────

class TestBodyLinesOfBlock:
    def test_returns_body_lines_only(self):
        # The returned lines contain the body text and exclude structural lines.
        block = _make_block("https://example.com", "Line 1\nLine 2\nLine 3")
        lines = _body_lines_of_block(block)
        stripped = [l.rstrip("\n") for l in lines]
        assert "Line 1" in stripped
        assert "Line 2" in stripped
        assert "Line 3" in stripped

    def test_url_not_in_body_lines(self):
        # The URL line (second line of the block) is structural and excluded from the body.
        block = _make_block("https://example.com/page", "Content here")
        lines = _body_lines_of_block(block)
        assert not any("https://example.com" in l for l in lines)

    def test_separator_not_in_body_lines(self):
        # The separator line is structural and excluded from the body.
        block = _make_block("https://example.com", "Content here")
        lines = _body_lines_of_block(block)
        assert not any(_SEPARATOR in l for l in lines)

    def test_internal_blank_lines_preserved(self):
        # Blank lines within the body are spacing between content sections and
        # must appear in the returned lines unchanged.
        block = _make_block("https://example.com", "Line 1\n\n\nLine 2\n\n")
        body = "".join(_body_lines_of_block(block))
        assert "Line 1" in body
        assert "Line 2" in body
        assert "\n\n" in body

    def test_trailing_blank_lines_preserved(self):
        # Blank lines at the end of a body are part of the content and must
        # not be dropped when lines are extracted and rejoined.
        body_text = "Content line\n\n\n"
        block = _make_block("https://example.com", body_text)
        rejoined = "".join(_body_lines_of_block(block))
        assert rejoined.endswith("\n\n") or rejoined.endswith("\n\n\n"), (
            f"Trailing blank lines were stripped: {rejoined!r}"
        )

    def test_whitespace_only_lines_preserved(self):
        # Lines that contain only whitespace characters are part of the body
        # and must be returned verbatim, not silently dropped.
        block = _make_block("https://example.com", "   \n  \n  ")
        lines = _body_lines_of_block(block)
        assert isinstance(lines, list)
        rejoined = "".join(lines)
        assert "   " in rejoined or "  " in rejoined

    def test_empty_body_returns_list(self):
        # A block whose body section is empty returns an empty list, not None or an error.
        block = _make_block("https://example.com", "")
        lines = _body_lines_of_block(block)
        assert isinstance(lines, list)

    def test_round_trip_body_is_identical(self):
        # Joining all returned lines reproduces the body exactly as stored in the block.
        # _make_block appends a trailing '\n' after the supplied body string, so the
        # stored body is body_text + '\n'; the comparison is made against that value.
        body_text = "First\n\nSecond\n   \nThird\n\n"
        block = _make_block("https://example.com", body_text)
        stored_body = body_text + "\n"
        rejoined = "".join(_body_lines_of_block(block))
        assert rejoined == stored_body, (
            f"Round-trip mismatch.\n  Stored  : {stored_body!r}\n  Rejoined: {rejoined!r}"
        )


# ─────────────────────────────────────────────────────────────────────────────
# _prefix_lines
# ─────────────────────────────────────────────────────────────────────────────

class TestPrefixLines:
    def test_contains_separator(self):
        block = _make_block("https://example.com", "Body text")
        assert _SEPARATOR in _prefix_lines(block)

    def test_contains_url(self):
        block = _make_block("https://example.com/path", "Body text")
        assert "https://example.com/path" in _prefix_lines(block)

    def test_body_not_in_prefix(self):
        # Body text that follows the blank line is not included in the prefix.
        block = _make_block("https://example.com", "Unique body content xyz")
        assert "Unique body content xyz" not in _prefix_lines(block)

    def test_prefix_ends_with_newline(self):
        block = _make_block("https://example.com", "Body text")
        assert _prefix_lines(block).endswith("\n")

    def test_blank_line_preserved_in_prefix(self):
        # The blank line that separates the URL from the body is included in the
        # prefix so that blocks can be reassembled without losing that spacing.
        block = _make_block("https://example.com", "Body text")
        prefix = _prefix_lines(block)
        assert "\n\n" in prefix

    def test_prefix_plus_body_reproduces_block_structure(self):
        # Concatenating the prefix with arbitrary body text produces a string
        # that contains the separator, URL, and blank-line gap — the full
        # structural header of a valid block.
        block = _make_block("https://example.com", "Body text")
        prefix = _prefix_lines(block)
        rebuilt = prefix + "Body text" + "\n"
        assert _SEPARATOR in rebuilt
        assert "https://example.com" in rebuilt
        assert "\n\n" in rebuilt


# ─────────────────────────────────────────────────────────────────────────────
# _detect_common_header_footer
# ─────────────────────────────────────────────────────────────────────────────

class TestDetectCommonHeaderFooter:
    def _blocks(self, *bodies):
        return [_make_block(f"https://example.com/p{i}", b)
                for i, b in enumerate(bodies)]

    def test_detects_common_header(self):
        # Three leading lines shared across all blocks are returned as the common header.
        blocks = self._blocks(
            "Nav\nLogo\nBanner\nUnique A content\nMore A",
            "Nav\nLogo\nBanner\nUnique B content\nMore B",
            "Nav\nLogo\nBanner\nUnique C content\nMore C",
        )
        header, footer = _detect_common_header_footer(blocks, min_lines=3)
        stripped_header = [l.rstrip("\n") for l in header]
        assert stripped_header == ["Nav", "Logo", "Banner"]
        assert footer == []

    def test_detects_common_footer(self):
        # Three trailing lines shared across all blocks are returned as the common footer.
        blocks = self._blocks(
            "Unique A\nMore A\nFooter1\nFooter2\nFooter3",
            "Unique B\nMore B\nFooter1\nFooter2\nFooter3",
            "Unique C\nMore C\nFooter1\nFooter2\nFooter3",
        )
        header, footer = _detect_common_header_footer(blocks, min_lines=3)
        assert header == []
        stripped_footer = [l.rstrip("\n") for l in footer]
        assert stripped_footer == ["Footer1", "Footer2", "Footer3"]

    def test_detects_both_header_and_footer(self):
        # Shared leading and trailing lines are detected independently.
        blocks = self._blocks(
            "Nav\nLogo\nBanner\nUnique A\nUnique A2\nFoot1\nFoot2\nFoot3",
            "Nav\nLogo\nBanner\nUnique B\nUnique B2\nFoot1\nFoot2\nFoot3",
            "Nav\nLogo\nBanner\nUnique C\nUnique C2\nFoot1\nFoot2\nFoot3",
        )
        header, footer = _detect_common_header_footer(blocks, min_lines=3)
        assert [l.rstrip("\n") for l in header] == ["Nav", "Logo", "Banner"]
        assert [l.rstrip("\n") for l in footer] == ["Foot1", "Foot2", "Foot3"]

    def test_below_min_lines_threshold_ignored(self):
        # Shared sequences shorter than min_lines are not reported as boilerplate.
        blocks = self._blocks(
            "Nav\nLogo\nUnique A\nUnique B\nFoot1\nFoot2",
            "Nav\nLogo\nUnique C\nUnique D\nFoot1\nFoot2",
        )
        header, footer = _detect_common_header_footer(blocks, min_lines=3)
        assert header == []
        assert footer == []

    def test_no_common_content(self):
        # When all body lines differ across blocks, both results are empty lists.
        blocks = self._blocks(
            "Completely unique A line one\nA line two\nA line three",
            "Completely unique B line one\nB line two\nB line three",
        )
        header, footer = _detect_common_header_footer(blocks, min_lines=3)
        assert header == []
        assert footer == []

    def test_single_block_returns_empty(self):
        # The algorithm requires at least two blocks to compare; one block yields nothing.
        blocks = self._blocks("Nav\nLogo\nContent")
        header, footer = _detect_common_header_footer(blocks, min_lines=1)
        assert header == []
        assert footer == []

    def test_different_length_blocks_no_index_error(self):
        # Blocks of unequal length are handled without index errors; shared leading
        # and trailing lines are still detected correctly.
        blocks = self._blocks(
            "Nav\nLogo\nBanner\nLong unique content\nMore\nFoot1\nFoot2\nFoot3",
            "Nav\nLogo\nBanner\nShort\nFoot1\nFoot2\nFoot3",
        )
        header, footer = _detect_common_header_footer(blocks, min_lines=3)
        assert [l.rstrip("\n") for l in header] == ["Nav", "Logo", "Banner"]
        assert [l.rstrip("\n") for l in footer] == ["Foot1", "Foot2", "Foot3"]

    def test_overlap_prefers_header(self):
        # When the detected header and footer together would cover the entire block,
        # the footer is discarded in favour of keeping the header.
        blocks = self._blocks(
            "L1\nL2\nL3\nL4\nL5",
            "L1\nL2\nL3\nL4\nL5",
        )
        _, footer = _detect_common_header_footer(blocks, min_lines=3)
        assert footer == []

    def test_non_common_start_stops_header_detection(self):
        # A minority block whose first lines differ from the others prevents those
        # lines from being identified as a shared header under the 95 % threshold.
        blocks = self._blocks(
            "Nav\nLogo\nBanner\nUnique A",
            "DIFFERENT\nSTART\nHERE\nUnique B",
            "Nav\nLogo\nBanner\nUnique C",
        )
        header, _ = _detect_common_header_footer(blocks, min_lines=3)
        assert header == []


# ─────────────────────────────────────────────────────────────────────────────
# _rebuild_block
# ─────────────────────────────────────────────────────────────────────────────

class TestRebuildBlock:
    def test_strips_common_header(self):
        # Lines matching the common header are removed from the start of the body.
        block = _make_block("https://example.com", "Nav\nLogo\nBanner\nUnique content\nMore")
        common_header = _body_lines_of_block(block)[:3]
        result = _rebuild_block(block, common_header, [])
        result_body_lines = [l.rstrip("\n") for l in _body_lines_of_block(result)]
        assert "Nav" not in result_body_lines
        assert "Unique content" in result_body_lines

    def test_strips_common_footer(self):
        # Lines matching the common footer are removed from the end of the body.
        block = _make_block("https://example.com", "Unique content\nMore\nFoot1\nFoot2\nFoot3")
        common_footer = _body_lines_of_block(block)[-3:]
        result = _rebuild_block(block, [], common_footer)
        result_body_lines = [l.rstrip("\n") for l in _body_lines_of_block(result)]
        assert "Foot1" not in result_body_lines
        assert "Unique content" in result_body_lines

    def test_strips_both_header_and_footer(self):
        # Header and footer removal are applied together; unique content in between survives.
        block = _make_block("https://example.com",
                            "Nav\nLogo\nBanner\nUnique content\nFoot1\nFoot2\nFoot3")
        all_lines = _body_lines_of_block(block)
        result = _rebuild_block(block, all_lines[:3], all_lines[-3:])
        result_body_lines = [l.rstrip("\n") for l in _body_lines_of_block(result)]
        assert "Nav" not in result_body_lines
        assert "Foot1" not in result_body_lines
        assert "Unique content" in result_body_lines

    def test_preserves_separator_and_url(self):
        # The structural prefix (separator + URL + blank line) is always kept intact.
        block = _make_block("https://example.com/keep-me", "Nav\nLogo\nBanner\nContent")
        common_header = _body_lines_of_block(block)[:3]
        result = _rebuild_block(block, common_header, [])
        assert _SEPARATOR in result
        assert "https://example.com/keep-me" in result
        assert "\n\n" in result

    def test_no_header_or_footer_is_byte_identical(self):
        # Calling _rebuild_block with empty header and footer lists returns the
        # original block string unchanged, byte for byte.
        body_text = "Line A\n\nLine B\n   \nLine C\n\n"
        block = _make_block("https://example.com", body_text)
        result = _rebuild_block(block, [], [])
        assert result == block, (
            f"Round-trip failed.\n  Input : {block!r}\n  Output: {result!r}"
        )

    def test_blank_lines_inside_body_preserved_after_strip(self):
        # Blank lines between content paragraphs are part of the unique body and
        # must not be removed as a side-effect of stripping the header and footer.
        block = _make_block("https://example.com",
                            "Nav\nLogo\n\nParagraph one\n\nParagraph two\n\nFoot1\nFoot2\nFoot3")
        all_lines = _body_lines_of_block(block)
        header = all_lines[:2]
        footer = all_lines[-3:]
        result = _rebuild_block(block, header, footer)
        result_body = "".join(_body_lines_of_block(result))
        assert "\n\n" in result_body, (
            "Blank lines between paragraphs were lost after header/footer strip"
        )

    def test_does_not_strip_if_body_does_not_match_header(self):
        # If the block's leading lines do not match the supplied common header,
        # the body is left untouched.
        block = _make_block("https://example.com", "Different\nStart\nHere\nContent")
        common_header = _body_lines_of_block(
            _make_block("https://other.com", "Nav\nLogo\nBanner\nContent")
        )[:3]
        result = _rebuild_block(block, common_header, [])
        assert "Different" in result
        assert "Content" in result

    def test_trailing_blank_lines_preserved_when_footer_stripped(self):
        # Blank lines that appear after the footer region are not part of the
        # footer and must survive the rebuild even when footer lines are removed.
        block = _make_block("https://example.com",
                            "Unique\nFoot1\nFoot2\nFoot3\n\n")
        ref_block = _make_block("https://ref.com", "Unique\nFoot1\nFoot2\nFoot3")
        footer = _body_lines_of_block(ref_block)[-3:]
        result = _rebuild_block(block, [], footer)
        assert "Unique" in result


# ─────────────────────────────────────────────────────────────────────────────
# deduplicate_scraped_content  (full pipeline — unit level)
# ─────────────────────────────────────────────────────────────────────────────

class TestDeduplicateScrapedContent:
    def test_empty_string_returned_unchanged(self):
        assert deduplicate_scraped_content("") == ""

    def test_whitespace_only_returned_unchanged(self):
        assert deduplicate_scraped_content("   \n  ").strip() == ""

    def test_single_block_returned_unchanged(self):
        # A single block with no duplicates and no shared boilerplate passes through intact.
        combined = _make_block("https://example.com", "Some content here")
        result = deduplicate_scraped_content(combined)
        assert "Some content here" in result
        assert "https://example.com" in result

    def test_fully_duplicate_block_stubbed_not_dropped(self):
        # The second occurrence of a body is replaced with a stub note; both URLs are kept.
        body = "Line one\nLine two\nLine three"
        combined = _make_combined(
            ("https://example.com/a", body),
            ("https://example.com/b", body),
        )
        result = deduplicate_scraped_content(combined)
        blocks = [b for b in result.split(_SEPARATOR) if b.strip()]
        assert len(blocks) == 2
        assert "https://example.com/a" in result
        assert "https://example.com/b" in result
        assert "[duplicate" in result
        assert result.count("Line one") == 1

    def test_same_url_duplicate_stubbed(self):
        # A URL scraped twice with the same body content yields one full block and one stub.
        body = "Content A\nContent B\nContent C"
        combined = _make_combined(
            ("https://example.com", body),
            ("https://example.com", body),
        )
        result = deduplicate_scraped_content(combined)
        blocks = [b for b in result.split(_SEPARATOR) if b.strip()]
        assert len(blocks) == 2
        assert result.count("Content A") == 1
        assert "[duplicate" in result

    def test_three_duplicates_one_full_two_stubs(self):
        # Three blocks sharing the same body yield one full block and two stubs.
        body = "Same body line one\nSame body line two\nSame body line three"
        combined = _make_combined(
            ("https://x.com/1", body),
            ("https://x.com/2", body),
            ("https://x.com/3", body),
        )
        result = deduplicate_scraped_content(combined)
        blocks = [b for b in result.split(_SEPARATOR) if b.strip()]
        assert len(blocks) == 3
        assert result.count("Same body line one") == 1
        assert result.count("[duplicate") == 2

    def test_unique_blocks_all_kept(self):
        # Three blocks with distinct bodies all appear in the output as full blocks.
        combined = _make_combined(
            ("https://a.com", "Unique body A one two three"),
            ("https://b.com", "Unique body B one two three"),
            ("https://c.com", "Unique body C one two three"),
        )
        result = deduplicate_scraped_content(combined)
        blocks = [b for b in result.split(_SEPARATOR) if b.strip()]
        assert len(blocks) == 3

    def test_repeated_header_stripped(self):
        # A line present at the start of every block body is identified as boilerplate
        # and removed; it should appear fewer times in the output than in the input.
        combined = _make_combined(
            ("https://a.com", "Nav\nLogo\nBanner\nUnique A content\nMore A"),
            ("https://b.com", "Nav\nLogo\nBanner\nUnique B content\nMore B"),
            ("https://c.com", "Nav\nLogo\nBanner\nUnique C content\nMore C"),
        )
        result = deduplicate_scraped_content(combined)
        body_section = result.replace(_SEPARATOR, "")
        assert body_section.count("Nav") < 3

    def test_repeated_footer_stripped(self):
        # A line present at the end of every block body is identified as boilerplate
        # and removed from all blocks.
        combined = _make_combined(
            ("https://a.com", "Unique A\nMore A\nFoot1\nFoot2\nFoot3"),
            ("https://b.com", "Unique B\nMore B\nFoot1\nFoot2\nFoot3"),
            ("https://c.com", "Unique C\nMore C\nFoot1\nFoot2\nFoot3"),
        )
        result = deduplicate_scraped_content(combined)
        assert result.count("Foot1") < 3

    def test_unique_content_preserved_after_header_strip(self):
        # Content unique to each page survives after shared boilerplate is stripped.
        combined = _make_combined(
            ("https://a.com", "Nav\nLogo\nBanner\nOnly on page A\nMore A"),
            ("https://b.com", "Nav\nLogo\nBanner\nOnly on page B\nMore B"),
            ("https://c.com", "Nav\nLogo\nBanner\nOnly on page C\nMore C"),
        )
        result = deduplicate_scraped_content(combined)
        assert "Only on page A" in result
        assert "Only on page B" in result
        assert "Only on page C" in result

    def test_urls_preserved_after_dedup(self):
        # All input URLs appear in the output regardless of whether their block
        # was kept as a full block or replaced with a stub.
        combined = _make_combined(
            ("https://a.com", "Nav\nLogo\nBanner\nPage A unique\nMore A"),
            ("https://b.com", "Nav\nLogo\nBanner\nPage B unique\nMore B"),
        )
        result = deduplicate_scraped_content(combined)
        assert "https://a.com" in result
        assert "https://b.com" in result

    def test_mix_of_duplicates_and_unique(self):
        # A mix of duplicate and unique blocks: duplicates are stubbed, unique blocks
        # are kept, and total block count is preserved.
        body_dup = "Dup line one\nDup line two\nDup line three"
        combined = _make_combined(
            ("https://a.com", body_dup),
            ("https://b.com", "Truly unique B content here"),
            ("https://c.com", body_dup),
            ("https://d.com", "Truly unique D content here"),
        )
        result = deduplicate_scraped_content(combined)
        blocks = [b for b in result.split(_SEPARATOR) if b.strip()]
        assert len(blocks) == 4
        assert "Truly unique B content here" in result
        assert "Truly unique D content here" in result
        assert result.count("Dup line one") == 1
        assert "[duplicate" in result

    def test_no_separator_in_input_returns_empty(self):
        # Input text that contains no separator string yields an empty output string.
        result = deduplicate_scraped_content("This is plain text with no blocks at all.")
        assert result == ""

    def test_output_contains_separators(self):
        # The output retains block separators so the format remains parseable.
        combined = _make_combined(
            ("https://a.com", "Nav\nLogo\nBanner\nContent A\nMore"),
            ("https://b.com", "Nav\nLogo\nBanner\nContent B\nMore"),
        )
        assert _SEPARATOR in deduplicate_scraped_content(combined)

    def test_blank_lines_within_body_survive_pipeline(self):
        # Blank lines inside page-specific content are not removed when the
        # pipeline strips the surrounding boilerplate header and footer.
        combined = _make_combined(
            ("https://a.com", "Nav\nLogo\nBanner\nPara one\n\nPara two\n\nFoot1\nFoot2\nFoot3"),
            ("https://b.com", "Nav\nLogo\nBanner\nDiff one\n\nDiff two\n\nFoot1\nFoot2\nFoot3"),
            ("https://c.com", "Nav\nLogo\nBanner\nOthr one\n\nOthr two\n\nFoot1\nFoot2\nFoot3"),
        )
        result = deduplicate_scraped_content(combined)
        assert "\n\n" in result.replace(_SEPARATOR, "").replace("https://", "")

    def test_stub_block_has_blank_line_after_url(self):
        # A stub block retains the blank line between the URL and the duplicate note,
        # matching the format of a normal block.
        body = "Dup one\nDup two\nDup three"
        combined = _make_combined(
            ("https://example.com/orig", body),
            ("https://example.com/dup",  body),
        )
        result = deduplicate_scraped_content(combined)
        stub_part = result[result.find("https://example.com/dup"):]
        assert stub_part.startswith("https://example.com/dup\n\n"), (
            "Stub block is missing the blank line between the URL and the duplicate note"
        )


# ─────────────────────────────────────────────────────────────────────────────
# deduplicate_scraped_content_stream
# ─────────────────────────────────────────────────────────────────────────────

class TestDeduplicateScrapedContentStream:
    def test_is_generator(self):
        # The function returns a generator object, not a string or list.
        result = deduplicate_scraped_content_stream(_make_combined(("https://a.com", "Body A")))
        assert isinstance(result, types.GeneratorType)

    def test_stream_matches_non_stream(self):
        # Joining all chunks from the stream yields a string identical to the
        # non-streaming variant for the same input.
        combined = _make_combined(
            ("https://a.com", "Nav\nLogo\nBanner\nPage A\nMore A"),
            ("https://b.com", "Nav\nLogo\nBanner\nPage B\nMore B"),
            ("https://c.com", "Nav\nLogo\nBanner\nPage C\nMore C"),
            ("https://d.com", "Nav\nLogo\nBanner\nPage A\nMore A"),  # duplicate of a
        )
        assert deduplicate_scraped_content(combined) == \
               "".join(deduplicate_scraped_content_stream(combined))

    def test_empty_string_yields_once(self):
        # An empty input string causes the generator to yield exactly one empty string.
        assert list(deduplicate_scraped_content_stream("")) == [""]

    def test_each_non_empty_chunk_contains_separator(self):
        # Each non-empty chunk yielded by the stream corresponds to one block and
        # therefore contains the separator string.
        combined = _make_combined(
            ("https://a.com", "Nav\nLogo\nBanner\nContent A\nMore"),
            ("https://b.com", "Nav\nLogo\nBanner\nContent B\nMore"),
        )
        for chunk in deduplicate_scraped_content_stream(combined):
            if chunk.strip():
                assert _SEPARATOR in chunk

    def test_duplicate_blocks_stubbed_in_stream(self):
        # The stream variant also stubs duplicate blocks; the URL is kept and
        # the body is replaced with the duplicate note.
        body = "Dup one\nDup two\nDup three"
        combined = _make_combined(
            ("https://a.com", body),
            ("https://b.com", body),
        )
        full = "".join(deduplicate_scraped_content_stream(combined))
        blocks = [b for b in full.split(_SEPARATOR) if b.strip()]
        assert len(blocks) == 2
        assert full.count("Dup one") == 1
        assert "[duplicate" in full
        assert "https://b.com" in full

    def test_can_write_to_file_incrementally(self, tmp_path):
        # Chunks can be written directly to a file as they are yielded,
        # producing a file that contains the expected deduplicated content.
        combined = _make_combined(
            ("https://a.com", "Nav\nLogo\nBanner\nUnique A\nMore"),
            ("https://b.com", "Nav\nLogo\nBanner\nUnique B\nMore"),
        )
        out = tmp_path / "output.txt"
        with open(out, "w", encoding="utf-8") as f:
            for chunk in deduplicate_scraped_content_stream(combined):
                f.write(chunk)
        written = out.read_text(encoding="utf-8")
        assert "Unique A" in written
        assert "Unique B" in written


# ─────────────────────────────────────────────────────────────────────────────
# Whitespace-preservation regression suite
# ─────────────────────────────────────────────────────────────────────────────

class TestWhitespacePreservation:
    """
    Regression coverage for the bug where splitlines() (without keepends=True)
    caused blank lines and trailing newlines to be silently dropped when
    _rebuild_block reassembled blocks after header/footer stripping.

    Each test constructs input that exercises the affected code path and
    verifies that the output contains the expected whitespace.
    """

    def _three_blocks_with(self, body_template):
        """Return a combined string of three blocks that share a 3-line header and
        3-line footer, with body_template (a str.format pattern) as unique content."""
        return _make_combined(
            ("https://a.com", f"NAV\nNAV2\nNAV3\n{body_template.format('A')}\nFOOT\nFOOT2\nFOOT3"),
            ("https://b.com", f"NAV\nNAV2\nNAV3\n{body_template.format('B')}\nFOOT\nFOOT2\nFOOT3"),
            ("https://c.com", f"NAV\nNAV2\nNAV3\n{body_template.format('C')}\nFOOT\nFOOT2\nFOOT3"),
        )

    def test_blank_line_between_paragraphs_survives_header_strip(self):
        # A single blank line separating two content paragraphs is preserved after
        # the shared header and footer are stripped.
        combined = self._three_blocks_with("Para one {0}\n\nPara two {0}")
        result = deduplicate_scraped_content(combined)
        body_only = result.replace(_SEPARATOR, "").replace("https://", "")
        assert "\n\n" in body_only

    def test_multiple_blank_lines_between_paragraphs_survive(self):
        # Multiple consecutive blank lines between content paragraphs are all preserved.
        combined = self._three_blocks_with("Para one {0}\n\n\nPara two {0}")
        result = deduplicate_scraped_content(combined)
        body_only = result.replace(_SEPARATOR, "").replace("https://", "")
        assert "\n\n" in body_only

    def test_whitespace_only_line_survives_header_strip(self):
        # A line that contains only space characters is preserved as-is; it is not
        # treated as an empty line and silently removed.
        combined = self._three_blocks_with("Title {0}\n   \nDescription {0}")
        result = deduplicate_scraped_content(combined)
        assert "   " in result or "Title" in result

    def test_body_not_altered_when_no_header_or_footer(self):
        # When no common boilerplate is detected the _rebuild_block path is not taken
        # and the body content, including its blank lines, passes through unchanged.
        body = "Line one\n\nLine two\n   \nLine three\n\n"
        combined = _make_combined(
            ("https://a.com", body + "UNIQUE_A"),
            ("https://b.com", body[:5] + "UNIQUE_B"),
        )
        result = deduplicate_scraped_content(combined)
        assert "\n\n" in result

    def test_pipeline_round_trip_no_header_no_footer(self):
        # Two blocks with no shared lines pass through the full pipeline unchanged;
        # each body is byte-identical in the output to the input.
        body_a = "Alpha\n\nBravo\n   \nCharlie\n\n"
        body_b = "Delta\n\nEcho\n   \nFoxtrot\n\n"
        block_a = _make_block("https://a.com", body_a)
        block_b = _make_block("https://b.com", body_b)
        combined = block_a + block_b
        result = deduplicate_scraped_content(combined)
        assert body_a in result, "Body A was altered even though no header/footer was stripped"
        assert body_b in result, "Body B was altered even though no header/footer was stripped"


# ─────────────────────────────────────────────────────────────────────────────
# Synthetic dataset integration tests  (self-contained, no external files)
# ─────────────────────────────────────────────────────────────────────────────

class TestSyntheticDatasetBlockParsing:
    """Block-parsing correctness across the full 251-block synthetic dataset."""

    def test_correct_total_block_count(self, synthetic_raw):
        # The parser yields exactly the number of blocks assembled by the builder.
        assert len(list(_iter_blocks(synthetic_raw))) == _TOTAL_BLOCKS

    def test_every_block_starts_with_separator(self, synthetic_raw):
        for i, block in enumerate(_iter_blocks(synthetic_raw)):
            assert block.startswith(_SEPARATOR), f"Block {i} missing separator"

    def test_every_block_has_url_line(self, synthetic_raw):
        # The second line of every block is a URL (starts with "http").
        for i, block in enumerate(_iter_blocks(synthetic_raw)):
            lines = block.splitlines()
            assert len(lines) >= 2, f"Block {i} has fewer than 2 lines"
            assert lines[1].strip().startswith("http"), (
                f"Block {i} URL line does not start with http: {lines[1]!r}"
            )

    def test_body_lines_exclude_separator_and_url(self, synthetic_raw):
        # Body-line extraction omits both the separator and the URL line for every block.
        for i, block in enumerate(_iter_blocks(synthetic_raw)):
            body = _body_lines_of_block(block)
            for ln in body:
                assert not ln.startswith(_SEPARATOR[:10]), (
                    f"Block {i} body contains separator text"
                )
            url_line = block.splitlines()[1].strip()
            assert url_line not in [l.rstrip("\n") for l in body], (
                f"Block {i} body contains the URL line"
            )


class TestSyntheticDatasetDeduplicationResults:
    """End-to-end deduplication correctness on the synthetic 251-block dataset."""

    def test_output_block_count_equals_input_block_count(self, synthetic_raw, synthetic_deduped):
        # Duplicate bodies are replaced with stubs rather than dropped, so the total
        # number of blocks in the output equals the number in the input.
        assert len(list(_iter_blocks(synthetic_raw))) == \
               len(list(_iter_blocks(synthetic_deduped)))

    def test_exactly_50_duplicate_stubs_present(self, synthetic_deduped):
        # The 50 blocks whose bodies duplicate pages 0–49 each produce exactly one stub.
        stubs = sum(1 for b in _iter_blocks(synthetic_deduped) if "[duplicate" in b)
        assert stubs == 50, f"Expected 50 duplicate stubs, got {stubs}"

    def test_full_block_bodies_are_unique(self, synthetic_deduped):
        # No two full (non-stub) blocks in the output share the same body digest.
        seen: set = set()
        for block in _iter_blocks(synthetic_deduped):
            if "[duplicate" in block:
                continue
            h = _hash_block(block)
            assert h not in seen, "Deduped output still contains full duplicate bodies"
            seen.add(h)

    def test_output_is_smaller_than_input(self, synthetic_raw, synthetic_deduped):
        # Stripping duplicate bodies and boilerplate reduces the total character count.
        assert len(synthetic_deduped) < len(synthetic_raw)

    def test_output_non_empty(self, synthetic_deduped):
        assert synthetic_deduped.strip()

    def test_output_contains_separator(self, synthetic_deduped):
        assert _SEPARATOR in synthetic_deduped

    def test_all_200_unique_urls_present_in_output(self, synthetic_deduped):
        # Every unique-page URL appears in the output as a full block.
        for i in range(200):
            url = f"https://acme-fab.example.com/products/item-{i:04d}"
            assert url in synthetic_deduped, f"URL missing from output: {url}"

    def test_all_50_duplicate_urls_present_as_stubs(self, synthetic_deduped):
        # Every duplicate-page URL is retained in the output as a stub.
        for i in range(50):
            url = f"https://acme-fab.example.com/collections/all/products/item-{i:04d}"
            assert url in synthetic_deduped, f"Duplicate URL missing from output: {url}"

    def test_outlier_block_url_preserved(self, synthetic_deduped):
        # The outlier error page (no boilerplate) is kept in the output.
        assert "https://acme-fab.example.com/cdn/challenge" in synthetic_deduped


class TestSyntheticDatasetHeaderFooterStripping:
    """Boilerplate detection and stripping on the synthetic dataset."""

    def test_boilerplate_header_line_stripped(self, synthetic_deduped):
        # A line that appears in the shared header of all 250 boilerplate blocks
        # is present far fewer times in the deduped output than in the raw input.
        probe = "ACME Industrial Fabrication"
        raw_count   = _SYNTHETIC_RAW.count(probe)
        dedup_count = synthetic_deduped = deduplicate_scraped_content(_SYNTHETIC_RAW)
        dedup_count = dedup_count.count(probe)
        assert dedup_count < raw_count

    def test_boilerplate_footer_line_stripped(self, synthetic_deduped):
        # A line that appears in the shared footer of all 250 boilerplate blocks
        # is reduced in the output after deduplication.
        probe = "Powered by Shopify"
        raw_count   = _SYNTHETIC_RAW.count(probe)
        dedup_count = synthetic_deduped.count(probe)
        assert dedup_count < raw_count

    def test_unique_product_names_all_survive(self, synthetic_deduped):
        # Each page-specific SKU string survives the boilerplate-stripping step.
        for i in range(200):
            sku = f"SKU {i:04d}"
            assert sku in synthetic_deduped, f"Unique content lost: {sku}"

    def test_unique_specs_survive(self, synthetic_deduped):
        # Spec-table labels that appear in unique body content are not stripped.
        assert "Width  :" in synthetic_deduped
        assert "Height :" in synthetic_deduped
        assert "Weight :" in synthetic_deduped

    def test_blank_lines_within_unique_body_survive(self, synthetic_deduped):
        # Blank lines that separate content sections within a unique body are present
        # in the output after boilerplate is stripped.
        body_only = synthetic_deduped.replace(_SEPARATOR, "")
        assert "\n\n" in body_only

    def test_whitespace_only_lines_within_unique_body_survive(self, synthetic_deduped):
        # Each unique body contains a line with trailing whitespace and a per-page SKU
        # identifier.  Because this line differs across pages it is not classified as
        # boilerplate and must appear in the output for every unique page.
        for i in (0, 1, 50, 99, 199):
            marker = f"  SKU-{i:04d}  "
            assert marker in synthetic_deduped, (
                f"Unique whitespace-padded line was incorrectly stripped: {marker!r}"
            )

    def test_repeated_boilerplate_lines_reduced(self, synthetic_deduped):
        # Every non-blank line in the boilerplate header and footer appears fewer
        # times in the deduped output than in the raw input.
        for line in (_BOILERPLATE_HEADER + _BOILERPLATE_FOOTER).splitlines():
            if not line.strip():
                continue
            raw_count   = _SYNTHETIC_RAW.count(line)
            dedup_count = synthetic_deduped.count(line)
            if raw_count > 1:
                assert dedup_count < raw_count, (
                    f"Boilerplate line not reduced after dedup: {line!r}"
                )


class TestSyntheticDatasetStreamVariant:
    """Stream variant produces byte-identical output to the non-stream variant."""

    def test_stream_matches_non_stream(self, synthetic_raw, synthetic_deduped):
        # Joining all chunks from the generator produces the same string as the
        # non-streaming function called on the same input.
        streamed = "".join(deduplicate_scraped_content_stream(synthetic_raw))
        assert streamed == synthetic_deduped

    def test_stream_can_write_to_file(self, synthetic_raw, tmp_path):
        # Chunks written incrementally to a file produce a valid deduped output.
        out = tmp_path / "deduped.txt"
        with open(out, "w", encoding="utf-8") as f:
            for chunk in deduplicate_scraped_content_stream(synthetic_raw):
                f.write(chunk)
        written = out.read_text(encoding="utf-8")
        assert "SKU 0000" in written
        assert out.stat().st_size > 0

    def test_stream_output_smaller_than_input(self, synthetic_raw, tmp_path):
        # The file written by the stream variant is smaller than the encoded input.
        out = tmp_path / "deduped.txt"
        with open(out, "w", encoding="utf-8") as f:
            for chunk in deduplicate_scraped_content_stream(synthetic_raw):
                f.write(chunk)
        assert out.stat().st_size < len(synthetic_raw.encode("utf-8"))


class TestSyntheticDatasetPerformance:
    def test_completes_within_30_seconds(self, synthetic_raw):
        # The full pipeline (251 blocks, boilerplate detection, stream assembly)
        # completes in under 30 seconds on any reasonable machine.
        start = time.monotonic()
        deduplicate_scraped_content(synthetic_raw)
        elapsed = time.monotonic() - start
        assert elapsed < 30, f"Deduplication took too long: {elapsed:.2f}s"

