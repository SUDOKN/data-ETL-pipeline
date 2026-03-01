"""
Utility for detecting and removing duplicate content across scraped page blocks.

Duplicate detection is exact-match only. It handles:
- Full duplicate pages (identical blocks)
- Repeated headers (identical leading lines shared across pages)
- Repeated footers (identical trailing lines shared across pages)

Memory-safety notes (designed for inputs up to ~1 GB+):
- Full-page identity uses SHA-256 hashes, NOT the raw block text as the set key.
- Body lines for header/footer detection are extracted one block at a time and
  only the *reference* set (first block's lines) is kept in memory alongside
  O(1) counters — every subsequent block is streamed and immediately discarded.
- Suffix detection walks indices instead of reversing the lists (avoids a copy).
- Output is assembled into a single ``io.StringIO`` buffer so Python never needs
  to hold more than one rebuilt block + the output buffer in memory at once.
- A ``deduplicate_scraped_content_stream`` generator is also provided for
  callers that want to process/write output incrementally without building one
  giant return string.

Header/footer detection uses a majority-vote threshold rather than requiring
100% agreement across all blocks. This handles real-world files where a minority
of blocks are error pages (e.g. Cloudflare challenges) that lack the site's
normal navigation/footer boilerplate.
"""

import hashlib
import io
import logging
from typing import Generator, Iterator, List, Tuple

logger = logging.getLogger(__name__)

# Minimum number of non-empty body lines a repeated block must share to be
# considered a common header or footer.
_MIN_REPEATED_LINES = 3

# Fraction of blocks that must share a header/footer line for it to be stripped.
# 0.95 means the line must appear in at least 95% of blocks.
# This tolerates minority outlier blocks (e.g. error/challenge pages) that lack
# the site's normal boilerplate without preventing detection.
_HEADER_FOOTER_THRESHOLD = 0.95

_SEPARATOR = "##################################################"


# ─────────────────────────────────────────────────────────────────────────────
# Low-level helpers
# ─────────────────────────────────────────────────────────────────────────────

def _hash_block(block: str) -> str:
    """
    Return a hex SHA-256 digest of the *body content only* of a block.

    The URL line is intentionally excluded so that two pages with identical
    content but different URLs are still detected as full-page duplicates.
    """
    sep_end = block.find("\n", block.find(_SEPARATOR))
    if sep_end == -1:
        body = block
    else:
        url_end = block.find("\n", sep_end + 1)
        body = block[url_end + 1:] if url_end != -1 else block[sep_end + 1:]
    return hashlib.sha256(body.strip().encode("utf-8", errors="replace")).hexdigest()


def _iter_blocks(combined: str) -> Iterator[str]:
    """
    Yield per-URL blocks one at a time without building the full block list.

    Uses ``str.find`` to locate each separator so only two positions are live
    at any point — the current start and the next separator position.
    """
    sep = _SEPARATOR
    sep_len = len(sep)
    start = combined.find(sep)
    if start == -1:
        return
    while True:
        next_start = combined.find(sep, start + sep_len)
        if next_start == -1:
            block = combined[start:]
            if block.strip():
                yield block
            break
        block = combined[start:next_start]
        if block.strip():
            yield block
        start = next_start


def _body_lines_of_block(block: str) -> List[str]:
    """
    Return the body lines of a block, skipping the separator and URL line.

    Block structure:
        ##################################################   <- separator  ┐
        <url>                                               <- URL line    ├ handled by _prefix_lines
                                                            <- blank line  ┘
        <body lines …>                                      <- returned as-is (blank lines preserved)

    Internal blank lines within the body are intentionally kept so that
    spacing between content sections is not lost when the block is rebuilt.
    Only the structural leading blank (between URL and body) is excluded —
    that is already captured by _prefix_lines.

    Lines are returned with their original line endings (keepends=True) so
    that ``_rebuild_block`` can join them back verbatim without any loss of
    trailing blank lines or spacing.
    """
    sep_end = block.find("\n", block.find(_SEPARATOR))
    if sep_end == -1:
        return []
    url_end = block.find("\n", sep_end + 1)
    if url_end == -1:
        return []
    # skip the blank line that _prefix_lines already captures
    body_start = block.find("\n", url_end + 1)
    if body_start == -1:
        return []
    body = block[body_start + 1:]
    return body.splitlines(keepends=True)


def _prefix_lines(block: str) -> str:
    """
    Return the separator + URL line + blank line portion of a block
    (everything up to and including the blank line before the body).

    Block structure:
        ##################################################   <- separator  ┐
        <url>                                               <- URL line    ├ prefix
                                                            <- blank line  ┘
        <body lines …>
    """
    sep_end = block.find("\n", block.find(_SEPARATOR))
    if sep_end == -1:
        return block
    url_end = block.find("\n", sep_end + 1)
    if url_end == -1:
        return block
    # include the blank line that follows the URL
    blank_end = block.find("\n", url_end + 1)
    if blank_end == -1:
        return block[: url_end + 1]
    return block[: blank_end + 1]


# ─────────────────────────────────────────────────────────────────────────────
# Header / footer detection — majority-vote
# ─────────────────────────────────────────────────────────────────────────────

def _detect_common_header_footer(
    blocks: List[str],
    min_lines: int,
    threshold: float = _HEADER_FOOTER_THRESHOLD,
) -> Tuple[List[str], List[str]]:
    """
    Find the longest common leading and trailing body-line sequences shared by
    at least *threshold* fraction of all blocks (default 95%).

    Algorithm:
    - Pre-extract body lines for every block once into a list (O(N) extractions).
    - Pick the longest extracted lines list as the reference so short/empty
      outlier blocks don't cap the candidate length.
    - For each candidate line position scan forward (header) and backward
      (footer), counting exact matches against the reference line. Stop
      extending as soon as the match rate falls below *threshold*.

    Returns ``(common_header_lines, common_footer_lines)``.
    """
    if len(blocks) < 2:
        return [], []

    # Extract once — avoids O(N * L) repeated calls inside the position loops.
    all_lines: List[List[str]] = [_body_lines_of_block(b) for b in blocks]

    # Pick longest as reference so outlier short/empty blocks don't cap us.
    ref_lines = max(all_lines, key=len)
    if not ref_lines:
        return [], []

    total = len(all_lines)
    required = threshold * total  # minimum hit count

    # ── header: scan forward ────────────────────────────────────────────────
    header_len = 0
    for pos in range(len(ref_lines)):
        candidate = ref_lines[pos]
        matches = sum(
            1 for lines in all_lines
            if len(lines) > pos and lines[pos] == candidate
        )
        if matches >= required:
            header_len += 1
        else:
            break

    # ── footer: scan backward ───────────────────────────────────────────────
    footer_len = 0
    ref_tail = len(ref_lines)
    for i in range(1, ref_tail + 1):
        candidate = ref_lines[ref_tail - i]
        matches = sum(
            1 for lines in all_lines
            if len(lines) >= i and lines[-i] == candidate
        )
        if matches >= required:
            footer_len += 1
        else:
            break

    common_header = ref_lines[:header_len] if header_len >= min_lines else []
    common_footer = ref_lines[ref_tail - footer_len:] if footer_len >= min_lines else []

    # Edge case: header and footer overlap on very short blocks — prefer header.
    if common_header and common_footer:
        if len(common_header) + len(common_footer) >= ref_tail:
            common_footer = []

    return common_header, common_footer


# ─────────────────────────────────────────────────────────────────────────────
# Block rebuilder
# ─────────────────────────────────────────────────────────────────────────────

def _rebuild_block(
    block: str,
    common_header: List[str],
    common_footer: List[str],
) -> str:
    """Strip *common_header* and *common_footer* from *block* and return rebuilt text.

    The prefix (separator + URL + blank line) is preserved exactly as-is.
    """
    prefix = _prefix_lines(block)   # ends with the blank line \n
    body   = _body_lines_of_block(block)

    h = len(common_header)
    f = len(common_footer)

    if h and body[:h] == common_header:
        body = body[h:]
    if f and body[-f:] == common_footer:
        body = body[:-f]

    return prefix + "".join(body)


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

def deduplicate_scraped_content_stream(
    combined: str,
) -> Generator[str, None, None]:
    """
    Memory-efficient generator variant of :func:`deduplicate_scraped_content`.

    Yields one cleaned block string at a time so callers can write directly to
    a file or stream without buffering the entire output.

    Usage::

        with open("out.txt", "w") as f:
            for chunk in deduplicate_scraped_content_stream(raw):
                f.write(chunk)
    """
    if not combined or not combined.strip():
        yield combined
        return

    # ── Pass 1: deduplicate blocks — keep body only for the first occurrence.
    # Subsequent duplicates are retained as URL-only stubs so the reader knows
    # every URL that was scraped, but without repeating the body content.
    seen_hashes: set = set()
    unique_blocks: List[str] = []   # full blocks  (body intact)
    stub_blocks:   List[str] = []   # separator + URL only  (body was a duplicate)
    all_blocks:    List[str] = []   # preserves original order
    removed = 0

    for block in _iter_blocks(combined):
        h = _hash_block(block)
        if h not in seen_hashes:
            seen_hashes.add(h)
            unique_blocks.append(block)
            all_blocks.append(("full", block))
        else:
            removed += 1
            # Build a stub: separator + URL + blank line + note, no body
            prefix = _prefix_lines(block)   # already ends with blank line \n
            stub = f"{prefix}[duplicate — content identical to a previously scraped page]\n"
            stub_blocks.append(stub)
            all_blocks.append(("stub", stub))
            logger.debug("Duplicate block stubbed (same page content): %s",
                         block.splitlines()[1].strip() if len(block.splitlines()) > 1 else "")

    del seen_hashes  # free the hash set — no longer needed

    if removed:
        logger.info(f"Stubbed {removed} fully-duplicate page block(s) (URL kept, body removed).")

    if len(unique_blocks) < 2:
        # Not enough unique blocks to compare — emit everything as-is
        for kind, block in all_blocks:
            yield block
        return

    # ── Pass 2: detect shared header / footer across unique (full) blocks ───
    common_header, common_footer = _detect_common_header_footer(
        unique_blocks, _MIN_REPEATED_LINES
    )

    if common_header:
        logger.info(
            f"Stripping {len(common_header)}-line repeated header from "
            f"{len(unique_blocks)} blocks."
        )
    if common_footer:
        logger.info(
            f"Stripping {len(common_footer)}-line repeated footer from "
            f"{len(unique_blocks)} blocks."
        )

    # ── Pass 3: stream blocks in original order ──────────────────────────────
    # Stubs are emitted as-is; full blocks have header/footer stripped.
    for kind, block in all_blocks:
        if kind == "stub":
            yield block
        elif common_header or common_footer:
            yield _rebuild_block(block, common_header, common_footer)
        else:
            yield block


def deduplicate_scraped_content(combined: str) -> str:
    """
    Remove exact-match duplicate content from a combined scraped-content string.

    For very large inputs prefer :func:`deduplicate_scraped_content_stream` to
    avoid buffering the full output in memory.

    Steps:
      1. Split into per-URL blocks (streaming — no full block list from split).
      2. Drop fully-duplicate blocks using SHA-256 hashes (not raw text keys).
      3. Identify common header/footer lines shared by >= 95% of blocks and
         strip them from every block that contains them.
      4. Re-assemble into a ``StringIO`` buffer and return as a single string.

    Args:
        combined: The raw combined string produced by the scraper.

    Returns:
        The de-duplicated content string.
    """
    buf = io.StringIO()
    for chunk in deduplicate_scraped_content_stream(combined):
        buf.write(chunk)
    return buf.getvalue()
