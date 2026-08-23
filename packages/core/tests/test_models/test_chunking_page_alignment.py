"""2026-08-22 (proposal N1, accepted): under ``ChunkingStrategy.align_to_page_headers``
both the macro chunks and the search sub-windows open at a page header — the
scraper's ``##########`` separator line, the page URL on the line after — so
the location stage always sees which page its text belongs to. Measured on the
sample subjects before the change: every one of 39 windows started mid-page.
Only a page longer than the limit still splits (its continuations are the one
mid-page case, handled on the wire by ``wire_window_text``)."""

import pytest

import llm_providers.utils.chunk_util as chunk_util
from core.models.chunking_strat import (
    PRODUCT_CHUNKING_STRAT,
    ChunkingStrategy,
    chunk_break_predicate,
    derive_search_sub_bounds,
    wide,
)
from core.utils.floor_scan import is_page_header_line
from llm_providers.models.llm_model import GPT_4o_mini
from llm_providers.utils.chunk_util import get_chunks_respecting_line_boundaries

SEP = "#" * 50


@pytest.fixture(autouse=True)
def _one_token_per_line(monkeypatch):
    monkeypatch.setattr(
        chunk_util.litellm,
        "token_counter",
        lambda model, text: max(1, len(text.splitlines())) if text else 0,
    )


def _site(pages: list[tuple[str, int]]) -> str:
    """Scraper layout: separator line, URL line, two blank lines, body lines."""
    return "".join(
        f"{SEP}\n{url}\n\n\n" + "".join(f"{url.rsplit('/', 1)[-1]} line {i}\n" for i in range(n))
        for url, n in pages
    )


def _first_line(text: str, bounds: str) -> str:
    start = int(bounds.split(":")[0])
    return text[start:].split("\n", 1)[0]


def _strategy(**overrides) -> ChunkingStrategy:
    base = dict(overlap=0, max_tokens_per_chunk=25, max_chunks=10, search_divisor=2)
    return ChunkingStrategy(**{**base, **overrides})


async def _chunks(text: str, strat: ChunkingStrategy) -> dict[str, str]:
    return await get_chunks_respecting_line_boundaries(
        text=text,
        soft_limit_tokens=strat.max_tokens_per_chunk,
        overlap_ratio=strat.overlap,
        max_chunks=strat.max_chunks,
        llm_model=GPT_4o_mini,
        break_before=chunk_break_predicate(strat),
    )


async def _subs(chunk_bounds: str, chunk_text: str, strat: ChunkingStrategy) -> list[str]:
    return await derive_search_sub_bounds(
        chunk_bounds=chunk_bounds,
        chunk_text=chunk_text,
        chunk_strategy=strat,
        llm_model=GPT_4o_mini,
    )


def test_page_header_predicate_matches_the_separator_line_only():
    assert is_page_header_line(SEP)
    assert is_page_header_line(SEP + "\n")
    assert is_page_header_line("  " + SEP + "  \r\n")
    assert not is_page_header_line("#########")  # fewer than 10
    assert not is_page_header_line("https://example.com/\n")
    assert not is_page_header_line("# heading\n")
    assert not is_page_header_line("")


def test_flag_default_off_and_on_for_the_phrase_strategies():
    plain = ChunkingStrategy(overlap=0, max_tokens_per_chunk=100, max_chunks=1)
    assert plain.align_to_page_headers is False
    assert chunk_break_predicate(plain) is None
    assert wide.align_to_page_headers is True
    assert PRODUCT_CHUNKING_STRAT.align_to_page_headers is True
    assert chunk_break_predicate(wide) is is_page_header_line


@pytest.mark.asyncio
async def test_macro_chunks_and_sub_windows_both_open_at_a_page_header():
    # 6 pages of 10 lines (4 header lines + 6 body lines) = 60 lines. Chunk
    # limit 25 → aligned chunks of 2 pages; divisor 2 → sub-windows of ~12
    # lines, i.e. one page each.
    text = _site([(f"https://x.com/p{i}", 6) for i in range(6)])
    strat = _strategy(align_to_page_headers=True)
    chunk_map = await _chunks(text, strat)
    assert len(chunk_map) == 3
    assert all(_first_line(text, b) == SEP for b in chunk_map)
    # chunks tile the text
    cursor = 0
    for b in chunk_map:
        s, e = (int(x) for x in b.split(":"))
        assert s == cursor
        cursor = e
    assert cursor == len(text)
    for chunk_bounds, chunk_text in chunk_map.items():
        subs = await _subs(chunk_bounds, chunk_text, strat)
        assert len(subs) == 2
        assert all(_first_line(text, b) == SEP for b in subs)


@pytest.mark.asyncio
async def test_without_the_flag_windows_start_mid_page():
    text = _site([(f"https://x.com/p{i}", 6) for i in range(6)])
    chunk_map = await _chunks(text, _strategy())
    assert not all(_first_line(text, b) == SEP for b in chunk_map)


@pytest.mark.asyncio
async def test_an_oversize_page_splits_and_only_its_continuation_is_mid_page():
    # page 1 has 30 body lines (> the 12-line window); pages 0 and 2 are small
    text = _site([("https://x.com/p0", 3), ("https://x.com/big", 30), ("https://x.com/p2", 3)])
    strat = _strategy(max_tokens_per_chunk=48, search_divisor=4, align_to_page_headers=True)
    chunk_bounds, chunk_text = next(iter((await _chunks(text, strat)).items()))
    subs = await _subs(chunk_bounds, chunk_text, strat)
    firsts = [_first_line(text, b) for b in subs]
    assert firsts[0] == SEP  # p0 (+ the head of big, if it fits)
    assert SEP in firsts[1:]  # p2 opens its own window
    assert any(f != SEP for f in firsts)  # the big page's continuation is mid-page
    # every continuation that is mid-page lies INSIDE the big page
    big_start = text.index("https://x.com/big") - len(SEP) - 1
    big_end = text.index(SEP, big_start + 1)
    for b, f in zip(subs, firsts, strict=True):
        if f != SEP:
            assert big_start < int(b.split(":")[0]) < big_end
