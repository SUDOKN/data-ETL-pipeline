"""Excluded (legal) pages are dropped from the text BEFORE the phrase pipelines
chunk it (2026-08-23, user decision after run 20260823T034518: blanked-on-the-wire
legal pages plus page alignment idled 47% of steelcraft's 40k-token budget). The
knob lives on ``ChunkingStrategy`` with the rule version pinned, so a change is
metadata drift; the prefill applies it on every run and hands the trimmed text
down the chain; the dump's provenance shows what was removed."""

from datetime import datetime
from types import SimpleNamespace

import pytest
from scraper.models.s3.scraped_text_file import ScrapedTextFile

from core.models.chunking_strat import ChunkingStrategy, wide
from core.models.pipeline_nodes.base.base_node import PipelineContext
from core.models.pipeline_nodes.base.base_prefill_node import PrefillNode
from core.utils.extraction_dump_util import build_run_provenance
from core.utils.floor_scan import PAGE_EXCLUSION_VERSION, drop_excluded_pages

SEP = "#" * 50
TEXT = (
    f"{SEP}\nhttps://acme.example/about\n\nWe machine Aluminum.\n"
    f"{SEP}\nhttps://acme.example/privacy-policy\n\ncookies and more cookies\n"
    f"{SEP}\nhttps://acme.example/products\n\nAluminum parts.\n"
)
T0 = datetime(2026, 8, 23, 4, 0, 0)


class _File(ScrapedTextFile):
    @classmethod
    async def can_delete_version(cls, s3_version_id: str) -> bool:
        return False


def _file(text: str = TEXT) -> _File:
    return _File(
        s3_version_id="v1",
        last_modified_on=T0,
        subject_unique_id="acme.example",
        text=text,
        num_tokens=123,
        urls_scraped=3,
        urls_failed=0,
        etld1_accessible_at="acme.example",
        success_rate=1.0,
        is_valid=True,
    )


class _FieldType:
    name = "products"

    def __hash__(self) -> int:
        return hash(self.name)


class _Prefill(PrefillNode):
    def __init__(self, strategy: ChunkingStrategy) -> None:
        self.field_type = _FieldType()
        self.chunk_strategy = strategy

    async def execute(self, *args, **kwargs) -> None: ...


def _strategy(**overrides) -> ChunkingStrategy:
    base = dict(overlap=0, max_tokens_per_chunk=100, max_chunks=2, search_divisor=2)
    return ChunkingStrategy(**{**base, **overrides})


def test_the_wide_strategy_drops_excluded_pages_under_the_pinned_rule_version():
    assert wide.drop_excluded_pages is True
    assert wide.page_exclusion_version == PAGE_EXCLUSION_VERSION
    plain = ChunkingStrategy(overlap=0, max_tokens_per_chunk=10, max_chunks=1)
    assert plain.drop_excluded_pages is False


def test_the_knob_and_the_rule_version_come_together():
    with pytest.raises(ValueError, match="page_exclusion_version"):
        _strategy(drop_excluded_pages=True)
    with pytest.raises(ValueError, match="only meaningful"):
        _strategy(page_exclusion_version="1")
    on = _strategy(drop_excluded_pages=True, page_exclusion_version="1")
    assert on.model_dump()["page_exclusion_version"] == "1"
    # an old stored strategy (no knob) loads with the defaults — and differs, so
    # the staleness guard re-defers it rather than resuming under other bounds
    old = ChunkingStrategy.model_validate(
        {"overlap": 0, "max_tokens_per_chunk": 100, "max_chunks": 2, "search_divisor": 2}
    )
    assert old.drop_excluded_pages is False and old.model_dump() != on.model_dump()


def test_prefill_hands_the_trimmed_text_down_and_records_what_it_dropped():
    strategy = _strategy(drop_excluded_pages=True, page_exclusion_version=PAGE_EXCLUSION_VERSION)
    node = _Prefill(strategy)
    original = _file()
    ctx = PipelineContext(subject_text=original.text)
    trimmed = node.apply_page_exclusion(original, ctx)
    expected = drop_excluded_pages(TEXT)
    assert trimmed is not original and trimmed.text == expected.text
    assert "privacy-policy" not in trimmed.text and "cookies" not in trimmed.text
    # the S3 object's identity and count travel unchanged; only the text differs
    assert trimmed.s3_version_id == "v1" and trimmed.num_tokens == 123
    assert trimmed.subject_unique_id == "acme.example"
    assert original.text == TEXT  # the frozen original is untouched
    assert ctx.subject_text == trimmed.text
    assert ctx.page_exclusion is not None and [p.url for p in ctx.page_exclusion.dropped] == [
        "https://acme.example/privacy-policy"
    ]
    # a second application (a resumed run) is a no-op on the trimmed text
    again = node.apply_page_exclusion(trimmed, PipelineContext())
    assert again.text == trimmed.text


def test_prefill_leaves_the_text_alone_when_the_knob_is_off():
    node = _Prefill(_strategy())
    original = _file()
    ctx = PipelineContext()
    assert node.apply_page_exclusion(original, ctx) is original
    assert ctx.subject_text == TEXT and ctx.page_exclusion is None


def test_prefill_refuses_a_strategy_pinned_to_another_rule_version():
    node = _Prefill(_strategy(drop_excluded_pages=True, page_exclusion_version="0"))
    with pytest.raises(ValueError, match="rule version"):
        node.apply_page_exclusion(_file(), PipelineContext())


def test_run_provenance_shows_the_dropped_pages():
    exclusion = drop_excluded_pages(TEXT)
    metadata = SimpleNamespace(model_dump=lambda mode: {"m": 1})
    with_pages = build_run_provenance(
        metadata=metadata, scraped_text_file=_file(), partial=False, page_exclusion=exclusion
    )
    block = with_pages["scraped_text"]["excluded_pages"]  # type: ignore[index]
    assert block["version"] == PAGE_EXCLUSION_VERSION
    assert [p["url"] for p in block["pages"]] == ["https://acme.example/privacy-policy"]
    assert block["chars_before"] - block["chars_removed"] == block["chars_after"]
    assert block["chars_after"] == len(exclusion.text)
    without = build_run_provenance(metadata=metadata, scraped_text_file=_file(), partial=False)
    assert "excluded_pages" not in without["scraped_text"]  # type: ignore[operator]
