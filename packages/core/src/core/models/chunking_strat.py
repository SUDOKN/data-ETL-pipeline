from collections.abc import Callable
from typing import Optional

from pydantic import BaseModel

from core.utils.floor_scan import PAGE_EXCLUSION_VERSION, is_page_header_line
from llm_providers.models.file_objects.prompt import Prompt
from llm_providers.models.llm_model import LLM_Model
from llm_providers.utils.chunk_util import get_chunks_respecting_line_boundaries


class ChunkingStrategy(BaseModel):
    overlap: float  # must be between [0, 1)
    max_chunks: int
    max_tokens_per_chunk: int
    # How many sub-windows the search + recursive-search stages split each chunk
    # into. The chunk itself (and every stage from phrase_relationship down) is
    # untouched: relationship reads the full chunk text, search reads
    # max_tokens_per_chunk // search_divisor sized windows of it. 1 = search on
    # whole chunks, exactly the pre-divisor behavior.
    search_divisor: int = 1
    # Close chunks AND search sub-windows before a page-header line (the
    # scraper's ``##########`` separator, the URL on the next line) so every
    # window opens at a page start: the location stage then always sees which
    # page its text belongs to, and the request nonce never sits directly above
    # prose (2026-08-22, proposal N1; measured: every one of 39 windows on the
    # sample subjects started mid-page, ~8% more windows once aligned). Only a
    # page longer than the limit still splits — its continuations start
    # mid-page and ``floor_scan.wire_window_text`` prepends the inherited page's
    # header on the wire. Off for the single-shot strategies, where alignment
    # would only trade a tail page for nothing.
    align_to_page_headers: bool = False
    # Drop excluded pages (legal boilerplate by URL path — see
    # ``floor_scan.is_excluded_page``) from the text BEFORE it is chunked, so they
    # cost no budget and can never make a window of their own (2026-08-23, user
    # decision; measured on run 20260823T034518: blanked-on-the-wire legal pages
    # plus page alignment idled 47% of steelcraft's 40k-token budget, real-content
    # coverage 80% → 64%). The phrase prefill nodes apply it and hand the trimmed
    # text down the chain; chunk and window bounds are then offsets into the
    # trimmed text. ``page_exclusion_version`` pins the rule that did the
    # trimming (``floor_scan.PAGE_EXCLUSION_VERSION``): it lives here so that a
    # rule change is metadata drift and fails the resume like any other. Off for
    # the single-stage strategies — an address can sit on an imprint page.
    drop_excluded_pages: bool = False
    page_exclusion_version: Optional[str] = None

    def __init__(self, **data):
        super().__init__(**data)
        if self.overlap < 0 or self.overlap >= 1:
            raise ValueError("Overlap must be between >=0 and <1")
        if self.drop_excluded_pages and not self.page_exclusion_version:
            raise ValueError(
                "drop_excluded_pages requires page_exclusion_version (the rule that trims the text)"
            )
        if not self.drop_excluded_pages and self.page_exclusion_version is not None:
            raise ValueError("page_exclusion_version is only meaningful with drop_excluded_pages")
        if self.max_tokens_per_chunk >= 128000:
            raise ValueError("Max Tokens must be less than 128000")
        if self.search_divisor < 1:
            raise ValueError("search_divisor must be >= 1")


def chunk_break_predicate(
    chunk_strategy: ChunkingStrategy,
) -> Optional[Callable[[str], bool]]:
    """The ``break_before`` predicate the chunkers take under *chunk_strategy*:
    the page-header line when ``align_to_page_headers`` is on, else None (pure
    line-boundary splitting)."""
    return is_page_header_line if chunk_strategy.align_to_page_headers else None


async def derive_search_sub_bounds(
    chunk_bounds: str,
    chunk_text: str,
    chunk_strategy: ChunkingStrategy,
    llm_model: LLM_Model,
) -> list[str]:
    """Absolute ``start:end`` bounds of the search sub-windows of one chunk.

    Bounds share the chunk's coordinate system (character offsets into the full
    subject text), so ``subject_text[start:end]`` yields a sub-window's text
    anywhere downstream without re-chunking. Derived once at prefill and stored
    on the bundle: embedding sub-window request ids happens in stages that don't
    hold the text, so the geometry has to already be there.
    """
    if chunk_strategy.search_divisor == 1:
        return [chunk_bounds]

    chunk_start = int(chunk_bounds.split(":")[0])
    sub_map = await get_chunks_respecting_line_boundaries(
        text=chunk_text,
        soft_limit_tokens=chunk_strategy.max_tokens_per_chunk
        // chunk_strategy.search_divisor,
        overlap_ratio=chunk_strategy.overlap,
        max_chunks=None,  # sub-windows must cover the whole chunk
        llm_model=llm_model,
        break_before=chunk_break_predicate(chunk_strategy),
    )
    bounds = [
        (chunk_start + int(rel_bounds.split(":")[0]), chunk_start + int(rel_bounds.split(":")[1]))
        for rel_bounds in sub_map.keys()
    ]
    return [f"{start}:{end}" for start, end in merge_trailing_remainder(bounds)]


# A trailing sub-window shorter than this fraction of its predecessor is a
# REMAINDER the line-respecting divider left over, not a window: on run
# 20260822T195947 (overlap 0) two such slivers (386 / 775 chars) cost 12 search
# requests and 10 dummy mention requests for 0–2 phrases (proposal P5, accepted
# 2026-08-22). It is merged into its predecessor instead.
REMAINDER_MERGE_RATIO = 0.2


def merge_trailing_remainder(bounds: list[tuple[int, int]]) -> list[tuple[int, int]]:
    """*bounds* (contiguous, in order) with a trailing remainder window — one
    shorter than ``REMAINDER_MERGE_RATIO`` of the window before it — merged
    into that window. Pure; one window, or none, is returned unchanged."""
    if len(bounds) < 2:
        return list(bounds)
    (prev_start, prev_end), (last_start, last_end) = bounds[-2], bounds[-1]
    if (last_end - last_start) < REMAINDER_MERGE_RATIO * (prev_end - prev_start):
        return list(bounds[:-2]) + [(prev_start, last_end)]
    return list(bounds)


def get_binary_classification_chunking_strat(prompt: Prompt) -> ChunkingStrategy:
    return _get_single_shot_chunking_strat(
        max_context_tokens=BINARY_CLASSIFICATION_CHUNKING_STRAT_MAX_TOKENS,
        prompt=prompt,
    )


def get_basic_field_chunking_strat(prompt: Prompt) -> ChunkingStrategy:
    return _get_single_shot_chunking_strat(
        max_context_tokens=BASIC_FIELD_EXTRACTION_CHUNKING_STRAT_MAX_TOKENS,
        prompt=prompt,
    )


def _get_single_shot_chunking_strat(
    max_context_tokens: int, prompt: Prompt
) -> ChunkingStrategy:
    # For binary classification, we want to be more conservative with chunking to ensure the model has enough context to make an accurate classification.
    # The exact parameters can be tuned based on experimentation, but as a starting point, we'll use a smaller max_tokens and only allow for 1 chunk to be generated.
    return ChunkingStrategy(
        overlap=0,
        max_tokens_per_chunk=max_context_tokens - prompt.num_tokens - 10_000,
        max_chunks=1,
    )


DEFAULT_MAX_CHUNKS = 2


BINARY_CLASSIFICATION_CHUNKING_STRAT_MAX_TOKENS = 120_000
BASIC_FIELD_EXTRACTION_CHUNKING_STRAT_MAX_TOKENS = 120_000

shallow = ChunkingStrategy(
    overlap=0.15, max_tokens_per_chunk=5000, max_chunks=DEFAULT_MAX_CHUNKS
)

medium = ChunkingStrategy(
    overlap=0.15, max_tokens_per_chunk=10_000, max_chunks=DEFAULT_MAX_CHUNKS
)

wide = ChunkingStrategy(
    # 0 since 2026-08-22 (user decision, v3): the same ratio drives chunk AND
    # search sub-window overlap, and in v3 overlap only duplicated work —
    # search is window-local and 97% verbatim, mention collection + the fold run
    # per chunk, synthesis is per group, and reconcile merges groups across
    # chunks by key. Measured on run 20260822T061410: 297 duplicate occurrences
    # across overlapping sub-windows and 87 occurrences folded (and so
    # synthesized) in both chunks. The v1/v2 reasons for overlap — a weaker
    # search pass and relationship context at chunk edges — are gone.
    overlap=0,
    max_tokens_per_chunk=20_000,  # <- the 20k knob
    max_chunks=2,
    search_divisor=4,  # <- 20k / 4 = 5k search windows
    align_to_page_headers=True,  # every chunk and window opens at a page (N1)
    # legal pages removed before chunking (2026-08-23) — see the field's note
    drop_excluded_pages=True,
    page_exclusion_version=PAGE_EXCLUSION_VERSION,
)

PRODUCT_CHUNKING_STRAT = wide
EQUIPMENT_CHUNKING_STRAT = wide
CONFORMITY_ATTESTATION_CHUNKING_STRAT = wide
MATERIAL_CAP_CHUNKING_STRAT = wide
PROCESS_CAP_CHUNKING_STRAT = wide
INDUSTRY_CHUNKING_STRAT = wide
