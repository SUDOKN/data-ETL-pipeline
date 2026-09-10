"""The mechanical mention collection + per-chunk AGGREGATION FOLD (PIPELINE
V3_PLAN.md D4–D7, as amended 2026-08-22; the LLM location wire that used to
live beside this machinery was retired 2026-09-03 when the synthesis stage
absorbed the location task — see ``llm_phrase_synthesis_node_service``).

CHUNK-WIDE, OCCURRENCE-FILTERED FORMS (D2/D5; user decision 2026-08-22;
subject-wide 2026-09-02). The scan runs per search sub-window (the 5k windows
of ``ChunkingStrategy.search_divisor``). Search harvests forms per sub-window,
but a form it listed for one sub-window and not for a sibling was measured to
leave ~1,100 occurrences uncollected per run (run 20260822T223715); the Ctrl+F
is free, so the pool is SUBJECT-wide — every sub-window's first-search phrases
∪ recursive rounds (off since 2026-08-22) ∪ the exact casings of brute-search
labels found in each (``brute_by_sub_bounds``, written at prefill) — and each
sub-window is handed the pooled forms that OCCUR in it (whole-word,
case-insensitive, the scan's own domain). Exact-string dedup only; sorted, so
the result is the same in every process. The SYNTHESIS node writes each
window's list onto the bundle (``llm_phrase_mention_sent_forms`` — the field
keeps its historical name) when it embeds its request ids, and every later
step — request creation, the fold, downstream reads — reads it back from
there: the fold has no search map. Search stays answerable for per-window
recall (the consistency metric in the plan); pooling only stops one window's
omission costing the subject.

COLLECTION (user decision 2026-08-22). ``collect_window`` does what the LLM
collector used to be asked to do, exactly: every whole-word occurrence of every
sent form (casing-expanded), longest-span containment, the snippet clipped to
the sentence or line holding it, one item per DISTINCT snippet. Measured on
run 20260822T195947 the LLM collector's accepted output was a subset of this
minus the 19% it satisficed away; the scan can miss nothing the fold would have
kept.

FOLD SCOPE (user decision 2026-08-21). The fold runs per CHUNK over the chunk's
sub-windows, giving one ``FoldResult`` per chunk. Bundles are keyed by the
content-derived ``group_id``, so the same group in two chunks shares an id and
reconcile merges across chunks as it always has. Since the location-stage
merge the fold is PURE code over (text, stored forms, knobs) — it needs no
completed request map at all.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Iterable, Optional

from llm_providers.db_models.gpt_batch_request import GPTBatchRequest
from llm_providers.field_types import BatchRequestIDType

from core.models.deferred_extraction.deferred_phrase_extraction_requests import (
    LLMPhraseExtractionRequestBundle,
)
from core.models.field_types import ExtractionFieldType
from core.services.pipeline_nodes.multi_stage.llm_phrase_recursive_search_node_service import (
    parse_recursive_search_round_result,
)
from core.services.pipeline_nodes.multi_stage.llm_phrase_search_node_service import (
    parse_search_window_union,
    window_pass_request_ids,
)
from core.utils.aggregation_fold import (
    FoldResult,
    WindowCollection,
    WindowInput,
    collect_window,
    fold_document,
)
from core.utils.floor_scan import floor_scan, preceding_page_of

logger = logging.getLogger(__name__)


# --- geometry helpers ------------------------------------------------------------------


def window_bounds(sub_bounds: str) -> tuple[int, int]:
    start, end = sub_bounds.split(":")
    return int(start), int(end)


def window_text_of(subject_text: str, sub_bounds: str) -> str:
    start, end = window_bounds(sub_bounds)
    return subject_text[start:end]


def window_preceding_page(subject_text: str, sub_bounds: str) -> Optional[str]:
    start, _end = window_bounds(sub_bounds)
    return preceding_page_of(subject_text, start)


# --- forms ------------------------------------------------------------------------------


def brute_casings_in_window(window_text: str, brute_labels: Iterable[str]) -> list[str]:
    """The distinct exact casings of *brute_labels* occurring in *window_text*
    (case-insensitive whole-word, page headers excluded — the floor scan's own
    rules, short labels staying exact), sorted. Pure; computed at prefill."""
    labels = sorted({label for label in brute_labels if label.strip()})
    if not labels:
        return []
    scan = floor_scan(window_text, labels)
    casings: set[str] = set()
    for hits in scan.tier2.values():
        casings.update(window_text[o.start:o.end] for o in hits)
    return sorted(casings)


async def get_window_forms(
    subject_unique_id: str,
    field_type: ExtractionFieldType,
    chunk_bounds: str,
    sub_bounds: str,
    extraction_bundle: LLMPhraseExtractionRequestBundle,
    llm_phrase_search_gpt_request_map: dict[BatchRequestIDType, GPTBatchRequest],
    llm_phrase_recursive_search_gpt_request_map: dict[BatchRequestIDType, GPTBatchRequest],
    timestamp: datetime,
) -> list[str]:
    """ONE sub-window's sent forms, from the completed search maps (see the
    module docstring). Called once per window, when the synthesis node embeds
    its ids; the result is stored on the bundle and read back from there
    afterwards."""
    if sub_bounds not in extraction_bundle.search_sub_bounds:
        raise ValueError(
            f"aggregation_fold: sub-window {sub_bounds} is not one of chunk "
            f"{chunk_bounds}'s search_sub_bounds in {subject_unique_id}:{field_type.name}"
        )
    index = extraction_bundle.search_sub_bounds.index(sub_bounds)
    if index >= len(extraction_bundle.llm_phrase_search_req_ids):
        raise ValueError(
            f"aggregation_fold: no first-search request embedded for sub-window "
            f"{sub_bounds} of chunk {chunk_bounds} in {subject_unique_id}:{field_type.name}"
        )
    # Retry-and-union (2026-09-03): the window's forms are the union of its
    # parseable search passes — see parse_search_window_union for the
    # missing-vs-unparseable distinction.
    forms: set[str] = set(
        await parse_search_window_union(
            subject_unique_id=subject_unique_id,
            field_type=field_type,
            window_request_ids=window_pass_request_ids(extraction_bundle, index),
            all_phrase_search_req_responses_map=llm_phrase_search_gpt_request_map,
            deferred_at=timestamp,
        )
    )
    for round_req_id in extraction_bundle.llm_phrase_recursive_search_req_ids.get(sub_bounds, []):
        forms |= await parse_recursive_search_round_result(
            subject_unique_id=subject_unique_id,
            field_type=field_type,
            chunk_bounds=chunk_bounds,
            round_req_id=round_req_id,
            completed_request_map=llm_phrase_recursive_search_gpt_request_map,
            timestamp=timestamp,
        )
    # Concept bundles carry the brute casings per sub-window; keyword bundles
    # have no brute phase and no such field.
    brute_by_sub_bounds: dict[str, list[str]] = getattr(
        extraction_bundle, "brute_by_sub_bounds", {}
    ) or {}
    forms.update(brute_by_sub_bounds.get(sub_bounds, []))
    # Exact strings, blanks dropped, sorted — NOT casefold-deduped: v3 sends case
    # variants as distinct forms (D5); the scan collapses their shared spans.
    return sorted(form for form in forms if form.strip())


async def get_chunk_forms(
    subject_unique_id: str,
    field_type: ExtractionFieldType,
    chunk_bounds: str,
    extraction_bundle: LLMPhraseExtractionRequestBundle,
    llm_phrase_search_gpt_request_map: dict[BatchRequestIDType, GPTBatchRequest],
    llm_phrase_recursive_search_gpt_request_map: dict[BatchRequestIDType, GPTBatchRequest],
    timestamp: datetime,
) -> list[str]:
    """The CHUNK's pooled forms: the union of every sub-window's
    ``get_window_forms`` (see the module docstring), exact strings, sorted."""
    forms: set[str] = set()
    for sub_bounds in extraction_bundle.search_sub_bounds:
        forms.update(
            await get_window_forms(
                subject_unique_id=subject_unique_id,
                field_type=field_type,
                chunk_bounds=chunk_bounds,
                sub_bounds=sub_bounds,
                extraction_bundle=extraction_bundle,
                llm_phrase_search_gpt_request_map=llm_phrase_search_gpt_request_map,
                llm_phrase_recursive_search_gpt_request_map=llm_phrase_recursive_search_gpt_request_map,
                timestamp=timestamp,
            )
        )
    return sorted(forms)


async def get_subject_forms(
    subject_unique_id: str,
    field_type: ExtractionFieldType,
    chunked_request_map,
    llm_phrase_search_gpt_request_map: dict[BatchRequestIDType, GPTBatchRequest],
    llm_phrase_recursive_search_gpt_request_map: dict[BatchRequestIDType, GPTBatchRequest],
    timestamp: datetime,
) -> list[str]:
    """The SUBJECT's pooled forms: the union of every chunk's
    ``get_chunk_forms``, sorted. 2026-09-02 (Phase B of the search-recall
    roadmap): the mention scan pools subject-wide rather than chunk-wide, so a
    form the search stage returned only in one chunk still has its mentions
    collected in every OTHER chunk whose text carries it — the measured
    cross-chunk gap was 61 of the census's 1,419 missed entities.
    ``forms_occurring_in_window`` still filters each window to the forms it
    can actually anchor, so a window is never sent a form its text lacks."""
    forms: set[str] = set()
    for chunk_bounds, extraction_bundle in chunked_request_map.items():
        forms.update(
            await get_chunk_forms(
                subject_unique_id=subject_unique_id,
                field_type=field_type,
                chunk_bounds=chunk_bounds,
                extraction_bundle=extraction_bundle,
                llm_phrase_search_gpt_request_map=llm_phrase_search_gpt_request_map,
                llm_phrase_recursive_search_gpt_request_map=llm_phrase_recursive_search_gpt_request_map,
                timestamp=timestamp,
            )
        )
    return sorted(forms)


def forms_occurring_in_window(
    subject_text: str, sub_bounds: str, forms: Iterable[str]
) -> list[str]:
    """Of *forms*, those with at least one occurrence in the sub-window — the
    collector's own tier-2 rule (whole word, case-insensitive except short
    forms, page headers and excluded pages masked, the inherited page
    honoured) — so a window is sent exactly the forms the collection can
    anchor. Pure; sorted; blanks dropped."""
    candidates = sorted({form for form in forms if form.strip()})
    if not candidates:
        return []
    scan = floor_scan(
        window_text_of(subject_text, sub_bounds),
        candidates,
        preceding_page=window_preceding_page(subject_text, sub_bounds),
    )
    return [form for form in candidates if scan.tier2.get(form)]


def stored_window_forms(
    subject_unique_id: str,
    field_type: ExtractionFieldType,
    chunk_bounds: str,
    sub_bounds: str,
    extraction_bundle: LLMPhraseExtractionRequestBundle,
) -> list[str]:
    """The sent forms the synthesis node stored for *sub_bounds* when it
    embedded its ids. Absent means the deferred field predates the stored-forms
    contract (or embed never ran): re-defer, do not guess."""
    forms = extraction_bundle.llm_phrase_mention_sent_forms.get(sub_bounds)
    if forms is None:
        raise ValueError(
            f"aggregation_fold: no stored sent forms for sub-window {sub_bounds} of "
            f"chunk {chunk_bounds} in {subject_unique_id}:{field_type.name}; the "
            f"deferred field predates llm_phrase_mention_sent_forms and must be "
            f"re-deferred."
        )
    return forms


# --- collection + the chunk fold --------------------------------------------------------


def collect_sub_window(
    subject_text: str, sub_bounds: str, forms: list[str], *, snippet_radius: int = 0
) -> WindowCollection:
    """The mechanical collection of ONE sub-window: pure in (text, forms,
    radius), so the node's id-minting pass, request creation and the fold all
    compute the same items."""
    return collect_window(
        window_text_of(subject_text, sub_bounds),
        forms,
        preceding_page=window_preceding_page(subject_text, sub_bounds),
        snippet_radius=snippet_radius,
    )


def get_chunk_fold(
    subject_unique_id: str,
    field_type: ExtractionFieldType,
    chunk_bounds: str,
    extraction_bundle: LLMPhraseExtractionRequestBundle,
    *,
    subject_text: str,
    verb_fold: bool,
    snippet_radius: int = 0,
    collapse_compounds: bool = False,
) -> FoldResult:
    """The chunk's aggregation fold: every sub-window re-collected from the
    text and its stored forms, bundled over the union of the chunk's forms.
    PURE code since the location-stage merge — no completed request map, no
    LLM answers. Windows are folded in ``search_sub_bounds`` order (document
    order); each inherits the page the text before it was on.
    ``snippet_radius`` is the clip dial the stored mention ids were minted
    under; ``collapse_compounds`` is D21's dial; the chunk is the 20k macro
    chunk, which is the scope siblinghood is judged in."""
    if not extraction_bundle.search_sub_bounds:
        raise ValueError(
            f"aggregation_fold: chunk {chunk_bounds} has no search_sub_bounds in "
            f"{subject_unique_id}:{field_type.name}"
        )
    windows = [
        WindowInput(
            text=window_text_of(subject_text, sub_bounds),
            sent_forms=stored_window_forms(
                subject_unique_id, field_type, chunk_bounds, sub_bounds, extraction_bundle
            ),
            preceding_page=window_preceding_page(subject_text, sub_bounds),
            window_id=sub_bounds,
        )
        for sub_bounds in extraction_bundle.search_sub_bounds
    ]
    return fold_document(
        windows,
        verb_fold=verb_fold,
        snippet_radius=snippet_radius,
        collapse_compounds=collapse_compounds,
    )


# --- fold knobs off run metadata --------------------------------------------------------


def fold_verb_fold_of(metadata: object) -> bool:
    """The fold's verb-fold dial off a metadata object (any shape — the partial
    dump walks node CLASSES and generic metadata). False when absent."""
    fold_metadata: Optional[object] = getattr(metadata, "aggregation_fold", None)
    return bool(getattr(fold_metadata, "verb_fold", False))


def fold_collapse_compounds_of(metadata: object) -> bool:
    """The fold's compound-collapse dial (D21) off a metadata object (any
    shape). False when absent — the fold every run before the dial used, so a
    document persisted earlier replays with every compound still synthesized."""
    fold_metadata: Optional[object] = getattr(metadata, "aggregation_fold", None)
    return bool(getattr(fold_metadata, "collapse_compounds", False))


def fold_snippet_radius_of(metadata: object) -> int:
    """The collector's snippet radius off a metadata object (any shape), read
    from the fold's metadata where it is request identity (moved there from the
    retired mention-collection metadata, 2026-09-03). 0 when absent — the clip
    every run before the knob used."""
    fold_metadata: Optional[object] = getattr(metadata, "aggregation_fold", None)
    return int(getattr(fold_metadata, "snippet_radius", 0) or 0)
