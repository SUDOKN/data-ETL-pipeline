"""Pipeline v3's mention-collection stage (PIPELINE_V3_PLAN.md D4–D7) — the LLM
half of what replaced relationship — and the per-chunk AGGREGATION FOLD over
its answers (``core.utils.aggregation_fold``), which is pure code and runs here
at parse time, never as a request.

WINDOW-LOCAL FORMS (D2/D5). The collector runs per search sub-window (the 5k
windows of ``ChunkingStrategy.search_divisor``). A sub-window's sent forms are
that sub-window's own first-search phrases ∪ its recursive rounds ∪ the exact
casings of brute-search labels found in it (``brute_by_sub_bounds``, written at
prefill where the text is — user decision 2026-08-21: brute keeps its recall,
the exact-string contract holds, the fold groups casings by key). Exact-string
dedup only, so ``Aluminum`` and ``aluminum`` ride as two forms; sorted, so the
prompt text is the same in every process. Never unioned across windows.

GROUPS. Forms split into ceil(n / max_forms_per_request) groups, each asked
against the same window text; a window with no forms gets one dummy request
answering ``{"forms": []}`` (the single-dummy-per-unit convention every stage
shares). A group's forms are digested into its custom_id (``|ud=``): change
what search feeds a window and the stale answer is simply never found.

HOLD (D6). A response is held EXACTLY to the forms its request sent — warn-only:
the tier-1 floor scan is what turns a hole into a visible discrepancy, and a
raising hold would replay a temperature-0 mis-echo to death.

FOLD SCOPE (user decision 2026-08-21). The fold runs per CHUNK over the chunk's
sub-windows — the 20k macro chunk relationship used to see, ``search_divisor``
windows of it — giving one ``FoldResult`` per chunk. Bundles are keyed by the
content-derived ``group_id``, so the same group in two chunks shares an id (as
``record_id`` does for a phrase today) and reconcile merges across chunks as it
always has. The fold reads a window's sent forms back off its requests' own
``<<<PHRASES`` fences — the same reader the hold uses — so it needs nothing but
the completed map and the subject text.
"""

from __future__ import annotations

import asyncio
import logging
import traceback
from datetime import datetime
from typing import Iterable, Optional

from llm_providers.db_models.gpt_batch_request import GPTBatchRequest
from llm_providers.field_types import BatchRequestIDType
from llm_providers.models.file_objects.prompt import Prompt
from llm_providers.models.llm_model import LLM_Model, NO_MODEL
from llm_providers.models.open_ai.gpt_batch_response_blob import (
    ChatCompletionChoiceMessage,
)
from llm_providers.models.open_ai.gpt_model_params import GPTModelParams
from llm_providers.services.gpt_batch_request.gpt_batch_request_service import (
    create_base_gpt_batch_request,
    get_dummy_gpt_batch_response,
)
from llm_providers.services.gpt_batch_request.gpt_batch_request_writes import (
    record_response_parse_error_capped,
)

from core.models.deferred_extraction.deferred_phrase_extraction_requests import (
    LLMPhraseExtractionRequestBundle,
    LLMPhraseExtractionRequestMap,
)
from core.models.extraction_results.llm_phrase_extraction_results import (
    BatchedMentionCollectionNodeMetadata,
)
from core.models.extraction_results.llm_phrase_extraction_results_v2 import (
    LLMPhraseExtractionMetadataV2,
)
from core.models.extraction_schemas.mention_collection import (
    DUMMY_MENTION_COLLECTION_RESPONSE_CONTENT,
    MENTION_COLLECTION_RESPONSE_SCHEMA,
    MentionsByForm,
    parse_mention_collection_response,
)
from core.models.field_types import ExtractionFieldType
from core.services.phrase_blocks_contract import (
    hold_response_to_sent_forms,
    render_phrases_block,
    sent_phrases_from_user_message,
)
from core.services.pipeline_nodes.multi_stage.llm_phrase_recursive_search_node_service import (
    parse_recursive_search_round_result,
)
from core.services.pipeline_nodes.multi_stage.llm_phrase_search_node_service import (
    parse_search_sub_request_result,
)
from core.utils.aggregation_fold import FoldResult, WindowInput, fold_document
from core.utils.floor_scan import floor_scan, preceding_page_of

logger = logging.getLogger(__name__)


# --- metadata + geometry helpers ------------------------------------------------------


def require_mention_metadata(
    metadata: LLMPhraseExtractionMetadataV2,
) -> BatchedMentionCollectionNodeMetadata:
    """The mention node's metadata, which a v3 chain always carries. None means
    the pipeline was built without it — a factory wiring error, not a run state."""
    mention = metadata.llm_phrase_mention_collection
    if mention is None:
        raise ValueError(
            "llm_phrase_mention_collection metadata is None: the pipeline was built "
            "without the v3 mention stage's metadata (ExtractionPipelineFactory must "
            "pass it alongside the mention node)."
        )
    return mention


def window_bounds(sub_bounds: str) -> tuple[int, int]:
    start, end = sub_bounds.split(":")
    return int(start), int(end)


def window_text_of(subject_text: str, sub_bounds: str) -> str:
    start, end = window_bounds(sub_bounds)
    return subject_text[start:end]


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
    """ONE sub-window's sent forms, exactly as the collector is asked about them
    (see the module docstring). Single source of truth: the node counts groups
    from this list before any request exists, and request creation renders it."""
    if sub_bounds not in extraction_bundle.search_sub_bounds:
        raise ValueError(
            f"mention_collection: sub-window {sub_bounds} is not one of chunk "
            f"{chunk_bounds}'s search_sub_bounds in {subject_unique_id}:{field_type.name}"
        )
    index = extraction_bundle.search_sub_bounds.index(sub_bounds)
    if index >= len(extraction_bundle.llm_phrase_search_req_ids):
        raise ValueError(
            f"mention_collection: no first-search request embedded for sub-window "
            f"{sub_bounds} of chunk {chunk_bounds} in {subject_unique_id}:{field_type.name}"
        )
    forms: set[str] = set(
        await parse_search_sub_request_result(
            subject_unique_id=subject_unique_id,
            field_type=field_type,
            llm_phrase_search_request_id=extraction_bundle.llm_phrase_search_req_ids[index],
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
    # variants as distinct forms (D5), and sorted order keeps the rendered prompt
    # identical across processes (a set iterates differently under hash
    # randomisation, and the order also decides which forms share a request).
    return sorted(form for form in forms if form.strip())


def split_into_form_groups(forms: list[str], group_size: int) -> list[list[str]]:
    """Ordered groups of at most *group_size* forms. Always at least one group —
    the empty group is what the single-dummy-request path keys off."""
    if not forms:
        return [[]]
    return [forms[i : i + group_size] for i in range(0, len(forms), group_size)]


# --- requests ------------------------------------------------------------------------------


def render_mention_collection_context(window_text: str, forms: list[str]) -> str:
    """The user message: the window's text, then the fenced forms at the very
    bottom (what the static promises the model). The fence, not a list repr — the
    scraped text sits before it and could otherwise forge a bare marker; see
    ``phrase_blocks_contract``. The hold reads the forms back off this fence."""
    return (
        f"text scraped from a manufacturer's website:\n{window_text}\n\n"
        f"{render_phrases_block(forms)}"
    )


def create_mention_collection_gpt_request(
    deferred_at: datetime,
    subject_unique_id: str,
    request_id: BatchRequestIDType,
    window_text: str,
    forms: list[str],
    phrase_mention_collection_prompt: Prompt,
    gpt_model: LLM_Model,
    model_params: GPTModelParams,
    eager: bool,
) -> GPTBatchRequest:
    return create_base_gpt_batch_request(
        deferred_at=deferred_at,
        subject_unique_id=subject_unique_id,
        custom_id=request_id,
        context=render_mention_collection_context(window_text, forms),
        prompt_text=phrase_mention_collection_prompt.text,
        gpt_model=gpt_model,
        model_params=model_params.with_response_format(MENTION_COLLECTION_RESPONSE_SCHEMA),
        batch_id="Eager" if eager else None,
    )


def create_dummy_completed_mention_collection_request(
    deferred_at: datetime,
    subject_unique_id: str,
    request_id: BatchRequestIDType,
    model_params: GPTModelParams,
    eager: bool,
) -> GPTBatchRequest:
    """A window with no forms: one pre-answered request, so the stage completes
    and the fold sees the window (with no forms, no bundles)."""
    request = create_base_gpt_batch_request(
        deferred_at=deferred_at,
        subject_unique_id=subject_unique_id,
        custom_id=request_id,
        context="No mention collection needed - no forms in this window.",
        prompt_text="No mention collection needed - search found no forms in this window.",
        gpt_model=NO_MODEL,
        model_params=model_params,
        batch_id="Eager" if eager else "dummy_mention_collection_batch_id",
    )
    request.response = get_dummy_gpt_batch_response(
        deferred_at=deferred_at,
        request_custom_id=request_id,
        dummy_chat_completion_id="dummy_completion_id",
        chat_completion_choice_message=ChatCompletionChoiceMessage(
            role="assistant", content=DUMMY_MENTION_COLLECTION_RESPONSE_CONTENT
        ),
    )
    return request


async def create_missing_mention_collection_requests(
    subject_unique_id: str,
    field_type: ExtractionFieldType,
    chunked_request_map: LLMPhraseExtractionRequestMap,
    missing_request_ids: set[BatchRequestIDType],
    subject_text: str,
    phrase_mention_collection_prompt: Prompt,
    llm_phrase_search_gpt_request_map: dict[BatchRequestIDType, GPTBatchRequest],
    llm_phrase_recursive_search_gpt_request_map: dict[BatchRequestIDType, GPTBatchRequest],
    timestamp: datetime,
    llm_model: LLM_Model,
    model_params: GPTModelParams,
    max_forms_per_request: int,
    eager: bool,
    BATCH_SIZE: int = 100,
) -> list[GPTBatchRequest]:
    """Fresh or only-missing requests, for every (chunk, sub-window, group) whose
    embedded id is in *missing_request_ids*."""
    logger.info(
        f"create_missing_mention_collection_requests: generating GPTBatchRequests "
        f"for {subject_unique_id}:{field_type.name}"
    )
    if not llm_phrase_search_gpt_request_map:
        raise ValueError(
            f"create_missing_mention_collection_requests: no completed search "
            f"requests for {subject_unique_id}:{field_type.name}"
        )

    work: list[tuple[str, str, LLMPhraseExtractionRequestBundle]] = [
        (chunk_bounds, sub_bounds, bundle)
        for chunk_bounds, bundle in chunked_request_map.items()
        for sub_bounds, group_req_ids in bundle.llm_phrase_mention_req_ids.items()
        if set(group_req_ids) & missing_request_ids
    ]

    batch_requests: list[GPTBatchRequest] = []
    for i in range(0, len(work), BATCH_SIZE):
        for chunk_bounds, sub_bounds, bundle in work[i : i + BATCH_SIZE]:
            forms = await get_window_forms(
                subject_unique_id=subject_unique_id,
                field_type=field_type,
                chunk_bounds=chunk_bounds,
                sub_bounds=sub_bounds,
                extraction_bundle=bundle,
                llm_phrase_search_gpt_request_map=llm_phrase_search_gpt_request_map,
                llm_phrase_recursive_search_gpt_request_map=llm_phrase_recursive_search_gpt_request_map,
                timestamp=timestamp,
            )
            groups = split_into_form_groups(forms, max_forms_per_request)
            group_req_ids = bundle.llm_phrase_mention_req_ids[sub_bounds]
            if len(group_req_ids) != len(groups):
                raise ValueError(
                    f"create_missing_mention_collection_requests: embedded group count "
                    f"({len(group_req_ids)}) does not match the computed count "
                    f"({len(groups)}) for sub-window {sub_bounds} of chunk {chunk_bounds} "
                    f"in {subject_unique_id}:{field_type.name}. Both come from "
                    f"get_window_forms over the same completed maps, so this should not "
                    f"happen."
                )
            window_text = window_text_of(subject_text, sub_bounds)
            for group_index, group_req_id in enumerate(group_req_ids):
                if group_req_id not in missing_request_ids:
                    continue
                group = groups[group_index]
                if not group:
                    if len(groups) != 1:
                        raise ValueError(
                            f"create_missing_mention_collection_requests: unexpected empty "
                            f"group at index {group_index} of {len(groups)} for sub-window "
                            f"{sub_bounds} in {subject_unique_id}:{field_type.name}."
                        )
                    logger.info(
                        f"No forms in sub-window {sub_bounds} for "
                        f"{subject_unique_id}:{field_type.name}; creating dummy "
                        f"mention-collection request"
                    )
                    batch_requests.append(
                        create_dummy_completed_mention_collection_request(
                            deferred_at=timestamp,
                            subject_unique_id=subject_unique_id,
                            request_id=group_req_id,
                            model_params=model_params,
                            eager=eager,
                        )
                    )
                    continue
                logger.info(
                    f"Sending {len(group)} form(s) to mention collection for "
                    f"{subject_unique_id}:{field_type.name} sub-window {sub_bounds} "
                    f"group {group_index}"
                )
                batch_requests.append(
                    create_mention_collection_gpt_request(
                        deferred_at=timestamp,
                        subject_unique_id=subject_unique_id,
                        request_id=group_req_id,
                        window_text=window_text,
                        forms=group,
                        phrase_mention_collection_prompt=phrase_mention_collection_prompt,
                        gpt_model=llm_model,
                        model_params=model_params,
                        eager=eager,
                    )
                )
        await asyncio.sleep(0)  # yield to the event loop between batches
    return batch_requests


# --- parse + hold ------------------------------------------------------------------------------


def _completed_request(
    subject_unique_id: str,
    field_type: ExtractionFieldType,
    group_req_id: BatchRequestIDType,
    completed_request_map: dict[BatchRequestIDType, GPTBatchRequest],
) -> GPTBatchRequest:
    req_obj = completed_request_map.get(group_req_id)
    if not req_obj:
        raise ValueError(
            f"mention_collection: missing GPTBatchRequest for request id {group_req_id} "
            f"in {subject_unique_id}:{field_type.name}"
        )
    if not req_obj.response:
        raise ValueError(
            f"mention_collection: GPTBatchRequest {group_req_id} has no response in "
            f"{subject_unique_id}:{field_type.name}"
        )
    return req_obj


async def parse_mention_group_result(
    subject_unique_id: str,
    field_type: ExtractionFieldType,
    group_req_id: BatchRequestIDType,
    completed_request_map: dict[BatchRequestIDType, GPTBatchRequest],
    timestamp: datetime,
) -> tuple[list[str], MentionsByForm]:
    """ONE group request's (sent forms, held answer). The sent forms are read
    back off the request's own fence — the dummy request has none and sends
    nothing. A malformed response is recorded against THIS request and raised."""
    req_obj = _completed_request(subject_unique_id, field_type, group_req_id, completed_request_map)
    response = req_obj.response
    if response is None:  # _completed_request raised already; this narrows the type
        raise ValueError(f"mention_collection: GPTBatchRequest {group_req_id} has no response")
    user_message = req_obj.request.body.user_message()
    sent_forms = sent_phrases_from_user_message(user_message) or []
    try:
        by_form = parse_mention_collection_response(response.result)
        held = hold_response_to_sent_forms(
            user_message=user_message,
            response_by_form=by_form,
            where=f"{subject_unique_id}:{field_type.name} mention collection {group_req_id}",
        )
    except Exception as e:
        await record_response_parse_error_capped(
            gpt_batch_request=req_obj,
            error_message=str(e),
            timestamp=timestamp,
            traceback_str=traceback.format_exc(),
        )
        logger.error(
            f"mention_collection: error parsing mention-collection response "
            f"{group_req_id} for {subject_unique_id}:{field_type.name}: {e}"
        )
        raise
    return sent_forms, held


async def get_window_mentions(
    subject_unique_id: str,
    field_type: ExtractionFieldType,
    chunk_bounds: str,
    sub_bounds: str,
    extraction_bundle: LLMPhraseExtractionRequestBundle,
    completed_request_map: dict[BatchRequestIDType, GPTBatchRequest],
    timestamp: datetime,
) -> tuple[list[str], MentionsByForm]:
    """ONE sub-window's (sent forms, held mentions), merged across its groups.
    A form is in exactly one group by construction; merging by extend keeps a
    stray duplicate visible rather than silently picking one."""
    group_req_ids = extraction_bundle.llm_phrase_mention_req_ids.get(sub_bounds)
    if not group_req_ids:
        raise ValueError(
            f"mention_collection: no mention requests embedded for sub-window "
            f"{sub_bounds} of chunk {chunk_bounds} in {subject_unique_id}:{field_type.name}"
        )
    sent_forms: list[str] = []
    mentions: MentionsByForm = {}
    for group_req_id in group_req_ids:
        group_sent, held = await parse_mention_group_result(
            subject_unique_id=subject_unique_id,
            field_type=field_type,
            group_req_id=group_req_id,
            completed_request_map=completed_request_map,
            timestamp=timestamp,
        )
        sent_forms.extend(form for form in group_sent if form not in sent_forms)
        for form, form_mentions in held.items():
            mentions.setdefault(form, []).extend(form_mentions)
    return sent_forms, mentions


async def get_chunk_fold(
    subject_unique_id: str,
    field_type: ExtractionFieldType,
    chunk_bounds: str,
    extraction_bundle: LLMPhraseExtractionRequestBundle,
    completed_request_map: dict[BatchRequestIDType, GPTBatchRequest],
    timestamp: datetime,
    *,
    subject_text: str,
    verb_fold: bool,
) -> FoldResult:
    """The chunk's aggregation fold: every sub-window located, re-attributed,
    deduped and held; bundles over the union of the chunk's window-local forms.
    Windows are folded in ``search_sub_bounds`` order (document order); each
    inherits the page the text before it was on."""
    if not extraction_bundle.search_sub_bounds:
        raise ValueError(
            f"mention_collection: chunk {chunk_bounds} has no search_sub_bounds in "
            f"{subject_unique_id}:{field_type.name}"
        )
    windows: list[WindowInput] = []
    for sub_bounds in extraction_bundle.search_sub_bounds:
        sent_forms, mentions = await get_window_mentions(
            subject_unique_id=subject_unique_id,
            field_type=field_type,
            chunk_bounds=chunk_bounds,
            sub_bounds=sub_bounds,
            extraction_bundle=extraction_bundle,
            completed_request_map=completed_request_map,
            timestamp=timestamp,
        )
        start, _end = window_bounds(sub_bounds)
        windows.append(
            WindowInput(
                text=window_text_of(subject_text, sub_bounds),
                sent_forms=sent_forms,
                mentions_by_form=mentions,
                preceding_page=preceding_page_of(subject_text, start),
                window_id=sub_bounds,
            )
        )
    return fold_document(windows, verb_fold=verb_fold)


def fold_verb_fold_of(metadata: object) -> bool:
    """The fold's verb-fold dial off a metadata object (any shape — the partial
    dump walks node CLASSES and generic metadata). False when absent."""
    fold_metadata: Optional[object] = getattr(metadata, "aggregation_fold", None)
    return bool(getattr(fold_metadata, "verb_fold", False))
