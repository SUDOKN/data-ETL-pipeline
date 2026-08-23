"""Pipeline v3's mention-collection stage (PIPELINE_V3_PLAN.md D4–D7, as
amended 2026-08-22): CODE collects the mentions, the LLM describes where each
sits, and the per-chunk AGGREGATION FOLD (``core.utils.aggregation_fold``)
joins the two — pure code, run here at parse time, never as a request.

CHUNK-WIDE, OCCURRENCE-FILTERED FORMS (D2/D5; user decision 2026-08-22). The
stage runs per search sub-window (the 5k windows of
``ChunkingStrategy.search_divisor``). Search harvests forms per sub-window, but
a form it listed for one sub-window and not for a sibling was measured to leave
~1,100 occurrences uncollected per run (run 20260822T223715); the Ctrl+F is free,
so the CHUNK's forms are pooled — every sub-window's first-search phrases ∪
recursive rounds (off since 2026-08-22) ∪ the exact casings of brute-search
labels found in each (``brute_by_sub_bounds``, written at prefill) — and each
sub-window is handed the pooled forms that OCCUR in it (whole-word,
case-insensitive, the scan's own domain). Exact-string dedup only; sorted, so
the result is the same in every process. The node writes each window's list onto
the bundle (``llm_phrase_mention_sent_forms``) when it embeds the window's
request ids, and every later step — request creation, the fold — reads it back
from there: the fold has no search map, and the request carries snippets, not
forms. Search stays answerable for per-window recall (the consistency metric in
the plan); pooling only stops one window's omission costing the chunk.

COLLECTION (user decision 2026-08-22). ``collect_window`` does what the LLM
collector used to be asked to do, exactly: every whole-word occurrence of every
sent form (casing-expanded), longest-span containment, the snippet clipped to
the sentence or line holding it, one wire item per DISTINCT snippet. Measured
on run 20260822T195947 the LLM collector's accepted output was a subset of this
minus the 19% it satisficed away; the scan can miss nothing the fold would have
kept.

GROUPS. A window's wire items split into ceil(n / max_mentions_per_request)
location requests, each against the same window text (excluded pages omitted —
``floor_scan.wire_window_text``); a window with no items gets one dummy request
answering ``{"mentions": []}`` (the single-dummy-per-unit convention every
stage shares). A group's items are digested into its custom_id (``|ud=``):
change what the window's text or forms yield and the stale answer is simply
never found.

HOLD (D6). A response is held EXACTLY on mention ids and WARN-ONLY both ways:
the fold defaults the location of anything the model left out.

UNDER-ANSWER POLICY (user decision 2026-08-22). Once a sub-window's group
requests are complete the node ASSESSES it: the sent ids no answer described are
stored on the bundle (``llm_phrase_mention_retry_mention_ids``; an empty list
means assessed and complete) and, when any are missing, ONE retry pass re-asks
for just those items against the same window text
(``llm_phrase_mention_retry_req_ids``, grouped like the first pass, custom id
``…>sub>{s}>retry>1>group>{j}>…``). The fold reads group answers first, then the
retry's; what is still missing after that is defaulted — nothing is retried
twice. Measured motive: 1/106 first-pass answers on run 20260822T223715 described
0 of 47 items (the model echoed the request nonce as an id).

FOLD SCOPE (user decision 2026-08-21). The fold runs per CHUNK over the chunk's
sub-windows, giving one ``FoldResult`` per chunk. Bundles are keyed by the
content-derived ``group_id``, so the same group in two chunks shares an id and
reconcile merges across chunks as it always has.
"""

from __future__ import annotations

import asyncio
import logging
import traceback
from dataclasses import dataclass
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
    LocationsByMentionId,
    MentionWireItem,
    parse_mention_location_response,
)
from core.models.field_types import ExtractionFieldType
from core.services.phrase_blocks_contract import (
    hold_response_to_sent_mention_ids,
    render_mention_blocks,
    sent_mention_ids_from_user_message,
)
from core.services.pipeline_nodes.multi_stage.llm_phrase_recursive_search_node_service import (
    parse_recursive_search_round_result,
)
from core.services.pipeline_nodes.multi_stage.llm_phrase_search_node_service import (
    parse_search_sub_request_result,
)
from core.utils.aggregation_fold import (
    FoldResult,
    WindowCollection,
    WindowInput,
    collect_window,
    fold_document,
)
from core.utils.floor_scan import floor_scan, preceding_page_of, wire_window_text

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
    module docstring). Called once per window, when the node embeds its ids;
    the result is stored on the bundle and read back from there afterwards."""
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
    """The sent forms the node stored for *sub_bounds* when it embedded the
    window's ids. Absent means the deferred field predates the stored-forms
    contract (or embed never ran): re-defer, do not guess."""
    forms = extraction_bundle.llm_phrase_mention_sent_forms.get(sub_bounds)
    if forms is None:
        raise ValueError(
            f"mention_collection: no stored sent forms for sub-window {sub_bounds} of "
            f"chunk {chunk_bounds} in {subject_unique_id}:{field_type.name}; the "
            f"deferred field predates llm_phrase_mention_sent_forms and must be "
            f"re-deferred."
        )
    return forms


# --- collection + groups ---------------------------------------------------------------------


def collect_sub_window(subject_text: str, sub_bounds: str, forms: list[str]) -> WindowCollection:
    """The mechanical collection of ONE sub-window: pure in (text, forms), so
    the node's id-minting pass, request creation and the fold all compute the
    same items."""
    return collect_window(
        window_text_of(subject_text, sub_bounds),
        forms,
        preceding_page=window_preceding_page(subject_text, sub_bounds),
    )


def split_into_item_groups(
    items: list[MentionWireItem], group_size: int
) -> list[list[MentionWireItem]]:
    """Ordered groups of at most *group_size* items. Always at least one group —
    the empty group is what the single-dummy-request path keys off."""
    if not items:
        return [[]]
    return [items[i : i + group_size] for i in range(0, len(items), group_size)]


def retry_items_of_window(
    subject_unique_id: str,
    field_type: ExtractionFieldType,
    sub_bounds: str,
    collection: WindowCollection,
    retry_mention_ids: Iterable[str],
) -> list[MentionWireItem]:
    """The window's wire items a retry pass re-asks for, in window order. A
    stored id the collection no longer yields means the text or forms changed
    under the stored state — raise, never guess."""
    wanted = set(retry_mention_ids)
    items = [item for item in collection.items if item.mention_id in wanted]
    unknown = wanted - {item.mention_id for item in items}
    if unknown:
        raise ValueError(
            f"mention_collection: retry mention ids {sorted(unknown)} for sub-window "
            f"{sub_bounds} in {subject_unique_id}:{field_type.name} are not among the "
            f"window's collected items; the stored retry state does not match the "
            f"text and forms it was computed from (re-defer)."
        )
    return items


def group_digest_payload(items: list[MentionWireItem]) -> list[list[str]]:
    """What a group's ``|ud=`` digests: its items, id and text, in order."""
    return [[item.mention_id, item.mention] for item in items]


# --- requests ------------------------------------------------------------------------------


def render_mention_location_context(wire_text: str, items: list[MentionWireItem]) -> str:
    """The user message: the window's text (excluded pages omitted), then the
    two fenced mention blocks at the very bottom (what the static promises the
    model). The hold reads the ids back off the first block."""
    return (
        f"text scraped from a manufacturer's website:\n{wire_text}\n\n"
        f"{render_mention_blocks([item.model_dump() for item in items])}"
    )


def create_mention_location_gpt_request(
    deferred_at: datetime,
    subject_unique_id: str,
    request_id: BatchRequestIDType,
    wire_text: str,
    items: list[MentionWireItem],
    phrase_mention_collection_prompt: Prompt,
    gpt_model: LLM_Model,
    model_params: GPTModelParams,
    eager: bool,
) -> GPTBatchRequest:
    return create_base_gpt_batch_request(
        deferred_at=deferred_at,
        subject_unique_id=subject_unique_id,
        custom_id=request_id,
        context=render_mention_location_context(wire_text, items),
        prompt_text=phrase_mention_collection_prompt.text,
        gpt_model=gpt_model,
        model_params=model_params.with_response_format(MENTION_COLLECTION_RESPONSE_SCHEMA),
        batch_id="Eager" if eager else None,
    )


def create_dummy_completed_mention_location_request(
    deferred_at: datetime,
    subject_unique_id: str,
    request_id: BatchRequestIDType,
    model_params: GPTModelParams,
    eager: bool,
) -> GPTBatchRequest:
    """A window with no mentions: one pre-answered request, so the stage
    completes and the fold sees the window (with its forms, no bundles)."""
    request = create_base_gpt_batch_request(
        deferred_at=deferred_at,
        subject_unique_id=subject_unique_id,
        custom_id=request_id,
        context="No mention location needed - no mentions collected in this window.",
        prompt_text="No mention location needed - no mentions collected in this window.",
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
    timestamp: datetime,
    llm_model: LLM_Model,
    model_params: GPTModelParams,
    max_mentions_per_request: int,
    eager: bool,
    BATCH_SIZE: int = 100,
) -> list[GPTBatchRequest]:
    """Fresh or only-missing requests, for every (chunk, sub-window, group) whose
    embedded id is in *missing_request_ids*. Re-collects from the text and the
    stored forms — the same pure computation the node minted the ids from."""
    logger.info(
        f"create_missing_mention_collection_requests: generating GPTBatchRequests "
        f"for {subject_unique_id}:{field_type.name}"
    )
    work: list[tuple[str, str, LLMPhraseExtractionRequestBundle]] = [
        (chunk_bounds, sub_bounds, bundle)
        for chunk_bounds, bundle in chunked_request_map.items()
        for sub_bounds in bundle.search_sub_bounds
        if set(bundle.llm_phrase_mention_req_ids.get(sub_bounds, [])) & missing_request_ids
        or set(bundle.llm_phrase_mention_retry_req_ids.get(sub_bounds, [])) & missing_request_ids
    ]

    batch_requests: list[GPTBatchRequest] = []
    for i in range(0, len(work), BATCH_SIZE):
        for chunk_bounds, sub_bounds, bundle in work[i : i + BATCH_SIZE]:
            forms = stored_window_forms(
                subject_unique_id, field_type, chunk_bounds, sub_bounds, bundle
            )
            collection = collect_sub_window(subject_text, sub_bounds, forms)
            groups = split_into_item_groups(collection.items, max_mentions_per_request)
            group_req_ids = bundle.llm_phrase_mention_req_ids.get(sub_bounds, [])
            if len(group_req_ids) != len(groups):
                raise ValueError(
                    f"create_missing_mention_collection_requests: embedded group count "
                    f"({len(group_req_ids)}) does not match the computed count "
                    f"({len(groups)}) for sub-window {sub_bounds} of chunk {chunk_bounds} "
                    f"in {subject_unique_id}:{field_type.name}. Both come from "
                    f"collect_sub_window over the same text and stored forms, so this "
                    f"should not happen."
                )
            start, end = window_bounds(sub_bounds)
            wire_text = wire_window_text(subject_text, start, end)

            # The retry pass: the stored missing ids, grouped like the first pass.
            retry_req_ids = bundle.llm_phrase_mention_retry_req_ids.get(sub_bounds, [])
            if set(retry_req_ids) & missing_request_ids:
                retry_items = retry_items_of_window(
                    subject_unique_id, field_type, sub_bounds, collection,
                    bundle.llm_phrase_mention_retry_mention_ids.get(sub_bounds, []),
                )
                retry_groups = split_into_item_groups(retry_items, max_mentions_per_request)
                if not retry_items or len(retry_req_ids) != len(retry_groups):
                    raise ValueError(
                        f"create_missing_mention_collection_requests: embedded retry group "
                        f"count ({len(retry_req_ids)}) does not match the computed count "
                        f"({len(retry_groups)}, {len(retry_items)} items) for sub-window "
                        f"{sub_bounds} of chunk {chunk_bounds} in "
                        f"{subject_unique_id}:{field_type.name}."
                    )
                for retry_group_index, retry_req_id in enumerate(retry_req_ids):
                    if retry_req_id not in missing_request_ids:
                        continue
                    logger.info(
                        f"Retrying location for {len(retry_groups[retry_group_index])} "
                        f"undescribed mention(s) in {subject_unique_id}:{field_type.name} "
                        f"sub-window {sub_bounds} retry group {retry_group_index}"
                    )
                    batch_requests.append(
                        create_mention_location_gpt_request(
                            deferred_at=timestamp,
                            subject_unique_id=subject_unique_id,
                            request_id=retry_req_id,
                            wire_text=wire_text,
                            items=retry_groups[retry_group_index],
                            phrase_mention_collection_prompt=phrase_mention_collection_prompt,
                            gpt_model=llm_model,
                            model_params=model_params,
                            eager=eager,
                        )
                    )

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
                        f"No mentions collected in sub-window {sub_bounds} for "
                        f"{subject_unique_id}:{field_type.name}; creating dummy "
                        f"mention-location request"
                    )
                    batch_requests.append(
                        create_dummy_completed_mention_location_request(
                            deferred_at=timestamp,
                            subject_unique_id=subject_unique_id,
                            request_id=group_req_id,
                            model_params=model_params,
                            eager=eager,
                        )
                    )
                    continue
                logger.info(
                    f"Sending {len(group)} mention(s) for location to "
                    f"{subject_unique_id}:{field_type.name} sub-window {sub_bounds} "
                    f"group {group_index}"
                )
                batch_requests.append(
                    create_mention_location_gpt_request(
                        deferred_at=timestamp,
                        subject_unique_id=subject_unique_id,
                        request_id=group_req_id,
                        wire_text=wire_text,
                        items=group,
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
) -> tuple[list[str], LocationsByMentionId, list[str]]:
    """ONE group request's (sent mention ids, held locations, answered ids that
    were never sent). The sent ids are read back off the request's own block —
    the dummy request has none and sends nothing. A malformed response is
    recorded against THIS request and raised."""
    req_obj = _completed_request(subject_unique_id, field_type, group_req_id, completed_request_map)
    response = req_obj.response
    if response is None:  # _completed_request raised already; this narrows the type
        raise ValueError(f"mention_collection: GPTBatchRequest {group_req_id} has no response")
    user_message = req_obj.request.body.user_message()
    sent_ids = sent_mention_ids_from_user_message(user_message) or []
    sent_set = set(sent_ids)
    try:
        by_id = parse_mention_location_response(response.result)
        unknown_ids = [mid for mid in by_id if mid not in sent_set]
        held = hold_response_to_sent_mention_ids(
            user_message=user_message,
            response_by_mention_id=by_id,
            where=f"{subject_unique_id}:{field_type.name} mention location {group_req_id}",
        )
    except Exception as e:
        await record_response_parse_error_capped(
            gpt_batch_request=req_obj,
            error_message=str(e),
            timestamp=timestamp,
            traceback_str=traceback.format_exc(),
        )
        logger.error(
            f"mention_collection: error parsing mention-location response "
            f"{group_req_id} for {subject_unique_id}:{field_type.name}: {e}"
        )
        raise
    return sent_ids, held, unknown_ids


@dataclass(frozen=True)
class WindowAnswer:
    """ONE sub-window's Location-stage answer, merged across its group requests
    and (when one ran) its retry pass: the sent ids in window order, the held
    locations, the ids the model answered that were never sent, and the ids the
    retry pass re-asked for."""

    sent_ids: list[str]
    locations: LocationsByMentionId
    unknown_answer_ids: list[str]
    retried_mention_ids: list[str]

    @property
    def missing_ids(self) -> list[str]:
        return [mid for mid in self.sent_ids if mid not in self.locations]


async def get_window_locations(
    subject_unique_id: str,
    field_type: ExtractionFieldType,
    chunk_bounds: str,
    sub_bounds: str,
    extraction_bundle: LLMPhraseExtractionRequestBundle,
    completed_request_map: dict[BatchRequestIDType, GPTBatchRequest],
    timestamp: datetime,
    *,
    include_retry: bool = True,
) -> WindowAnswer:
    """ONE sub-window's answer, merged across its groups and then (when
    *include_retry*, the default) its retry pass. An id is in exactly one group
    by construction; the first answer for a stray duplicate wins and the
    duplicate is warned. The retry pass only ever fills ids the groups left
    undescribed. ``include_retry=False`` is the assessment view: what the first
    pass alone described."""
    group_req_ids = extraction_bundle.llm_phrase_mention_req_ids.get(sub_bounds)
    if not group_req_ids:
        raise ValueError(
            f"mention_collection: no mention requests embedded for sub-window "
            f"{sub_bounds} of chunk {chunk_bounds} in {subject_unique_id}:{field_type.name}"
        )
    retry_req_ids = (
        extraction_bundle.llm_phrase_mention_retry_req_ids.get(sub_bounds, [])
        if include_retry
        else []
    )
    sent_ids: list[str] = []
    locations: LocationsByMentionId = {}
    unknown_answer_ids: list[str] = []
    for request_id in [*group_req_ids, *retry_req_ids]:
        group_sent, held, unknown = await parse_mention_group_result(
            subject_unique_id=subject_unique_id,
            field_type=field_type,
            group_req_id=request_id,
            completed_request_map=completed_request_map,
            timestamp=timestamp,
        )
        unknown_answer_ids.extend(unknown)
        sent_ids.extend(mid for mid in group_sent if mid not in sent_ids)
        for mention_id, location in held.items():
            if mention_id in locations:
                logger.warning(
                    f"mention_collection: mention id {mention_id!r} answered in two requests "
                    f"of sub-window {sub_bounds} in {subject_unique_id}:{field_type.name}; "
                    f"keeping the first"
                )
                continue
            locations[mention_id] = location
    return WindowAnswer(
        sent_ids=sent_ids,
        locations=locations,
        unknown_answer_ids=unknown_answer_ids,
        retried_mention_ids=list(
            extraction_bundle.llm_phrase_mention_retry_mention_ids.get(sub_bounds, [])
            if include_retry
            else []
        ),
    )


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
    """The chunk's aggregation fold: every sub-window re-collected from the text
    and its stored forms, located from its held answers, bundled over the union
    of the chunk's forms. Windows are folded in ``search_sub_bounds`` order
    (document order); each inherits the page the text before it was on."""
    if not extraction_bundle.search_sub_bounds:
        raise ValueError(
            f"mention_collection: chunk {chunk_bounds} has no search_sub_bounds in "
            f"{subject_unique_id}:{field_type.name}"
        )
    windows: list[WindowInput] = []
    for sub_bounds in extraction_bundle.search_sub_bounds:
        answer = await get_window_locations(
            subject_unique_id=subject_unique_id,
            field_type=field_type,
            chunk_bounds=chunk_bounds,
            sub_bounds=sub_bounds,
            extraction_bundle=extraction_bundle,
            completed_request_map=completed_request_map,
            timestamp=timestamp,
        )
        windows.append(
            WindowInput(
                text=window_text_of(subject_text, sub_bounds),
                sent_forms=stored_window_forms(
                    subject_unique_id, field_type, chunk_bounds, sub_bounds, extraction_bundle
                ),
                locations_by_mention_id=answer.locations,
                preceding_page=window_preceding_page(subject_text, sub_bounds),
                window_id=sub_bounds,
                retried_mention_ids=answer.retried_mention_ids,
                unknown_answer_ids=answer.unknown_answer_ids,
            )
        )
    return fold_document(windows, verb_fold=verb_fold)


def fold_verb_fold_of(metadata: object) -> bool:
    """The fold's verb-fold dial off a metadata object (any shape — the partial
    dump walks node CLASSES and generic metadata). False when absent."""
    fold_metadata: Optional[object] = getattr(metadata, "aggregation_fold", None)
    return bool(getattr(fold_metadata, "verb_fold", False))
