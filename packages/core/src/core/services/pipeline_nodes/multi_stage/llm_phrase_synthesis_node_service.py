"""Pipeline v3's synthesis stage (PIPELINE_V3_PLAN.md D15 as amended
2026-08-22, D16; Phase 3.2): one LLM description per GROUP, written from the
aggregation fold's entries — the stage after mention collection, per chunk.

WHAT A REQUEST CARRIES. The chunk's fold (``get_chunk_fold``, recomputed from
the text + the stored mention state — the fold is deterministic, nothing is
stored but request ids) yields one record per NON-EMPTY bundle, in bundle
order: ``{record_id: group_id, focal_form, entries: [{location, snippet}]}``
(``core.models.extraction_schemas.synthesis``). The FOCAL FORM is the
bundle's most frequent member form, chosen in code (``MentionBundle.focal_form``);
the model is told to describe that entity using the entries as evidence (user
decision 2026-08-22 — reverses D15's faithful-aggregation framing). The
LOCATION ARM (``BatchedSynthesisNodeMetadata.include_location``) decides
whether entries carry their Location-stage description or the snippet alone —
the A/B the user asked for; both arms can coexist (``|loc=1`` / ``|loc=0`` in
the id). The user message names the manufacturer (the static asks the model to
mask the name in its output) and carries the two fenced record blocks
(``render_synthesis_record_blocks``).

PACKING (D15). Records are packed in bundle order into requests of at most
``max_entries_per_request`` entries — a SOFT cutoff: a record is never split,
and a record larger than the cap travels alone. A chunk with no records gets
one dummy request answering ``{"syntheses": []}`` (the single-dummy-per-unit
convention every stage shares). A group's records are digested into its custom
id (``|ud=``): change what the fold yields and the stale answer is never found.

HOLD. A response is held EXACTLY on record ids: an id the model answered that
was never sent is DROPPED with a warning and reported (``unknown_answer_ids``
— a mis-echo, never a record), a sent id with no answer is MISSING. There is
no fabrication path: an unknown id cannot reach downstream, because results
are read by sent id.

UNDER-ANSWER POLICY (user decision 2026-08-22, same family as the Location
stage's). Once a chunk's group requests are complete the node ASSESSES it: the
record ids no answer synthesized are stored on the bundle
(``llm_phrase_synthesis_retry_record_ids``; an empty list = assessed, none
missing; None = not yet assessed) and, when any are missing, ONE retry pass
re-asks for exactly those records (packed the same way into
``llm_phrase_synthesis_retry_req_ids``, custom id ``…>chunk>{b}>retry>1>group>{j}>…``).
The result reads group answers first, then the retry's; what is still missing
after that is reported (``not_synthesized`` in the dump) — nothing is retried
twice, and a missing record never becomes a crash.
"""

from __future__ import annotations

import asyncio
import logging
import traceback
from dataclasses import dataclass, field, replace
from datetime import datetime
from typing import Optional

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
from core.models.extraction_results.extraction_node_metadata import (
    BatchedSynthesisNodeMetadata,
)
from core.models.extraction_results.llm_phrase_extraction_results import (
    LLMPhraseExtractionMetadata,
)
from core.models.extraction_schemas.synthesis import (
    DUMMY_SYNTHESIS_RESPONSE_CONTENT,
    SYNTHESIS_RESPONSE_SCHEMA,
    GroupRecord,
    GroupRecords,
    SynthesesByGroupId,
    SynthesisRecordInput,
    parse_synthesis_response,
)
from core.models.field_types import ExtractionFieldType
from core.services.phrase_blocks_contract import (
    render_synthesis_record_blocks,
    sent_record_ids_from_user_message,
)
from core.utils.designation_tokens import designation_coverage, missing_designations
from core.services.pipeline_nodes.multi_stage.llm_phrase_mention_collection_node_service import (
    fold_collapse_compounds_of,
    fold_snippet_radius_of,
    fold_verb_fold_of,
    get_chunk_fold,
)
from core.utils.aggregation_fold import FoldResult

logger = logging.getLogger(__name__)


# --- metadata -------------------------------------------------------------------------------


def require_synthesis_metadata(
    metadata: LLMPhraseExtractionMetadata,
) -> BatchedSynthesisNodeMetadata:
    """The synthesis node's metadata, which a v3 chain always carries. None means
    the pipeline was built without it — a factory wiring error, not a run state."""
    synthesis = metadata.llm_phrase_synthesis
    if synthesis is None:
        raise ValueError(
            "llm_phrase_synthesis metadata is None: the pipeline was built without the "
            "v3 synthesis stage's metadata (ExtractionPipelineFactory must pass it "
            "alongside the synthesis node)."
        )
    return synthesis


def synthesis_include_location_of(metadata: object) -> bool:
    """The location arm off a metadata object (any shape — the partial dump
    walks generic metadata). True (the with-location arm) when absent."""
    synthesis_metadata: Optional[object] = getattr(metadata, "llm_phrase_synthesis", None)
    value = getattr(synthesis_metadata, "include_location", None)
    return True if value is None else bool(value)


def synthesis_max_entries_of(metadata: object) -> Optional[int]:
    """The synthesis stage's soft entry cap off any pipeline metadata (duck-
    typed like ``synthesis_include_location_of``). None when the metadata
    carries no synthesis node — callers then skip request-level accounting."""
    synthesis_metadata: Optional[object] = getattr(metadata, "llm_phrase_synthesis", None)
    value = getattr(synthesis_metadata, "max_entries_per_request", None)
    return None if value is None else int(value)


# --- records + packing ---------------------------------------------------------------------


async def chunk_fold(
    subject_unique_id: str,
    field_type: ExtractionFieldType,
    chunk_bounds: str,
    extraction_bundle: LLMPhraseExtractionRequestBundle,
    mention_completed_request_map: dict[BatchRequestIDType, GPTBatchRequest],
    timestamp: datetime,
    *,
    subject_text: str,
    verb_fold: bool,
    snippet_radius: int,
    collapse_compounds: bool = False,
) -> FoldResult:
    """The chunk's fold as the mention stage left it — the one computation the
    node's id-minting pass, request creation and the result all rest on."""
    return await get_chunk_fold(
        subject_unique_id=subject_unique_id,
        field_type=field_type,
        chunk_bounds=chunk_bounds,
        extraction_bundle=extraction_bundle,
        completed_request_map=mention_completed_request_map,
        timestamp=timestamp,
        subject_text=subject_text,
        verb_fold=verb_fold,
        snippet_radius=snippet_radius,
        collapse_compounds=collapse_compounds,
    )


def pack_records(
    records: list[SynthesisRecordInput], max_entries_per_request: int
) -> list[list[SynthesisRecordInput]]:
    """Ordered groups of records under a SOFT entry cap: records are taken in
    order, a group closes when the next record would push it past the cap, a
    record is never split, a record over the cap alone fills its own group.
    Always at least one group — the empty group is what the single-dummy path
    keys off."""
    if max_entries_per_request < 1:
        raise ValueError(f"max_entries_per_request must be >= 1, got {max_entries_per_request}")
    if not records:
        return [[]]
    groups: list[list[SynthesisRecordInput]] = []
    current: list[SynthesisRecordInput] = []
    count = 0
    for record in records:
        size = len(record.entries)
        if current and count + size > max_entries_per_request:
            groups.append(current)
            current, count = [], 0
        current.append(record)
        count += size
    if current:
        groups.append(current)
    return groups


def retry_records_of_chunk(
    subject_unique_id: str,
    field_type: ExtractionFieldType,
    chunk_bounds: str,
    records: list[SynthesisRecordInput],
    retry_record_ids: list[str],
) -> list[SynthesisRecordInput]:
    """The chunk's records a retry pass re-asks for, in bundle order. A stored
    id the fold no longer yields means the text or mention state changed under
    the stored state — raise, never guess."""
    wanted = set(retry_record_ids)
    chosen = [r for r in records if r.record_id in wanted]
    unknown = wanted - {r.record_id for r in chosen}
    if unknown:
        raise ValueError(
            f"synthesis: retry record ids {sorted(unknown)} for chunk {chunk_bounds} in "
            f"{subject_unique_id}:{field_type.name} are not among the chunk's fold "
            f"records; the stored retry state does not match the text and mention "
            f"state it was computed from (re-defer)."
        )
    return chosen


def group_digest_payload(records: list[SynthesisRecordInput]) -> list[dict]:
    """What a group's ``|ud=`` digests: its records exactly as the wire shows
    them (ids, focal forms, entries — location included only on that arm)."""
    return [record.wire_dict() for record in records]


# --- requests --------------------------------------------------------------------------------


def render_synthesis_context(subject_name: str, records: list[SynthesisRecordInput]) -> str:
    """The user message: the manufacturer's name (the static asks the model to
    mask it), then the two fenced record blocks at the very bottom. The hold
    reads the ids back off the first block."""
    return (
        f"the name of the manufacturer in question: {subject_name}\n\n"
        f"{render_synthesis_record_blocks(group_digest_payload(records))}"
    )


def create_synthesis_gpt_request(
    deferred_at: datetime,
    subject_unique_id: str,
    request_id: BatchRequestIDType,
    subject_name: str,
    records: list[SynthesisRecordInput],
    phrase_synthesis_prompt: Prompt,
    gpt_model: LLM_Model,
    model_params: GPTModelParams,
    eager: bool,
) -> GPTBatchRequest:
    return create_base_gpt_batch_request(
        deferred_at=deferred_at,
        subject_unique_id=subject_unique_id,
        custom_id=request_id,
        context=render_synthesis_context(subject_name, records),
        prompt_text=phrase_synthesis_prompt.text,
        gpt_model=gpt_model,
        model_params=model_params.with_response_format(SYNTHESIS_RESPONSE_SCHEMA),
        batch_id="Eager" if eager else None,
    )


def create_dummy_completed_synthesis_request(
    deferred_at: datetime,
    subject_unique_id: str,
    request_id: BatchRequestIDType,
    model_params: GPTModelParams,
    eager: bool,
) -> GPTBatchRequest:
    """A chunk with no records: one pre-answered request, so the stage
    completes and the result sees the chunk (no syntheses)."""
    request = create_base_gpt_batch_request(
        deferred_at=deferred_at,
        subject_unique_id=subject_unique_id,
        custom_id=request_id,
        context="No synthesis needed - no records in this chunk.",
        prompt_text="No synthesis needed - no records in this chunk.",
        gpt_model=NO_MODEL,
        model_params=model_params,
        batch_id="Eager" if eager else "dummy_synthesis_batch_id",
    )
    request.response = get_dummy_gpt_batch_response(
        deferred_at=deferred_at,
        request_custom_id=request_id,
        dummy_chat_completion_id="dummy_completion_id",
        chat_completion_choice_message=ChatCompletionChoiceMessage(
            role="assistant", content=DUMMY_SYNTHESIS_RESPONSE_CONTENT
        ),
    )
    return request


async def create_missing_synthesis_requests(
    subject_unique_id: str,
    field_type: ExtractionFieldType,
    chunked_request_map: LLMPhraseExtractionRequestMap,
    missing_request_ids: set[BatchRequestIDType],
    subject_text: str,
    subject_name: str,
    mention_completed_request_map: dict[BatchRequestIDType, GPTBatchRequest],
    phrase_synthesis_prompt: Prompt,
    timestamp: datetime,
    llm_model: LLM_Model,
    model_params: GPTModelParams,
    max_entries_per_request: int,
    include_location: bool,
    verb_fold: bool,
    snippet_radius: int,
    eager: bool,
    collapse_compounds: bool = False,
    BATCH_SIZE: int = 100,
) -> list[GPTBatchRequest]:
    """Fresh or only-missing requests, for every (chunk, group) whose embedded id
    is in *missing_request_ids*. Recomputes the chunk's fold and records — the
    same pure computation the node minted the ids from."""
    logger.info(
        f"create_missing_synthesis_requests: generating GPTBatchRequests for "
        f"{subject_unique_id}:{field_type.name}"
    )
    work = [
        (chunk_bounds, bundle)
        for chunk_bounds, bundle in chunked_request_map.items()
        if set(bundle.llm_phrase_synthesis_req_ids) & missing_request_ids
        or set(bundle.llm_phrase_synthesis_retry_req_ids) & missing_request_ids
    ]

    batch_requests: list[GPTBatchRequest] = []
    for i in range(0, len(work), BATCH_SIZE):
        for chunk_bounds, bundle in work[i : i + BATCH_SIZE]:
            fold = await chunk_fold(
                subject_unique_id,
                field_type,
                chunk_bounds,
                bundle,
                mention_completed_request_map,
                timestamp,
                subject_text=subject_text,
                verb_fold=verb_fold,
                snippet_radius=snippet_radius,
                collapse_compounds=collapse_compounds,
            )
            records = fold.synthesis_records(include_location=include_location)
            groups = pack_records(records, max_entries_per_request)
            group_req_ids = bundle.llm_phrase_synthesis_req_ids
            if len(group_req_ids) != len(groups):
                raise ValueError(
                    f"create_missing_synthesis_requests: embedded group count "
                    f"({len(group_req_ids)}) does not match the computed count "
                    f"({len(groups)}) for chunk {chunk_bounds} in "
                    f"{subject_unique_id}:{field_type.name}. Both come from the chunk's "
                    f"fold over the same text and mention state, so this should not happen."
                )

            # The retry pass: the stored missing ids, packed like the first pass.
            retry_req_ids = bundle.llm_phrase_synthesis_retry_req_ids
            if set(retry_req_ids) & missing_request_ids:
                retry_records = retry_records_of_chunk(
                    subject_unique_id,
                    field_type,
                    chunk_bounds,
                    records,
                    bundle.llm_phrase_synthesis_retry_record_ids or [],
                )
                retry_groups = pack_records(retry_records, max_entries_per_request)
                if not retry_records or len(retry_req_ids) != len(retry_groups):
                    raise ValueError(
                        f"create_missing_synthesis_requests: embedded retry group count "
                        f"({len(retry_req_ids)}) does not match the computed count "
                        f"({len(retry_groups)}, {len(retry_records)} records) for chunk "
                        f"{chunk_bounds} in {subject_unique_id}:{field_type.name}."
                    )
                for retry_group_index, retry_req_id in enumerate(retry_req_ids):
                    if retry_req_id not in missing_request_ids:
                        continue
                    logger.info(
                        f"Retrying synthesis for {len(retry_groups[retry_group_index])} "
                        f"unsynthesized record(s) in {subject_unique_id}:{field_type.name} "
                        f"chunk {chunk_bounds} retry group {retry_group_index}"
                    )
                    batch_requests.append(
                        create_synthesis_gpt_request(
                            deferred_at=timestamp,
                            subject_unique_id=subject_unique_id,
                            request_id=retry_req_id,
                            subject_name=subject_name,
                            records=retry_groups[retry_group_index],
                            phrase_synthesis_prompt=phrase_synthesis_prompt,
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
                            f"create_missing_synthesis_requests: unexpected empty group at "
                            f"index {group_index} of {len(groups)} for chunk {chunk_bounds} "
                            f"in {subject_unique_id}:{field_type.name}."
                        )
                    logger.info(
                        f"No records to synthesize in chunk {chunk_bounds} for "
                        f"{subject_unique_id}:{field_type.name}; creating dummy synthesis request"
                    )
                    batch_requests.append(
                        create_dummy_completed_synthesis_request(
                            deferred_at=timestamp,
                            subject_unique_id=subject_unique_id,
                            request_id=group_req_id,
                            model_params=model_params,
                            eager=eager,
                        )
                    )
                    continue
                logger.info(
                    f"Sending {len(group)} record(s) ({sum(len(r.entries) for r in group)} "
                    f"entries) for synthesis to {subject_unique_id}:{field_type.name} chunk "
                    f"{chunk_bounds} group {group_index}"
                )
                batch_requests.append(
                    create_synthesis_gpt_request(
                        deferred_at=timestamp,
                        subject_unique_id=subject_unique_id,
                        request_id=group_req_id,
                        subject_name=subject_name,
                        records=group,
                        phrase_synthesis_prompt=phrase_synthesis_prompt,
                        gpt_model=llm_model,
                        model_params=model_params,
                        eager=eager,
                    )
                )
        await asyncio.sleep(0)  # yield to the event loop between batches
    return batch_requests


# --- parse + hold ----------------------------------------------------------------------------


def _completed_request(
    subject_unique_id: str,
    field_type: ExtractionFieldType,
    group_req_id: BatchRequestIDType,
    completed_request_map: dict[BatchRequestIDType, GPTBatchRequest],
) -> GPTBatchRequest:
    req_obj = completed_request_map.get(group_req_id)
    if not req_obj:
        raise ValueError(
            f"synthesis: missing GPTBatchRequest for request id {group_req_id} in "
            f"{subject_unique_id}:{field_type.name}"
        )
    if not req_obj.response:
        raise ValueError(
            f"synthesis: GPTBatchRequest {group_req_id} has no response in "
            f"{subject_unique_id}:{field_type.name}"
        )
    return req_obj


async def parse_synthesis_group_result(
    subject_unique_id: str,
    field_type: ExtractionFieldType,
    group_req_id: BatchRequestIDType,
    completed_request_map: dict[BatchRequestIDType, GPTBatchRequest],
    timestamp: datetime,
) -> tuple[list[str], SynthesesByGroupId, list[str]]:
    """ONE group request's (sent record ids, held syntheses, answered ids that
    were never sent). The sent ids are read back off the request's own block —
    the dummy request has none and sends nothing. Held EXACTLY: unknown ids
    are dropped and reported, missing ids are simply absent (the caller's
    assessment decides the retry). A malformed response is recorded against
    THIS request and raised."""
    req_obj = _completed_request(subject_unique_id, field_type, group_req_id, completed_request_map)
    response = req_obj.response
    if response is None:  # _completed_request raised already; this narrows the type
        raise ValueError(f"synthesis: GPTBatchRequest {group_req_id} has no response")
    user_message = req_obj.request.body.user_message()
    sent_ids = sent_record_ids_from_user_message(user_message) or []
    sent_set = set(sent_ids)
    where = f"{subject_unique_id}:{field_type.name} synthesis {group_req_id}"
    try:
        by_id = parse_synthesis_response(response.result)
    except Exception as e:
        await record_response_parse_error_capped(
            gpt_batch_request=req_obj,
            error_message=str(e),
            timestamp=timestamp,
            traceback_str=traceback.format_exc(),
        )
        logger.error(
            f"synthesis: error parsing synthesis response {group_req_id} for "
            f"{subject_unique_id}:{field_type.name}: {e}"
        )
        raise
    unknown_ids = [rid for rid in by_id if rid not in sent_set]
    if unknown_ids:
        logger.warning(
            f"{where}: dropping {len(unknown_ids)} response record id(s) that were never "
            f"sent: {unknown_ids}"
        )
    held: SynthesesByGroupId = {rid: by_id[rid] for rid in sent_ids if rid in by_id}
    missing = [rid for rid in sent_ids if rid not in by_id]
    if missing:
        logger.warning(
            f"{where}: response synthesized {len(held)} of {len(sent_ids)} sent records; "
            f"nothing came back for {missing}"
        )
    return sent_ids, held, unknown_ids


@dataclass(frozen=True)
class ChunkAnswer:
    """ONE chunk's synthesis answer, merged across its group requests and (when
    one ran) its retry pass: the sent record ids in request order, the held
    syntheses, the ids the model answered that were never sent, and the ids the
    retry pass re-asked for.

    ``syntheses`` is the RESOLVED map (one answer per record). Where a record
    was re-asked for UNDER-ENUMERATION (2026-09-02: it had a first answer that
    dropped designations), both answers exist for a while — the retry's is kept
    in ``retry_syntheses`` and ``resolve_under_enumeration`` picks the better
    of the two by designation coverage, retry winning ties."""

    sent_ids: list[str]
    syntheses: SynthesesByGroupId
    unknown_answer_ids: list[str]
    retried_record_ids: list[str]
    retry_syntheses: SynthesesByGroupId = field(default_factory=dict)

    @property
    def missing_ids(self) -> list[str]:
        return [rid for rid in self.sent_ids if rid not in self.syntheses]


async def get_chunk_syntheses(
    subject_unique_id: str,
    field_type: ExtractionFieldType,
    chunk_bounds: str,
    extraction_bundle: LLMPhraseExtractionRequestBundle,
    completed_request_map: dict[BatchRequestIDType, GPTBatchRequest],
    timestamp: datetime,
    *,
    include_retry: bool = True,
) -> ChunkAnswer:
    """ONE chunk's answer, merged across its groups and then (when
    *include_retry*, the default) its retry pass. A record is in exactly one
    group by construction; the first answer for a stray duplicate wins and the
    duplicate is warned. ``include_retry=False`` is the assessment view: what
    the first pass alone synthesized."""
    group_req_ids = extraction_bundle.llm_phrase_synthesis_req_ids
    if not group_req_ids:
        raise ValueError(
            f"synthesis: no synthesis requests embedded for chunk {chunk_bounds} in "
            f"{subject_unique_id}:{field_type.name}"
        )
    retry_req_ids = extraction_bundle.llm_phrase_synthesis_retry_req_ids if include_retry else []
    sent_ids: list[str] = []
    syntheses: SynthesesByGroupId = {}
    retry_syntheses: SynthesesByGroupId = {}
    unknown_answer_ids: list[str] = []
    for request_id, is_retry in [
        *((rid, False) for rid in group_req_ids),
        *((rid, True) for rid in retry_req_ids),
    ]:
        group_sent, held, unknown = await parse_synthesis_group_result(
            subject_unique_id=subject_unique_id,
            field_type=field_type,
            group_req_id=request_id,
            completed_request_map=completed_request_map,
            timestamp=timestamp,
        )
        unknown_answer_ids.extend(unknown)
        if not is_retry:
            sent_ids.extend(rid for rid in group_sent if rid not in sent_ids)
        target = retry_syntheses if is_retry else syntheses
        for record_id, synthesis in held.items():
            if record_id in target:
                logger.warning(
                    f"synthesis: record id {record_id!r} answered in two requests of one "
                    f"pass of chunk {chunk_bounds} in {subject_unique_id}:{field_type.name}; "
                    f"keeping the first"
                )
                continue
            target[record_id] = synthesis
    # A record only the retry answered (the original missing-answer case) is
    # resolved here; a record BOTH passes answered (an under-enumeration
    # re-ask) stays in retry_syntheses for resolve_under_enumeration, which
    # has the records and can compare designation coverage.
    for record_id, synthesis in retry_syntheses.items():
        syntheses.setdefault(record_id, synthesis)
    return ChunkAnswer(
        sent_ids=sent_ids,
        syntheses=syntheses,
        unknown_answer_ids=unknown_answer_ids,
        retried_record_ids=list(
            (extraction_bundle.llm_phrase_synthesis_retry_record_ids or [])
            if include_retry
            else []
        ),
        retry_syntheses=retry_syntheses,
    )


# --- under-enumeration (2026-09-02, Phase B of the search-recall roadmap) --------------------


def _record_entry_texts(record: SynthesisRecordInput) -> list[str]:
    """The verbatim evidence a record's designations are read from: snippets
    only — the Location stage's descriptions are model-authored, never a
    source of demanded tokens."""
    return [entry.snippet for entry in record.entries]


def under_enumerated_record_ids(
    records: list[SynthesisRecordInput], syntheses: SynthesesByGroupId
) -> list[str]:
    """Records whose ANSWERED synthesis drops designation-shaped tokens their
    entries carry (the conservation check; see ``core.utils.designation_tokens``
    for the tiers and why they are precision-first). Records with no answer are
    the missing-answer path's business, not this one's. Order follows
    *records* — bundle order, like every other id list here."""
    flagged: list[str] = []
    for record in records:
        synthesis = syntheses.get(record.record_id)
        if synthesis is None:
            continue
        missing = missing_designations(
            _record_entry_texts(record), synthesis, focal_form=record.focal_form
        )
        if missing:
            flagged.append(record.record_id)
    return flagged


def resolve_under_enumeration(
    answer: ChunkAnswer, records: list[SynthesisRecordInput]
) -> tuple[ChunkAnswer, dict[str, str]]:
    """One answer per record where both passes answered: keep whichever names
    more of the record's designations, the retry winning ties (it ran under
    the same prompt with a fresh sample — the measured recovery path). Returns
    the resolved answer and ``{record_id: "first"|"retry"}`` provenance for
    the records that were actually contested."""
    contested = [
        record
        for record in records
        if record.record_id in answer.retry_syntheses
        and record.record_id in answer.syntheses
        and answer.retry_syntheses[record.record_id] != answer.syntheses[record.record_id]
    ]
    if not contested:
        return answer, {}
    resolved = dict(answer.syntheses)
    provenance: dict[str, str] = {}
    for record in contested:
        entry_texts = _record_entry_texts(record)
        first_text = answer.syntheses[record.record_id]
        retry_text = answer.retry_syntheses[record.record_id]
        first_present, _ = designation_coverage(
            entry_texts, first_text, focal_form=record.focal_form
        )
        retry_present, _ = designation_coverage(
            entry_texts, retry_text, focal_form=record.focal_form
        )
        if retry_present >= first_present:
            resolved[record.record_id] = retry_text
            provenance[record.record_id] = "retry"
        else:
            provenance[record.record_id] = "first"
    return replace(answer, syntheses=resolved), provenance


# --- the result ------------------------------------------------------------------------------


@dataclass(frozen=True)
class ChunkSynthesisResult:
    """What the synthesis stage leaves for one chunk: the fold it was written
    from, the records it sent (focal forms + entries, on the arm that ran), and
    the held answer. ``syntheses`` is keyed by group_id — the value that rode
    the wire as ``record_id`` (D16 naming note)."""

    fold: FoldResult
    records: list[SynthesisRecordInput]
    include_location: bool
    answer: ChunkAnswer
    group_request_count: int
    retry_request_count: int
    # {record_id: "first"|"retry"} for records where BOTH passes answered and
    # the under-enumeration comparator picked one (2026-09-02). Empty when
    # nothing was contested — the pre-extension shape.
    under_enumeration_resolved: dict[str, str] = field(default_factory=dict)

    @property
    def syntheses(self) -> SynthesesByGroupId:
        return self.answer.syntheses

    @property
    def not_synthesized(self) -> list[str]:
        """Records sent and never synthesized, after the retry pass."""
        return self.answer.missing_ids

    def synthesis_of(self, group_id: str) -> Optional[str]:
        return self.answer.syntheses.get(group_id)


async def get_chunk_synthesis_result(
    subject_unique_id: str,
    field_type: ExtractionFieldType,
    chunk_bounds: str,
    extraction_bundle: LLMPhraseExtractionRequestBundle,
    completed_request_map: dict[BatchRequestIDType, GPTBatchRequest],
    timestamp: datetime,
    *,
    mention_completed_request_map: dict[BatchRequestIDType, GPTBatchRequest],
    subject_text: str,
    verb_fold: bool,
    snippet_radius: int,
    include_location: bool,
    collapse_compounds: bool = False,
) -> ChunkSynthesisResult:
    """The chunk's fold, its records, and the held syntheses (groups, then
    retry). The sent ids the requests carry and the fold's records are the
    same computation; a disagreement means the text or mention state changed
    under the stored ids and is raised, never papered over."""
    fold = await chunk_fold(
        subject_unique_id,
        field_type,
        chunk_bounds,
        extraction_bundle,
        mention_completed_request_map,
        timestamp,
        subject_text=subject_text,
        verb_fold=verb_fold,
        snippet_radius=snippet_radius,
        collapse_compounds=collapse_compounds,
    )
    records = fold.synthesis_records(include_location=include_location)
    answer = await get_chunk_syntheses(
        subject_unique_id=subject_unique_id,
        field_type=field_type,
        chunk_bounds=chunk_bounds,
        extraction_bundle=extraction_bundle,
        completed_request_map=completed_request_map,
        timestamp=timestamp,
    )
    record_ids = [r.record_id for r in records]
    if set(answer.sent_ids) != set(record_ids):
        raise ValueError(
            f"synthesis: the sent record ids of chunk {chunk_bounds} in "
            f"{subject_unique_id}:{field_type.name} do not match the chunk's fold records "
            f"(sent-only {sorted(set(answer.sent_ids) - set(record_ids))}, fold-only "
            f"{sorted(set(record_ids) - set(answer.sent_ids))}); the text or mention state "
            f"changed under the stored request ids (re-defer)."
        )
    answer, under_enumeration_resolved = resolve_under_enumeration(answer, records)
    if under_enumeration_resolved:
        kept_retry = sum(1 for v in under_enumeration_resolved.values() if v == "retry")
        logger.info(
            f"[{subject_unique_id}] synthesis: chunk {chunk_bounds} "
            f"({field_type.name}) had {len(under_enumeration_resolved)} "
            f"under-enumeration re-ask(s) resolved ({kept_retry} kept the retry)."
        )
    return ChunkSynthesisResult(
        fold=fold,
        records=records,
        include_location=include_location,
        answer=answer,
        group_request_count=len(extraction_bundle.llm_phrase_synthesis_req_ids),
        retry_request_count=len(extraction_bundle.llm_phrase_synthesis_retry_req_ids),
        under_enumeration_resolved=under_enumeration_resolved,
    )

# --- downstream records (D16) ----------------------------------------------------------------


def downstream_group_records(result: ChunkSynthesisResult) -> GroupRecords:
    """The chunk's per-group records for every stage downstream of synthesis
    (D16): one ``GroupRecord`` per SYNTHESIZED group, keyed ``group_id``, in
    the fold's bundle order (dicts preserve insertion order, and group
    membership downstream is part of request identity, so the order must be
    the fold's, not a hash's).

    A group left unsynthesized after the retry pass has no synthesis to judge
    and is ABSENT — the v3 analog of the v2 ``records_with_mentions`` filter
    (a record with nothing to show downstream is skipped, never invented). It
    stays visible in the synthesis dump as ``not_synthesized``.
    """
    records: GroupRecords = {}
    for record in result.records:
        synthesis = result.synthesis_of(record.record_id)
        if synthesis is None:
            continue
        records[record.record_id] = GroupRecord(
            focal_form=record.focal_form, synthesis=synthesis
        )
    return records


async def get_chunk_group_records(
    subject_unique_id: str,
    field_type: ExtractionFieldType,
    chunk_bounds: str,
    extraction_bundle: LLMPhraseExtractionRequestBundle,
    timestamp: datetime,
    *,
    synthesis_completed_request_map: dict[BatchRequestIDType, GPTBatchRequest],
    mention_completed_request_map: dict[BatchRequestIDType, GPTBatchRequest],
    subject_text: str,
    metadata: object,
) -> GroupRecords:
    """ONE derivation for every downstream consumer (grounding, OOV,
    screening, descent, reconcile): the chunk's synthesis result — the fold
    recomputed from the stored mention answers, the held syntheses — reduced
    to its per-group records. The fold knobs are read off *metadata* the same
    way the synthesis node itself reads them, so a consumer can never disagree
    with the stage it consumes about what a group contains."""
    result = await get_chunk_synthesis_result(
        subject_unique_id=subject_unique_id,
        field_type=field_type,
        chunk_bounds=chunk_bounds,
        extraction_bundle=extraction_bundle,
        completed_request_map=synthesis_completed_request_map,
        timestamp=timestamp,
        mention_completed_request_map=mention_completed_request_map,
        subject_text=subject_text,
        verb_fold=fold_verb_fold_of(metadata),
        snippet_radius=fold_snippet_radius_of(metadata),
        include_location=synthesis_include_location_of(metadata),
        collapse_compounds=fold_collapse_compounds_of(metadata),
    )
    return downstream_group_records(result)
