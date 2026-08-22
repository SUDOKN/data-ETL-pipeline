"""Pipeline v2's recursive descent: passed in-vocab concepts, deepened level by
level over the records that evidence them.

The descent runs POST-SCREENING (fork F10): its seed is every in-vocab
candidate that passed screening, grouped tag-major by
``pipeline_v2_derivations.descent_seed_tagging_results`` — the same
``TaggingResult`` shape the v1 machinery consumed, with record ids where
phrases used to be. Descent requests carry record payloads (mentions +
synthesis from the masked relationship results) plus the parent's children as
options; responses are record-keyed and parsed by the shared grounding parse
with MINTED labels allowed, because RGR-M3 lets the model propose a sibling
type of its own. Survivors are not re-screened.

The sentinel is gone: a record from which nothing more specific qualifies
answers an empty ``options`` array with an explanation (the structural
declination), so a parent whose records all decline simply produces no
children — the empty-options stop. ``false_child`` remains: a response can
still name a real concept that is not a child of the parent it was asked
under, and that event is recorded on the node rather than asserted as a
finding.
"""

from __future__ import annotations

import asyncio
import json
import logging
import traceback
from datetime import datetime
from typing import Any, Optional

from requests.structures import CaseInsensitiveDict

from llm_providers.db_models.gpt_batch_request import (
    GPTBatchRequest,
)
from core.models.extraction_schemas.grounding import (
    RecordGroundingResults,
    is_sentinel_grounding_label,
)
from core.models.extraction_schemas.iterative_tagging import (
    IterativelyTaggedPhraseGroup,
    IterativeGroundingResult,
    IterativelyTaggedPhrase,
    PhraseTrail,
)
from core.models.extraction_schemas.relationship import (
    MaskedLLMPhraseRelationshipResults,
)
from core.models.rule_catalog import (
    STAGE_INITIAL_GROUNDING,
    STAGE_RECURSIVE_GROUNDING,
    RuleCatalog,
)
from core.models.extraction_schemas.catalog_wire_schema import (
    response_format_for,
)
from llm_providers.models.llm_model import (
    LLM_Model,
)
from llm_providers.models.file_objects.prompt import Prompt
from core.services.phrase_blocks_contract import (
    hold_response_to_sent_record_ids,
    render_record_blocks,
)
from core.models.deferred_extraction.deferred_concept_extraction import (
    ConceptExtractionRequestBundle,
    ConceptExtractionRequestMap,
    IterativeTaggingRequest,
    TaggingResult,
    TaggingResultsGroupedByConcept,
)
from core.models.skos_concept import Concept, ConceptJSONEncoder
from core.models.field_types import (
    ConceptFieldType,
)
from llm_providers.models.open_ai.gpt_model_params import (
    GPTModelParams,
)
from llm_providers.field_types import BatchRequestIDType

from llm_providers.services.gpt_batch_request.gpt_batch_request_writes import (
    record_response_parse_error,
)
from llm_providers.services.gpt_batch_request.gpt_batch_request_service import (
    create_base_gpt_batch_request,
)
from core.services.pipeline_nodes.multi_stage.llm_grounding_node_service import (
    get_record_grounding_result,
    parse_record_grounding_result,
)
from core.services.pipeline_nodes.multi_stage.llm_phrase_relationship_node_service import (
    get_masked_phrase_relationship_result,
)
from core.services.pipeline_nodes.multi_stage.llm_relationship_screening_node_service import (
    get_record_screening_result,
    screening_catalog_for,
)
from core.services.pipeline_nodes.multi_stage.pipeline_v2_derivations import (
    descent_seed_tagging_results,
)
from core.services.rule_catalog_registry import get_rule_catalog

logger = logging.getLogger(__name__)


def recursive_catalog_for(field_name: str) -> RuleCatalog:
    return get_rule_catalog(STAGE_RECURSIVE_GROUNDING, field_name)


def in_vocab_catalog_for(field_name: str) -> RuleCatalog:
    return get_rule_catalog(STAGE_INITIAL_GROUNDING, field_name)


async def get_descent_seed_tagging_results(
    subject_unique_id: str,
    field_type: ConceptFieldType,
    chunk_bounds: str,
    extraction_bundle: ConceptExtractionRequestBundle,
    completed_in_vocab_grounding_req_map: dict[BatchRequestIDType, GPTBatchRequest],
    completed_screening_req_map: dict[BatchRequestIDType, GPTBatchRequest],
    match_label_to_concept_map: CaseInsensitiveDict[Concept],
    timestamp: datetime,
) -> list[TaggingResult]:
    """The chunk's descent seed: every PASSED in-vocab candidate, tag-major.

    Both the embed walk (which counts and names descent nodes) and request
    creation derive the seed through here, so the two can never disagree about
    which concepts a chunk descends.
    """
    in_vocab_results = await get_record_grounding_result(
        stage_label="in-vocab grounding",
        subject_unique_id=subject_unique_id,
        field_name=field_type.name,
        chunk_bounds=chunk_bounds,
        catalog=in_vocab_catalog_for(field_type.name),
        group_req_ids=extraction_bundle.llm_phrase_initial_grounding_req_ids,
        completed_request_map=completed_in_vocab_grounding_req_map,
        timestamp=timestamp,
        allowed_labels=list(match_label_to_concept_map.keys()),
    )
    screening_results = await get_record_screening_result(
        subject_unique_id=subject_unique_id,
        field_name=field_type.name,
        chunk_bounds=chunk_bounds,
        catalog=screening_catalog_for(field_type.name),
        group_req_ids=extraction_bundle.llm_phrase_relationship_screening_req_ids,
        completed_request_map=completed_screening_req_map,
        timestamp=timestamp,
    )
    return descent_seed_tagging_results(in_vocab_results, screening_results)


def get_tagging_results_from_record_groundings(
    groundings: RecordGroundingResults,
) -> list[TaggingResult]:
    """A record-keyed grounding result regrouped tag-major — the shape the
    descent walk consumes. Declined records (empty tags) contribute nothing:
    that is the empty-options stop."""
    tag_to_tr_map: dict[str, TaggingResult] = {}
    for record_id, entry in groundings.items():
        for tag, applied_rules in entry.tags.items():
            tr = tag_to_tr_map.setdefault(
                tag, TaggingResult(group_id=tag, phrase_rules_map={})
            )
            tr.phrase_rules_map[record_id] = applied_rules
    return list(tag_to_tr_map.values())


def get_tcs_and_oov_trs_from_trs(
    trs: list[TaggingResult],
    match_label_to_concept_map: CaseInsensitiveDict[Concept],  # DO NOT MUTATE
) -> tuple[list[TaggingResult], list[TaggingResultsGroupedByConcept]]:
    """Split tag-major results into recognized concepts (altLabels folded onto
    their concept) and out-of-vocab proposals."""
    oov_trs: list[TaggingResult] = []
    concept_to_tc_map: dict[Concept, TaggingResultsGroupedByConcept] = {}

    for tr in trs:
        concept_obj = match_label_to_concept_map.get(
            tr.group_id
        )  # NOTE: multiple group tags may point to same concept_obj because they can be name/altLabels

        if not concept_obj:
            oov_trs.append(tr)
            continue

        tc = concept_to_tc_map.setdefault(
            concept_obj,
            TaggingResultsGroupedByConcept(
                concept=concept_obj,
                og_tag_w_phrase_rules_map={},
            ),
        )
        if tr.group_id in tc.og_tag_w_phrase_rules_map:
            raise ValueError(
                f"tr.group_id:{tr.group_id} was already present in "
                f"tc.og_tag_w_phrase_rules_map[tr.group_id]:{tc.og_tag_w_phrase_rules_map[tr.group_id]}"
            )
        tc.og_tag_w_phrase_rules_map[tr.group_id] = tr.phrase_rules_map

    return (oov_trs, list(concept_to_tc_map.values()))


def get_descend_worthy_tcs_from_tagged_results(
    seed_trs: list[TaggingResult],
    match_label_to_concept_map: CaseInsensitiveDict[Concept],
) -> list[TaggingResultsGroupedByConcept]:
    """Every recognized seeded concept, grouped: they all descend.

    The seed is the passed in-vocab candidates, whose labels the in-vocab parse
    already held to the vocabulary, so the out-of-vocab half of the split is
    empty by construction here. Leaf concepts still never descend, but that is
    the descent gate's business (``get_descendable_concept``), not a label
    comparison's.
    """
    _, tcs = get_tcs_and_oov_trs_from_trs(
        trs=seed_trs, match_label_to_concept_map=match_label_to_concept_map
    )
    return tcs


async def parse_recursive_grounding_batch_request_result(
    subject_unique_id: str,
    field_type: ConceptFieldType,
    chunk_bounds: str,
    descend_req_id: BatchRequestIDType,
    completed_request_map: dict[BatchRequestIDType, GPTBatchRequest],
    deferred_at: datetime,
) -> list[TaggingResult]:
    if not descend_req_id:
        raise ValueError(
            f"phrase_recursive_grounding_node.parse_batch_request_result: phrase_recursive_grounding_request_id is None for chunk bounds {chunk_bounds} in {subject_unique_id}:{field_type.name}"
        )
    gpt_req = completed_request_map.get(descend_req_id)
    if not gpt_req:
        raise ValueError(
            f"phrase_recursive_grounding_node.parse_batch_request_result: Missing GPTBatchRequest for phrase_recursive_grounding request ID {descend_req_id} in {subject_unique_id}:{field_type.name}"
        )
    if not gpt_req.response or not gpt_req.response.result:
        raise ValueError(
            f"phrase_recursive_grounding_node.parse_batch_request_result: GPTBatchRequest for phrase_recursive_grounding request ID {descend_req_id} has no response in {subject_unique_id}:{field_type.name}"
        )

    try:
        # Minted labels allowed on purpose: RGR-M3 lets the model propose a
        # sibling type of its own, so this stage has no vocabulary hold. What
        # the walk does with an unrecognized or non-child label is its own
        # classification (OOV proposal / false child), not a parse defect.
        recursive_grounding_result = parse_record_grounding_result(
            gpt_req.response.result,
            catalog=recursive_catalog_for(field_type.name),
        )
    except Exception as e:
        await record_response_parse_error(
            gpt_batch_request=gpt_req,
            error_message=str(e),
            timestamp=deferred_at,
            traceback_str=traceback.format_exc(),
        )
        logger.error(
            f"phrase_recursive_grounding_node.parse_batch_request_result: Error parsing phrase_recursive_grounding results for subject {subject_unique_id} from GPT response: {e}"
        )
        raise

    # Warn-and-drop, never raise: the embed path deletes incomplete descent
    # requests, wiping the error history a retry cap would count against, so a
    # raise here could re-dispatch forever. A thinned descent costs depth on
    # the dropped records, not their existence — their node keeps the parent's
    # groundings.
    recursive_grounding_result = hold_response_to_sent_record_ids(
        user_message=gpt_req.request.body.user_message(),
        response_by_record_id=recursive_grounding_result,
        where=f"{subject_unique_id}:{field_type.name} recursive grounding {descend_req_id}",
        on_missing="drop",
    )

    return get_tagging_results_from_record_groundings(recursive_grounding_result)


async def get_all_recursive_grounding_results(
    subject_unique_id: str,
    field_type: ConceptFieldType,
    chunk_bounds: str,
    extraction_bundle: ConceptExtractionRequestBundle,
    completed_in_vocab_grounding_req_map: dict[BatchRequestIDType, GPTBatchRequest],
    completed_screening_req_map: dict[BatchRequestIDType, GPTBatchRequest],
    completed_recursive_grounding_req_map: dict[
        BatchRequestIDType, GPTBatchRequest
    ],
    match_label_to_concept_map: CaseInsensitiveDict[Concept],
    timestamp: datetime,
) -> IterativeGroundingResult:
    """
    Initialize retval: RecursiveGroundingResult = {}
    Iterate each level in llm_phrase_recursive_tagging_reqs:
        Iterate each itr in the level:
            Create a RecursivelyTaggedPhrase from each itr
            using the seed tagging results for direct evidence
            Add to retval
    """
    retval: IterativeGroundingResult = {}

    seed_trs = await get_descent_seed_tagging_results(
        subject_unique_id=subject_unique_id,
        field_type=field_type,
        chunk_bounds=chunk_bounds,
        extraction_bundle=extraction_bundle,
        completed_in_vocab_grounding_req_map=completed_in_vocab_grounding_req_map,
        completed_screening_req_map=completed_screening_req_map,
        match_label_to_concept_map=match_label_to_concept_map,
        timestamp=timestamp,
    )
    if extraction_bundle.llm_phrase_recursive_tagging_reqs is None:
        raise ValueError(
            f"Cannot get recursive grounding results as llm_phrase_recursive_tagging_reqs is None."
        )

    for level, itr_list in extraction_bundle.llm_phrase_recursive_tagging_reqs.items():
        rt_phrases: set[IterativelyTaggedPhraseGroup] = set()
        for rt_req in itr_list:
            rtp = await get_itp_from_itr(
                subject_unique_id=subject_unique_id,
                field_type=field_type,
                chunk_bounds=chunk_bounds,
                bundle=extraction_bundle,
                it_req=rt_req,
                seed_trs=seed_trs,
                completed_recursive_grounding_req_map=completed_recursive_grounding_req_map,
                match_label_to_concept_map=match_label_to_concept_map,
                timestamp=timestamp,
            )
            rt_phrases.add(rtp)
        retval[level] = rt_phrases  # this will insert if rt_phrases empty too

    return retval


def get_descendable_concept(
    label: str,
    match_label_to_concept_map: CaseInsensitiveDict[Concept],  # DO NOT MUTATE
) -> Optional[Concept]:
    """The single descent gate: the concept a node can be descended INTO, or None.

    A label is descendable only when it resolves to a known concept that has
    children — a descent prompt offers the children as options, so a leaf
    concept renders an empty option list and the model could only decline
    every record. Every place that expects, awaits, creates, or parses a
    descent request must consult this same gate, or an ID becomes
    expected-but-never-created and the level walk deadlocks waiting for it.
    Node CREATION is not gated here: a leaf still gets its tagging-request node
    so its records and rules reach the phrase trail.

    Returns the concept rather than a bool so callers don't look it up twice.
    """
    concept = match_label_to_concept_map.get(label)
    if concept is None or not concept.children:
        return None
    return concept


def get_itr_descendable_concept(
    itr: IterativeTaggingRequest,
    match_label_to_concept_map: CaseInsensitiveDict[Concept],  # DO NOT MUTATE
) -> Optional[Concept]:
    """The label gate plus the node's own history: a node whose creation already
    recorded why descent must stop (false child) is never descended even when
    its name resolves to a descendable concept — a false child IS a real
    concept with children, just not under this parent."""
    if itr.stop_reason is not None:
        return None
    return get_descendable_concept(itr.name, match_label_to_concept_map)


def is_rtp_descend_worthy(
    itp_group: IterativelyTaggedPhraseGroup,
    match_label_to_concept_map: CaseInsensitiveDict[Concept],  # DO NOT MUTATE
) -> bool:
    return (
        itp_group.stop_reason is None
        and get_descendable_concept(itp_group.group_id, match_label_to_concept_map)
        is not None
        and bool(
            itp_group.direct_phrases_to_og_tag_w_rules
            or itp_group.iterative_phrases_to_og_tag_w_rules
        )
    )


def get_phrase_trails(
    lvl_by_lvl_iterative_grounding_results: IterativeGroundingResult,
) -> list[PhraseTrail]:
    """One trail per RECORD (the maps' keys are record ids in v2; the field
    names still say phrase because the model shapes carried over — the
    id→phrase join lives in the masked relationship stats)."""
    retval_dict: dict[str, PhraseTrail] = {}
    # record_id -> PhraseTrail

    for lvl, itp_groups in lvl_by_lvl_iterative_grounding_results.items():
        for itp_group in itp_groups:
            # break ITP Group into individual records and their tags
            for (
                d_phrase,
                og_tag_rules_map,
            ) in itp_group.direct_phrases_to_og_tag_w_rules.items():
                phrase_trail = retval_dict.get(
                    d_phrase, PhraseTrail(phrase=d_phrase, lvl_by_lvl_itps={})
                )
                phrase_trail.lvl_by_lvl_itps.setdefault(lvl, set())
                # Matched on (parent, group) like the models' identity: two
                # groups sharing a name under different parents are distinct
                # records, and folding them lost one parent's verdicts.
                matching_itp = next(
                    (
                        itp
                        for itp in phrase_trail.lvl_by_lvl_itps[lvl]
                        if itp.group_id == itp_group.group_id
                        and itp.parent_group_id == itp_group.parent_group_id
                    ),
                    IterativelyTaggedPhrase(
                        parent_group_id=itp_group.parent_group_id,
                        group_id=itp_group.group_id,
                        stop_reason=itp_group.stop_reason,
                        direct_og_tag_w_rules={},
                        iterative_og_tag_w_rules={},
                    ),
                )
                matching_itp.direct_og_tag_w_rules.update(og_tag_rules_map)
                phrase_trail.lvl_by_lvl_itps[lvl].add(matching_itp)
                retval_dict[d_phrase] = phrase_trail
            for (
                i_phrase,
                og_tag_rules_map,
            ) in itp_group.iterative_phrases_to_og_tag_w_rules.items():
                phrase_trail = retval_dict.get(
                    i_phrase, PhraseTrail(phrase=i_phrase, lvl_by_lvl_itps={})
                )
                phrase_trail.lvl_by_lvl_itps.setdefault(lvl, set())
                matching_itp = next(
                    (
                        itp
                        for itp in phrase_trail.lvl_by_lvl_itps[lvl]
                        if itp.group_id == itp_group.group_id
                        and itp.parent_group_id == itp_group.parent_group_id
                    ),
                    IterativelyTaggedPhrase(
                        parent_group_id=itp_group.parent_group_id,
                        group_id=itp_group.group_id,
                        stop_reason=itp_group.stop_reason,
                        direct_og_tag_w_rules={},
                        iterative_og_tag_w_rules={},
                    ),
                )
                matching_itp.iterative_og_tag_w_rules.update(og_tag_rules_map)
                phrase_trail.lvl_by_lvl_itps[lvl].add(matching_itp)
                retval_dict[i_phrase] = phrase_trail

    return list(retval_dict.values())


def get_deepest_concepts_and_oov(
    phrase_trail: PhraseTrail, match_label_to_concept_map: CaseInsensitiveDict[Concept]
) -> tuple[set[Concept], set[str]]:
    concepts: set[Concept] = set()
    oov: set[str] = set()
    for lvl, itps in phrase_trail.lvl_by_lvl_itps.items():
        for itp in itps:
            if itp.stop_reason is not None or is_sentinel_grounding_label(
                itp.group_id
            ):
                # Records that the descent stopped here, not a label that was
                # found. A false child leaves the parent tag in place — the
                # model named a concept that is not a child of the parent it
                # was asked under, so the parent stands. The label check is
                # the none-of-the-above tripwire: the descent trail is the one
                # result path screening never vets, so a v1-habit sentinel
                # echo must not persist as a discovered label here.
                continue

            concept = match_label_to_concept_map.get(itp.group_id)
            if concept:
                if itp.parent_group_id:
                    # replace the parent added on previous level
                    parent_concept = match_label_to_concept_map.get(itp.parent_group_id)
                    if parent_concept:
                        # NOTE: this only discards immediate parent, anything higher up stays
                        # which can happen when a record was tagged to grand dad, never descended
                        # but also directly tagged to grand child
                        # both of which will be captured in the trail
                        concepts.discard(parent_concept)
                    concepts.add(concept)
                else:
                    concepts.add(concept)
            else:
                oov.add(itp.group_id)
    return (concepts, oov)


async def get_itp_from_itr(
    subject_unique_id: str,
    field_type: ConceptFieldType,
    chunk_bounds: str,
    bundle: ConceptExtractionRequestBundle,
    it_req: IterativeTaggingRequest,
    seed_trs: list[TaggingResult],
    completed_recursive_grounding_req_map: dict[BatchRequestIDType, GPTBatchRequest],
    match_label_to_concept_map: CaseInsensitiveDict[Concept],  # DO NOT MUTATE
    timestamp: datetime,
) -> IterativelyTaggedPhraseGroup:
    if bundle.llm_phrase_recursive_tagging_reqs is None:
        raise ValueError(
            f"Cannot build node evidence for recursive grounding as llm_phrase_recursive_tagging_reqs is None."
        )

    logger.info(
        f"Creating IterativelyTaggedPhraseGroup for it_req: l{it_req.level}>{it_req.name} in chunk {chunk_bounds} for {subject_unique_id}:{field_type.name}"
    )

    itp: IterativelyTaggedPhraseGroup = IterativelyTaggedPhraseGroup(
        parent_group_id=it_req.parent_name,
        group_id=it_req.name,
        stop_reason=it_req.stop_reason,
        direct_phrases_to_og_tag_w_rules={},
        iterative_phrases_to_og_tag_w_rules={},
    )

    # A stopped node carries only what its own parent's response said about it
    # (the iterative map below). Seed tags matched here by NAME would be a
    # different event wearing the same label: a false child's direct evidence
    # belongs to the concept's real node, not to the record of the
    # misparenting.
    if it_req.stop_reason is None:
        for seed_tr in seed_trs:
            if (seed_tr.group_id == it_req.name) or (
                (concept := match_label_to_concept_map.get(seed_tr.group_id))
                and concept.name == it_req.name
            ):
                logger.info(
                    f"seed tr.group_tag:{seed_tr.group_id} matches rt_req.name:{it_req.name}, copying over record rules map."
                )
                for (
                    record_id,
                    applied_rules,
                ) in seed_tr.phrase_rules_map.items():
                    itp.direct_phrases_to_og_tag_w_rules[record_id] = {
                        seed_tr.group_id: applied_rules
                    }

    logger.info(
        f"Searching for parent_itr for it_req: l{it_req.level}>{it_req.name} in chunk {chunk_bounds} for {subject_unique_id}:{field_type.name}"
    )
    parent_itr = (
        next(
            (
                parent_itr
                for parent_itr in bundle.llm_phrase_recursive_tagging_reqs[
                    it_req.level - 1
                ]
                if parent_itr.is_parent_of(
                    child_tagging_req=it_req,
                )
            ),
            None,
        )
        if it_req.level != 1
        else None
    )
    if it_req.level != 1 and it_req.parent_descend_req_id and parent_itr is None:
        raise ValueError(
            f"Previous level parent of {it_req.name} could not be located."
        )

    if parent_itr:
        tagged_children_trs = await parse_recursive_grounding_batch_request_result(
            subject_unique_id=subject_unique_id,
            field_type=field_type,
            chunk_bounds=chunk_bounds,
            descend_req_id=parent_itr.descend_req_id,
            completed_request_map=completed_recursive_grounding_req_map,
            deferred_at=timestamp,
        )
        for child_tr in tagged_children_trs:
            if (child_tr.group_id == it_req.name) or (
                (concept := match_label_to_concept_map.get(child_tr.group_id))
                and concept.name == it_req.name
            ):
                logger.info(
                    f"child_tr.group_tag:{child_tr.group_id} matches rt_req.name:{it_req.name}, copying over record rules map."
                )
                for record_id, applied_rules in child_tr.phrase_rules_map.items():
                    itp.iterative_phrases_to_og_tag_w_rules[record_id] = {
                        child_tr.group_id: applied_rules
                    }

    return itp


def descent_evidence_record_ids(
    itp_group: IterativelyTaggedPhraseGroup,
) -> set[str]:
    """The records that evidence a descent node — what its request will carry."""
    return set(itp_group.direct_phrases_to_og_tag_w_rules) | set(
        itp_group.iterative_phrases_to_og_tag_w_rules
    )


def descent_record_payloads(
    record_ids: set[str],
    masked: MaskedLLMPhraseRelationshipResults,
) -> dict[str, dict[str, Any]]:
    """The id → record payload a descent request renders. Sorted for render
    stability; a record id with no masked relationship entry is a pipeline bug
    — every id in the descent tree came out of a stage that derived it from
    the relationship results."""
    missing = record_ids - set(masked)
    if missing:
        raise ValueError(
            f"descent_record_payloads: no relationship record for id(s) "
            f"{sorted(missing)}"
        )
    return {
        record_id: masked[record_id].record.model_dump()
        for record_id in sorted(record_ids)
    }


async def create_missing_phrase_recursive_grounding_requests(
    # used for logging and debugging
    subject_unique_id: str,
    subject_name: str,
    field_type: ConceptFieldType,
    # context
    chunked_request_map: ConceptExtractionRequestMap,
    missing_phrase_recursive_grounding_req_ids: set[  # will all belong to the same level
        BatchRequestIDType
    ],  # spread across all chunks, can be partially covering a level in req tree
    llm_phrase_relationship_gpt_request_map: dict[BatchRequestIDType, GPTBatchRequest],
    completed_in_vocab_grounding_req_map: dict[BatchRequestIDType, GPTBatchRequest],
    completed_screening_req_map: dict[BatchRequestIDType, GPTBatchRequest],
    completed_recursive_grounding_req_map: dict[BatchRequestIDType, GPTBatchRequest],
    match_label_to_concept_map: CaseInsensitiveDict[Concept],  # DO NOT MUTATE
    phrase_recursive_grounding_prompt: Prompt,
    # req metadata
    timestamp: datetime,
    llm_model: LLM_Model,
    model_params: GPTModelParams,
    eager: bool,
    # defaults
    BATCH_SIZE=100,
) -> list[GPTBatchRequest]:
    if not missing_phrase_recursive_grounding_req_ids:
        raise ValueError(f"missing_phrase_recursive_grounding_req_ids is empty.")

    logger.info(
        f"create_missing_recursive_grounding_requests: Generating GPTBatchRequests for {subject_unique_id}:{field_type}"
    )

    batch_requests: list[GPTBatchRequest] = []

    chunk_items: list[
        tuple[str, dict[str, dict[str, Any]], IterativeTaggingRequest]
    ] = []
    for chunk_bounds, bundle in chunked_request_map.items():
        logger.info(
            f"Processing chunk {chunk_bounds} for {subject_unique_id}:{field_type} to create missing recursive grounding requests. "
        )
        if not bundle.llm_phrase_recursive_tagging_reqs:
            raise ValueError(
                f"Cannot create batch requests for recursive grounding as llm_phrase_recursive_tagging_reqs is empty."
            )
        masked_relationship_results = await get_masked_phrase_relationship_result(
            subject_unique_id=subject_unique_id,
            field_type=field_type,
            chunk_bounds=chunk_bounds,
            extraction_bundle=bundle,
            completed_request_map=llm_phrase_relationship_gpt_request_map,
            timestamp=timestamp,
        )

        pending_level = max(
            bundle.llm_phrase_recursive_tagging_reqs.keys(),
        )
        logger.info(f"Found pending level:{pending_level}")
        seed_trs = await get_descent_seed_tagging_results(
            subject_unique_id=subject_unique_id,
            field_type=field_type,
            chunk_bounds=chunk_bounds,
            extraction_bundle=bundle,
            completed_in_vocab_grounding_req_map=completed_in_vocab_grounding_req_map,
            completed_screening_req_map=completed_screening_req_map,
            match_label_to_concept_map=match_label_to_concept_map,
            timestamp=timestamp,
        )

        pending_tagging_req_w_concept_pair = [
            (req, concept)
            for req in bundle.llm_phrase_recursive_tagging_reqs[pending_level]
            # Same gate as get_embedded_request_ids: a non-descendable node
            # (out-of-vocab proposal, false-child, or leaf) never gets a
            # request created, so it is never among the missing IDs —
            # including it here would trip the not-passed-as-missing check
            # below.
            if (
                concept := get_itr_descendable_concept(req, match_label_to_concept_map)
            )
            is not None
        ]
        tmp_set: set[Concept] = set()
        for _req, c in pending_tagging_req_w_concept_pair:
            if c in tmp_set:
                raise ValueError(
                    f"Concept: {c} repeats across tagging requests at the same level:{pending_level}"
                )
            else:
                assert c
                tmp_set.add(c)

        logger.info(
            f"Pending tagging requests for chunk {chunk_bounds} in {subject_unique_id}:{field_type} \nat level {pending_level}: {[req.name for req, _c in pending_tagging_req_w_concept_pair]}"
        )

        for pending_tagging_req, concept_obj in pending_tagging_req_w_concept_pair:
            logger.info(
                f"Processing pending_tagging_req {pending_tagging_req.name} in chunk {chunk_bounds} for {subject_unique_id}:{field_type} at level {pending_level}"
            )
            if (
                pending_tagging_req.descend_req_id
                not in missing_phrase_recursive_grounding_req_ids
            ):
                raise ValueError(
                    f"descend_req_id:{pending_tagging_req.descend_req_id} from last level was not passed as missing"
                )
            elif not concept_obj:
                logger.info(
                    f"Skipping pending_tagging_req {pending_tagging_req.name} as concept_obj is None"
                )

            itp = await get_itp_from_itr(
                subject_unique_id=subject_unique_id,
                field_type=field_type,
                chunk_bounds=chunk_bounds,
                bundle=bundle,
                it_req=pending_tagging_req,
                seed_trs=seed_trs,
                completed_recursive_grounding_req_map=completed_recursive_grounding_req_map,
                match_label_to_concept_map=match_label_to_concept_map,
                timestamp=timestamp,
            )

            if not is_rtp_descend_worthy(
                itp_group=itp, match_label_to_concept_map=match_label_to_concept_map
            ):
                continue

            record_payloads = descent_record_payloads(
                descent_evidence_record_ids(itp), masked_relationship_results
            )
            logger.info(
                f"descent record payloads for {pending_tagging_req.name}: {sorted(record_payloads)}"
            )
            chunk_items.append(
                (
                    chunk_bounds,
                    record_payloads,
                    pending_tagging_req,
                )
            )

    # Process chunks in batches to yield control periodically
    for i in range(0, len(chunk_items), BATCH_SIZE):
        batch = chunk_items[i : i + BATCH_SIZE]

        # Process current batch
        for chunk_bounds, record_payloads, pending_itr in batch:
            tagged_concept_obj = match_label_to_concept_map.get(pending_itr.name)
            if not tagged_concept_obj:
                raise ValueError(
                    f"Cannot create batch requests for recursive grounding for {subject_unique_id}:{subject_name}:{chunk_bounds} "
                    f"as tagged_concept:{pending_itr.name} is unrecognized."
                )

            llm_phrase_grounding_batch_request = create_deferred_phrase_recursive_grounding_gpt_request(
                # used for logging and debugging
                deferred_at=timestamp,
                subject_unique_id=subject_unique_id,
                field_type=field_type,
                # context variables
                phrase_recursive_grounding_prompt=phrase_recursive_grounding_prompt,
                llm_phrase_recursive_grounding_request_id=pending_itr.descend_req_id,
                parent_concept=tagged_concept_obj,
                child_concepts={
                    match_label_to_concept_map[child_name]
                    for child_name in tagged_concept_obj.children
                },
                record_payloads=record_payloads,
                # model info
                eager=eager,
                gpt_model=llm_model,
                model_params=model_params,
            )
            batch_requests.append(llm_phrase_grounding_batch_request)

        # Yield control to event loop after each batch
        await asyncio.sleep(0)

        if (i + BATCH_SIZE) % 500 == 0:
            logger.info(
                f"Created {min(i + BATCH_SIZE, len(chunk_items))}/{len(chunk_items)} "
                f"gpt request for {subject_unique_id}:{field_type}"
            )

    return batch_requests


def create_deferred_phrase_recursive_grounding_gpt_request(
    deferred_at: datetime,
    subject_unique_id: str,
    field_type: ConceptFieldType,
    llm_phrase_recursive_grounding_request_id: str,
    phrase_recursive_grounding_prompt: Prompt,
    # context
    parent_concept: Concept,
    child_concepts: set[Concept],
    record_payloads: dict[str, dict[str, Any]],
    # model info
    gpt_model: LLM_Model,
    eager: bool,
    model_params: GPTModelParams,
) -> GPTBatchRequest:
    logger.info(
        f"create_deferred_phrase_recursive_grounding_gpt_request: Generating GPTBatchRequest for {llm_phrase_recursive_grounding_request_id}"
    )
    parent_placeholder_stem, child_placeholder_stem = (
        field_type.recursive_grounding_placeholders
    )

    # The parent and its child options ride in the PROMPT (per-request
    # placeholder replacement, as in v1); the context carries the record
    # blocks alone.
    refactored_text = phrase_recursive_grounding_prompt.text.replace(
        parent_placeholder_stem, parent_concept.name
    ).replace(
        child_placeholder_stem,
        json.dumps(
            list(child_concepts),
            cls=ConceptJSONEncoder,
        ),
    )
    context = render_record_blocks(record_payloads)

    gpt_batch_request = create_base_gpt_batch_request(
        deferred_at=deferred_at,
        subject_unique_id=subject_unique_id,
        custom_id=llm_phrase_recursive_grounding_request_id,
        context=context,
        prompt_text=refactored_text,
        gpt_model=gpt_model,
        model_params=model_params.with_response_format(
            response_format_for(recursive_catalog_for(field_type.name))
        ),
        batch_id="Eager" if eager else None,
    )
    return gpt_batch_request
