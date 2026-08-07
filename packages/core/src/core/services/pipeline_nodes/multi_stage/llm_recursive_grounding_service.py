from __future__ import annotations

import asyncio
import json
import logging
import traceback
from datetime import datetime
from requests.structures import CaseInsensitiveDict

from pydantic import ValidationError

from llm_providers.db_models.gpt_batch_request import (
    GPTBatchRequest,
)
from core.models.extraction_schemas.grounding import (
    PhraseGroundingResponse,
    PhraseToTagAndReasonMap,
)
from core.models.extraction_schemas.iterative_tagging import (
    IterativelyTaggedPhraseGroup,
    IterativeGroundingResult,
    IterativelyTaggedPhrase,
    PhraseTrail,
)
from core.models.extraction_schemas.relationship import (
    LLMPhraseRelationshipResults,
)
from core.models.extraction_schemas.response_format_util import (
    build_gpt_response_format,
)
from llm_providers.models.llm_model import (
    LLM_Model,
    NO_MODEL,
)
from llm_providers.models.file_objects.prompt import Prompt
from llm_providers.models.open_ai.gpt_batch_response_blob import (
    ChatCompletionChoiceMessage,
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
    get_dummy_gpt_batch_response,
)
from core.services.pipeline_nodes.multi_stage.llm_phrase_relationship_node_service import (
    get_phrase_relationship_result,
)
from core.services.pipeline_nodes.multi_stage.llm_initial_grounding_service import (
    get_tagged_results_from_initial_grounding,
)

from core.utils.request_custom_id_util import (
    get_name_from_recursive_grounding_request_custom_id,
)

logger = logging.getLogger(__name__)


LLM_PHRASE_RECURSIVE_GROUNDING_RESPONSE_SCHEMA = build_gpt_response_format(
    PhraseGroundingResponse, name="phrase_recursive_grounding_result"
)


def parse_llm_phrase_recursive_grounding_result(
    subject_unique_id: str,
    field_type: ConceptFieldType,
    gpt_req: GPTBatchRequest,
) -> PhraseToTagAndReasonMap:
    if not gpt_req.response:
        raise ValueError(
            f"phrase_recursive_grounding_node.parse_batch_request_result: GPTBatchRequest for phrase_recursive_grounding request ID {gpt_req.request.custom_id} has no response_blob in {subject_unique_id}:{field_type.name}"
        )
    elif not gpt_req.response.result:
        raise ValueError(
            f"phrase_recursive_grounding_node.parse_batch_request_result: GPTBatchRequest for phrase_recursive_grounding request ID {gpt_req.request.custom_id} has no result in {subject_unique_id}:{field_type.name}"
        )

    try:
        parsed = PhraseGroundingResponse.model_validate_json(gpt_req.response.result)
    except ValidationError as e:
        raise ValueError(
            f"parse_llm_phrase_recursive_grounding_result: Invalid response from GPT:{gpt_req.response.result}"
        ) from e

    raw_llm_recursive_grounding_result: PhraseToTagAndReasonMap = {}
    for entry in parsed.groundings:
        if entry.phrase in raw_llm_recursive_grounding_result:
            raise ValueError(
                f"parse_llm_phrase_recursive_grounding_result: Duplicate phrase {entry.phrase!r} in groundings response"
            )
        raw_llm_recursive_grounding_result[entry.phrase] = {
            tag.tag: tag.reason for tag in entry.tags
        }

    logger.debug(
        f"raw_llm_recursive_grounding_result:{raw_llm_recursive_grounding_result}"
    )

    return raw_llm_recursive_grounding_result


async def parse_recursive_grounding_batch_request_result(
    subject_unique_id: str,
    field_type: ConceptFieldType,
    chunk_bounds: str,
    descend_req_id: BatchRequestIDType,
    completed_request_map: dict[BatchRequestIDType, GPTBatchRequest],
    # expected_tagged_concept_level: int,
    # match_label_to_concept_map: CaseInsensitiveDict[Concept],
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

    try:
        recursive_grounding_result = parse_llm_phrase_recursive_grounding_result(
            subject_unique_id=subject_unique_id,
            field_type=field_type,
            gpt_req=gpt_req,
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

    return get_tagging_results_from_recursive_grounding_results(
        # expected_tagged_concept_level=expected_tagged_concept_level,
        grounding_result=recursive_grounding_result,
    )


async def get_all_recursive_grounding_results(
    subject_unique_id: str,
    field_type: ConceptFieldType,
    chunk_bounds: str,
    extraction_bundle: ConceptExtractionRequestBundle,
    completed_initial_grounding_req_map: dict[BatchRequestIDType, GPTBatchRequest],
    completed_recursive_grounding_req_map: dict[BatchRequestIDType, GPTBatchRequest],
    match_label_to_concept_map: CaseInsensitiveDict[Concept],
    timestamp: datetime,
) -> IterativeGroundingResult:
    """
    Initialize retval: RecursiveGroundingResult = {}
    Iterate each level in llm_phrase_recursive_tagging_reqs:
        Iterate each itr in the level:
            Create a RecursivelyTaggedPhrase from each itr
            using initially tagged dtcs from current level
            Add to retval
    """
    retval: IterativeGroundingResult = {}

    all_initially_tagged_dtrs = await get_tagged_results_from_initial_grounding(
        subject_unique_id=subject_unique_id,
        field_type=field_type,
        chunk_bounds=chunk_bounds,
        extraction_bundle=extraction_bundle,
        completed_request_map=completed_initial_grounding_req_map,
        timestamp=timestamp,
    )
    if not extraction_bundle.llm_phrase_recursive_tagging_reqs:
        raise ValueError(
            f"Cannot get recursive grounding results as llm_phrase_recursive_tagging_reqs is empty."
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
                initially_tagged_trs=all_initially_tagged_dtrs,
                completed_recursive_grounding_req_map=completed_recursive_grounding_req_map,
                match_label_to_concept_map=match_label_to_concept_map,
                timestamp=timestamp,
            )
            rt_phrases.add(rtp)
        retval[level] = rt_phrases  # this will insert if rt_phrases empty too

    return retval


def get_pruned_itp_group(
    itp_group: IterativelyTaggedPhraseGroup,
) -> IterativelyTaggedPhraseGroup:
    """
    LOGIC

    To pass
    1. itp_group.tag must be in vocab
    2. some phrases still remain after removing nested phrases that exactly match the tag

    Return the pruned version or None
    """

    for phrase, tag_w_reason in list(
        itp_group.direct_phrases_to_og_tag_w_reason.items()
    ):
        for tag in tag_w_reason:
            if phrase in [
                tag,
                itp_group.group_id,
            ]:  # tag and rtp.tag won't be same if phrase was tagged to concept with altLabel, in which case rtp.tag is concept name and tag is altLabel
                itp_group.direct_phrases_to_og_tag_w_reason.pop(phrase)

    for phrase, tag_w_reason in list(
        itp_group.iterative_phrases_to_og_tag_w_reason.items()
    ):
        for tag in tag_w_reason:
            if phrase in [
                tag,
                itp_group.group_id,
            ]:  # tag and rtp.tag won't be same if phrase was tagged to concept with altLabel, in which case rtp.tag is concept name and tag is altLabel
                itp_group.iterative_phrases_to_og_tag_w_reason.pop(phrase)

    return itp_group


def is_rtp_descend_worthy(
    itp_group: IterativelyTaggedPhraseGroup,
    match_label_to_concept_map: CaseInsensitiveDict[Concept],  # DO NOT MUTATE
) -> bool:
    return itp_group.group_id in match_label_to_concept_map and bool(
        itp_group.direct_phrases_to_og_tag_w_reason
        or itp_group.iterative_phrases_to_og_tag_w_reason
    )


def get_phrase_trails(
    lvl_by_lvl_iterative_grounding_results: IterativeGroundingResult,
    # match_label_to_concept_map: CaseInsensitiveDict[Concept],  # DO NOT MUTATE
) -> list[PhraseTrail]:
    retval_dict: dict[str, PhraseTrail] = {}
    # phrase -> PhraseTrail

    for lvl, itp_groups in lvl_by_lvl_iterative_grounding_results.items():
        for itp_group in itp_groups:
            # break ITP Group into individual phrases and their tags
            for (
                d_phrase,
                og_tag_reason_map,
            ) in itp_group.direct_phrases_to_og_tag_w_reason.items():
                phrase_trail = retval_dict.get(
                    d_phrase, PhraseTrail(phrase=d_phrase, lvl_by_lvl_itps={})
                )
                phrase_trail.lvl_by_lvl_itps.setdefault(lvl, set())
                matching_itp = next(
                    (
                        itp
                        for itp in phrase_trail.lvl_by_lvl_itps[lvl]
                        if itp.group_id == itp_group.group_id
                    ),
                    IterativelyTaggedPhrase(
                        parent_group_id=itp_group.parent_group_id,
                        group_id=itp_group.group_id,
                        direct_og_tag_w_reason={},
                        iterative_og_tag_w_reason={},
                    ),
                )
                matching_itp.direct_og_tag_w_reason.update(og_tag_reason_map)
                phrase_trail.lvl_by_lvl_itps[lvl].add(matching_itp)
                retval_dict[d_phrase] = phrase_trail
            for (
                i_phrase,
                og_tag_reason_map,
            ) in itp_group.iterative_phrases_to_og_tag_w_reason.items():
                phrase_trail = retval_dict.get(
                    i_phrase, PhraseTrail(phrase=i_phrase, lvl_by_lvl_itps={})
                )
                phrase_trail.lvl_by_lvl_itps.setdefault(lvl, set())
                matching_itp = next(
                    (
                        itp
                        for itp in phrase_trail.lvl_by_lvl_itps[lvl]
                        if itp.group_id == itp_group.group_id
                    ),
                    IterativelyTaggedPhrase(
                        parent_group_id=itp_group.parent_group_id,
                        group_id=itp_group.group_id,
                        direct_og_tag_w_reason={},
                        iterative_og_tag_w_reason={},
                    ),
                )
                matching_itp.iterative_og_tag_w_reason.update(og_tag_reason_map)
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
            concept = match_label_to_concept_map.get(itp.group_id)
            if concept:
                if itp.parent_group_id:
                    # replace the parent added on previous level
                    parent_concept = match_label_to_concept_map.get(itp.parent_group_id)
                    if parent_concept:
                        # NOTE: this only discards immediate parent, anything higher up stays
                        # which can happen when a phrase was tagged to grand dad, never descended
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
    # completed_initial_grounding_req_map: dict[BatchRequestIDType, GPTBatchRequest],
    initially_tagged_trs: list[TaggingResult],
    completed_recursive_grounding_req_map: dict[BatchRequestIDType, GPTBatchRequest],
    match_label_to_concept_map: CaseInsensitiveDict[Concept],  # DO NOT MUTATE
    timestamp: datetime,
) -> IterativelyTaggedPhraseGroup:
    if not bundle.llm_phrase_recursive_tagging_reqs:
        raise ValueError(
            f"Cannot create batch requests for recursive grounding as llm_phrase_recursive_grounding_root_req_nodes is empty."
        )

    logger.info(
        f"Creating IterativelyTaggedPhraseGroup for it_req: l{it_req.level}>{it_req.name} in chunk {chunk_bounds} for {subject_unique_id}:{field_type.name}"
    )

    itp: IterativelyTaggedPhraseGroup = IterativelyTaggedPhraseGroup(
        parent_group_id=(
            get_name_from_recursive_grounding_request_custom_id(
                it_req.parent_descend_req_id
            )
            if it_req.parent_descend_req_id
            else None
        ),
        group_id=it_req.name,
        direct_phrases_to_og_tag_w_reason={},
        iterative_phrases_to_og_tag_w_reason={},
    )

    # directly_tagged_phrases: PhraseToTagAndReasonMap = {}
    for initial_tr in initially_tagged_trs:
        if (initial_tr.group_id == it_req.name) or (
            (concept := match_label_to_concept_map.get(initial_tr.group_id))
            and concept.name == it_req.name
        ):
            logger.info(
                f"initially tagged tr.group_tag:{initial_tr.group_id} matches rt_req.name:{it_req.name}, copying over phrase reason map."
            )
            for (
                phrase,
                reason,
            ) in initial_tr.phrase_reason_map.items():
                itp.direct_phrases_to_og_tag_w_reason[phrase] = {
                    initial_tr.group_id: reason
                }

    # iteratively_tagged: PhraseToTagAndReasonMap = {}
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
                    f"child_tr.group_tag:{child_tr.group_id} matches rt_req.name:{it_req.name}, copying over phrase reason map."
                )
                for phrase, reason in child_tr.phrase_reason_map.items():
                    itp.iterative_phrases_to_og_tag_w_reason[phrase] = {
                        child_tr.group_id: reason
                    }

    return itp


def get_tagging_results_from_recursive_grounding_results(
    grounding_result: PhraseToTagAndReasonMap,
) -> list[TaggingResult]:
    tag_to_tr_map: dict[str, TaggingResult] = {}
    for phrase, tag_reason_map in grounding_result.items():
        for tag, reason in tag_reason_map.items():
            tr = tag_to_tr_map.setdefault(
                tag, TaggingResult(group_id=tag, phrase_reason_map={})
            )
            tr.phrase_reason_map[phrase] = reason
    return list(tag_to_tr_map.values())


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
    completed_initial_grounding_req_map: dict[BatchRequestIDType, GPTBatchRequest],
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
        tuple[str, LLMPhraseRelationshipResults, IterativeTaggingRequest]
    ] = []
    for chunk_bounds, bundle in chunked_request_map.items():
        logger.info(
            f"Processing chunk {chunk_bounds} for {subject_unique_id}:{field_type} to create missing recursive grounding requests. "
        )
        if not bundle.llm_phrase_recursive_tagging_reqs:
            raise ValueError(
                f"Cannot create batch requests for recursive grounding as llm_phrase_recursive_grounding_root_req_nodes is empty."
            )
        llm_phrase_relationship_results = await get_phrase_relationship_result(
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
        initially_tagged_trs = await get_tagged_results_from_initial_grounding(
            subject_unique_id=subject_unique_id,
            field_type=field_type,
            chunk_bounds=chunk_bounds,
            extraction_bundle=bundle,
            completed_request_map=completed_initial_grounding_req_map,
            timestamp=timestamp,
        )

        pending_tagging_req_w_concept_pair = [
            (req, match_label_to_concept_map.get(req.name))
            for req in bundle.llm_phrase_recursive_tagging_reqs[pending_level]
            if req.name
            in match_label_to_concept_map  # otherwise not descend worthy, because embedding may contain a last executed req which resulted in non descend worthy child
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
                initially_tagged_trs=initially_tagged_trs,
                completed_recursive_grounding_req_map=completed_recursive_grounding_req_map,
                match_label_to_concept_map=match_label_to_concept_map,
                timestamp=timestamp,
            )

            logger.info(f"itp before pruning: {itp}")
            # itp = get_pruned_itp_group(itp)
            logger.info(f"itp after pruning: {itp}")

            if not is_rtp_descend_worthy(
                itp_group=itp, match_label_to_concept_map=match_label_to_concept_map
            ):
                continue

            phrases_referenced_in_node = set(
                itp.direct_phrases_to_og_tag_w_reason.keys()
            ) | set(itp.iterative_phrases_to_og_tag_w_reason.keys())
            phrases_referenced_in_node_w_summary = {
                phrase: summary
                for phrase, summary in llm_phrase_relationship_results.items()
                if phrase in phrases_referenced_in_node
            }
            logger.info(
                f"phrases_referenced_in_node_w_summary:{phrases_referenced_in_node_w_summary}"
            )
            chunk_items.append(
                (
                    chunk_bounds,
                    phrases_referenced_in_node_w_summary,
                    pending_tagging_req,
                )
            )

    # Process chunks in batches to yield control periodically
    for i in range(0, len(chunk_items), BATCH_SIZE):
        batch = chunk_items[i : i + BATCH_SIZE]

        # Process current batch
        for chunk_bounds, phrases_w_summary, pending_itr in batch:
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
                subject_name=subject_name,
                phrase_recursive_grounding_prompt=phrase_recursive_grounding_prompt,
                llm_phrase_recursive_grounding_request_id=pending_itr.descend_req_id,
                parent_concept=tagged_concept_obj,
                child_concepts={
                    match_label_to_concept_map[child_name]
                    for child_name in tagged_concept_obj.children
                },
                verified_phrases_w_og_summary=phrases_w_summary,
                # model info
                eager=eager,
                gpt_model=llm_model,
                model_params=model_params,
            )
            new_batch_request = llm_phrase_grounding_batch_request

            batch_requests.append(new_batch_request)

        # Yield control to event loop after each batch
        await asyncio.sleep(0)

        if (i + BATCH_SIZE) % 500 == 0:
            logger.info(
                f"Created {min(i + BATCH_SIZE, len(chunk_items))}/{len(chunk_items)} "
                f"gpt request for {subject_unique_id}:{field_type}"
            )

    return batch_requests


# TODO: needs fixing if used, not used right now
def _create_dummy_completed_phrase_recursive_grounding_batch_request(
    deferred_at: datetime,
    subject_unique_id: str,
    llm_phrase_recursive_grounding_request_id: BatchRequestIDType,
    model_params: GPTModelParams,
    eager: bool,
) -> GPTBatchRequest:
    if llm_phrase_recursive_grounding_request_id is None:
        raise ValueError(
            "_create_dummy_completed_phrase_recursive_grounding_batch_request: llm_phrase_recursive_grounding_request_id is None"
        )

    base_gpt_batch_request = create_base_gpt_batch_request(
        deferred_at=deferred_at,
        subject_unique_id=subject_unique_id,
        custom_id=llm_phrase_recursive_grounding_request_id,
        context="No phrase recursive grounding needed - nothing was tagged in initial grounding.",
        prompt_text="No phrase recursive grounding needed - nothing was tagged in initial grounding.",
        gpt_model=NO_MODEL,
        model_params=model_params,
        batch_id="Eager" if eager else "dummy_phrase_relationship_batch_id",
    )

    base_gpt_batch_request.response = get_dummy_gpt_batch_response(
        deferred_at=deferred_at,
        request_custom_id=llm_phrase_recursive_grounding_request_id,
        dummy_chat_completion_id="dummy_completion_id",
        chat_completion_choice_message=ChatCompletionChoiceMessage(
            role="assistant", content='{"groundings": []}'
        ),
    )

    return base_gpt_batch_request


def create_deferred_phrase_recursive_grounding_gpt_request(
    deferred_at: datetime,
    subject_unique_id: str,
    field_type: ConceptFieldType,
    llm_phrase_recursive_grounding_request_id: str,
    phrase_recursive_grounding_prompt: Prompt,
    # context
    subject_name: str,
    parent_concept: Concept,
    child_concepts: set[Concept],
    verified_phrases_w_og_summary: LLMPhraseRelationshipResults,
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

    refactored_text = phrase_recursive_grounding_prompt.text.replace(
        parent_placeholder_stem, parent_concept.name
    ).replace(
        child_placeholder_stem,
        json.dumps(
            list(child_concepts),
            cls=ConceptJSONEncoder,
        ),
    )
    context = (
        # f"Manufacturer name: {subject_name}\n\n "
        f"extracted phrases:\n{list(verified_phrases_w_og_summary.keys())}\n "
        f"extracted phrases with their relationship summaries:\n{json.dumps(verified_phrases_w_og_summary)}"
    )

    gpt_batch_request = create_base_gpt_batch_request(
        deferred_at=deferred_at,
        subject_unique_id=subject_unique_id,
        custom_id=llm_phrase_recursive_grounding_request_id,
        context=context,
        prompt_text=refactored_text,
        gpt_model=gpt_model,
        model_params=model_params.with_response_format(
            LLM_PHRASE_RECURSIVE_GROUNDING_RESPONSE_SCHEMA
        ),
        batch_id="Eager" if eager else None,
    )
    return gpt_batch_request
