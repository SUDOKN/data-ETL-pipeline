from __future__ import annotations

import asyncio
import json
import logging
import traceback
from datetime import datetime
from requests.structures import CaseInsensitiveDict

from core.models.db.gpt_batch_request import GPTBatchRequest
from core.models.field_types import (
    PhraseToTagAndReasonMap,
    LLMPhraseRelationshipResults,
)
from core.models.llm_model import LLM_Model, NO_MODEL
from core.models.file_objects.prompt import Prompt
from core.models.batch_request_objects.gpt_batch_response_blob import (
    ChatCompletionChoiceMessage,
)
from core.models.deferred_extraction.deferred_concept_extraction import (
    ConceptExtractionRequestMap,
    RecursiveTaggingRequest,
    TaggedResult,
    TaggedConceptResult,
)
from data_etl_app.models.skos_concept import Concept, ConceptJSONEncoder
from data_etl_app.models.types_and_enums import (
    ConceptTypeEnum,
    LLMExtractedFieldTypeEnum,
)
from open_ai_key_app.models.gpt_model_params import GPTModelParams
from open_ai_key_app.models.field_types import GPTBatchRequestCustomID

from core.services.gpt_batch_request_writes import record_response_parse_error
from core.services.gpt_batch_request_service import (
    create_base_gpt_batch_request,
    get_dummy_gpt_batch_response,
)
from data_etl_app.services.extraction.deferred_llm_phrase_relationship_node_service import (
    parse_batch_request_result as parse_phrase_relationhip_batch_req_result,
)
from data_etl_app.services.extraction.deferred_llm_initial_grounding_service import (
    flip_tag_to_phrase_reason_map,
    parse_initial_grounding_batch_request_result,
    get_descend_worthy_directly_tagged_concept_results,
)

logger = logging.getLogger(__name__)


def parse_llm_phrase_recursive_grounding_result(
    mfg_etld1: str,
    field_type: LLMExtractedFieldTypeEnum,
    gpt_req: GPTBatchRequest,
) -> PhraseToTagAndReasonMap:
    if not gpt_req.response:
        raise ValueError(
            f"phrase_recursive_grounding_node.parse_batch_request_result: GPTBatchRequest for phrase_recursive_grounding request ID {gpt_req.request.custom_id} has no response_blob in {mfg_etld1}:{field_type.name}"
        )
    elif not gpt_req.response.result:
        raise ValueError(
            f"phrase_recursive_grounding_node.parse_batch_request_result: GPTBatchRequest for phrase_recursive_grounding request ID {gpt_req.request.custom_id} has no result in {mfg_etld1}:{field_type.name}"
        )

    try:
        json_response_string = gpt_req.response.result.replace("```", "").replace(
            "json", ""
        )
        raw_llm_recursive_grounding_result: dict[str, dict[str, str]] = json.loads(
            json_response_string
        )
        logger.debug(
            f"raw_llm_recursive_grounding_result:{json.dumps(raw_llm_recursive_grounding_result, indent=2)}"
        )
    except json.JSONDecodeError as e:
        raise ValueError(
            f"parse_llm_phrase_recursive_grounding_result: Invalid response from GPT:{json_response_string}"
        ) from e

    if not isinstance(raw_llm_recursive_grounding_result, dict):
        raise ValueError(
            "parse_llm_phrase_recursive_grounding_result: Expected raw_llm_recursive_grounding_result to be a dictionary"
        )

    logger.debug(
        f"raw_llm_recursive_grounding_result:{raw_llm_recursive_grounding_result}"
    )

    return raw_llm_recursive_grounding_result


async def parse_recursive_grounding_batch_request_result(
    mfg_etld1: str,
    field_type: LLMExtractedFieldTypeEnum,
    chunk_bounds: str,
    descend_req_id: GPTBatchRequestCustomID,
    completed_request_map: dict[GPTBatchRequestCustomID, GPTBatchRequest],
    # expected_tagged_concept_level: int,
    # match_label_to_concept_map: CaseInsensitiveDict[Concept],
    deferred_at: datetime,
) -> list[TaggedResult]:
    if not descend_req_id:
        raise ValueError(
            f"phrase_recursive_grounding_node.parse_batch_request_result: phrase_recursive_grounding_request_id is None for chunk bounds {chunk_bounds} in {mfg_etld1}:{field_type.name}"
        )
    gpt_req = completed_request_map.get(descend_req_id)
    if not gpt_req:
        raise ValueError(
            f"phrase_recursive_grounding_node.parse_batch_request_result: Missing GPTBatchRequest for phrase_recursive_grounding request ID {descend_req_id} in {mfg_etld1}:{field_type.name}"
        )

    try:
        recursive_grounding_result = parse_llm_phrase_recursive_grounding_result(
            mfg_etld1=mfg_etld1,
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
            f"phrase_recursive_grounding_node.parse_batch_request_result: Error parsing phrase_recursive_grounding results for manufacturer {mfg_etld1} from GPT response: {e}"
        )
        raise

    return get_tagged_results_from_recursive_grounding_results(
        # expected_tagged_concept_level=expected_tagged_concept_level,
        grounding_result=recursive_grounding_result,
    )


def get_tagged_results_from_recursive_grounding_results(
    grounding_result: PhraseToTagAndReasonMap,
) -> list[TaggedResult]:
    tag_to_tr_map: dict[str, TaggedResult] = {}
    for phrase, tag_reason_map in grounding_result.items():
        for tag, reason in tag_reason_map.items():
            tr = tag_to_tr_map.get(tag, TaggedResult(tag=tag, phrase_reason_map={}))
            tr.phrase_reason_map[phrase] = reason
    return list(tag_to_tr_map.values())


async def create_missing_phrase_recursive_grounding_requests(
    # used for logging and debugging
    mfg_etld1: str,
    mfg_name: str,
    field_type: ConceptTypeEnum,
    # context
    chunked_request_map: ConceptExtractionRequestMap,
    missing_phrase_recursive_grounding_req_ids: set[  # will all belong to the same level
        GPTBatchRequestCustomID
    ],  # spread across all chunks, can be partially covering a level in req tree
    llm_phrase_relationship_gpt_request_map: dict[
        GPTBatchRequestCustomID, GPTBatchRequest
    ],
    completed_initial_grounding_req_map: dict[GPTBatchRequestCustomID, GPTBatchRequest],
    completed_recursive_grounding_req_map: dict[
        GPTBatchRequestCustomID, GPTBatchRequest
    ],
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

    logger.info(
        f"create_missing_recursive_grounding_requests: Generating GPTBatchRequests for {mfg_etld1}:{field_type}"
    )

    batch_requests: list[GPTBatchRequest] = []

    chunk_items: list[
        tuple[str, LLMPhraseRelationshipResults, RecursiveTaggingRequest]
    ] = []
    for chunk_bounds, bundle in chunked_request_map.items():
        logger.info(
            f"Processing chunk {chunk_bounds} for {mfg_etld1}:{field_type} to create missing recursive grounding requests. "
        )
        if not bundle.llm_phrase_recursive_tagging_reqs:
            raise ValueError(
                f"Cannot create batch requests for recursive grounding as llm_phrase_recursive_grounding_root_req_nodes is empty."
            )
        llm_phrase_relationship_results = (
            await parse_phrase_relationhip_batch_req_result(
                mfg_etld1=mfg_etld1,
                field_type=field_type,
                chunk_bounds=chunk_bounds,
                extraction_bundle=bundle,
                completed_request_map=llm_phrase_relationship_gpt_request_map,
                deferred_at=timestamp,
            )
        )

        next_level_to_be_executed = max(
            bundle.llm_phrase_recursive_tagging_reqs.keys(),
        )
        next_level_descend_worthy_dtcs = get_descend_worthy_directly_tagged_concept_results(  # tagged concept result will always be in vocab
            initially_tagged_trs=(
                await parse_initial_grounding_batch_request_result(
                    mfg_etld1=mfg_etld1,
                    field_type=field_type,
                    chunk_bounds=chunk_bounds,
                    extraction_bundle=bundle,
                    completed_request_map=completed_initial_grounding_req_map,
                    deferred_at=timestamp,
                )
            ),
            level=next_level_to_be_executed,
            match_label_to_concept_map=match_label_to_concept_map,
        )

        next_level_descend_worthy_tagging_reqs = (
            req
            for req in bundle.llm_phrase_recursive_tagging_reqs[
                next_level_to_be_executed
            ]
            if req.name in match_label_to_concept_map  # otherwise not descend worthy
        )
        next_level_to_be_executed_req_ids = {
            req.descend_req_id for req in next_level_descend_worthy_tagging_reqs
        }
        if (
            next_level_to_be_executed_req_ids
            - missing_phrase_recursive_grounding_req_ids
        ):
            raise ValueError(
                f"Some req ids from last level were not passed as missing: {next_level_to_be_executed_req_ids - missing_phrase_recursive_grounding_req_ids}"
            )

        for next_level_req in next_level_descend_worthy_tagging_reqs:
            next_level_concept_obj = match_label_to_concept_map.get(next_level_req.name)
            if not next_level_concept_obj:
                raise ValueError(
                    f"Cannot create br for out-of-vocab recursive tagging req:{next_level_req.name}"
                )

            # find any directly tagged concepts at this level of execution
            next_level_dtc = next(
                (
                    dtc
                    for dtc in next_level_descend_worthy_dtcs
                    if dtc.concept.name == next_level_concept_obj.name
                ),
                None,
            )
            directly_tagged_phrases = set()
            if next_level_dtc:
                for (
                    _tag,
                    phrase_reason_map,
                ) in next_level_dtc.og_tag_w_phrase_reason_map.items():
                    for phrase, _reason in phrase_reason_map.items():
                        directly_tagged_phrases.add(phrase)

            # now find iteratively_tagged_phrases from any previous level executions
            iteratively_tagged_phrases = set()
            previous_level_parent_req = next(
                (
                    rtr
                    for rtr in bundle.llm_phrase_recursive_tagging_reqs[
                        next_level_to_be_executed - 1
                    ]
                    if rtr.is_parent_of(
                        child_tagging_req=next_level_req,
                        match_label_to_concept_map=match_label_to_concept_map,
                    )
                ),
                None,
            )
            if next_level_to_be_executed != 1 and previous_level_parent_req is None:
                raise ValueError(
                    f"Previous level parent of {next_level_req.name} could not be located."
                )

            if previous_level_parent_req:
                tagged_children_trs = await parse_recursive_grounding_batch_request_result(  # throws error right here if unexpected level encountered
                    mfg_etld1=mfg_etld1,
                    field_type=field_type,
                    chunk_bounds=chunk_bounds,
                    descend_req_id=previous_level_parent_req.descend_req_id,
                    completed_request_map=completed_recursive_grounding_req_map,
                    deferred_at=timestamp,
                )
                for child_tr in tagged_children_trs:
                    child_concept = match_label_to_concept_map.get(child_tr.tag)
                    if not child_concept:
                        continue
                    for phrase, _reason in child_tr.phrase_reason_map.items():
                        iteratively_tagged_phrases.add(phrase)

            # else: this is the very first recursion

            phrases_referenced_in_node = set(directly_tagged_phrases) | set(
                iteratively_tagged_phrases
            )
            phrases_referenced_in_node_w_summary = {
                phrase: summary
                for phrase, summary in llm_phrase_relationship_results.items()
                if phrase in phrases_referenced_in_node
            }
            logger.info(
                f"phrases_referenced_in_node_w_summary:{phrases_referenced_in_node_w_summary}"
            )
            chunk_items.append(
                (chunk_bounds, phrases_referenced_in_node_w_summary, next_level_req)
            )

    # Process chunks in batches to yield control periodically
    for i in range(0, len(chunk_items), BATCH_SIZE):
        batch = chunk_items[i : i + BATCH_SIZE]

        # Process current batch
        for chunk_bounds, phrases_w_summary, pending_rtr in batch:
            tagged_concept_obj = match_label_to_concept_map.get(pending_rtr.name)
            if not tagged_concept_obj:
                raise ValueError(
                    f"Cannot create batch requests for recursive grounding for {mfg_etld1}:{mfg_name}:{chunk_bounds} "
                    f"as tagged_concept:{pending_rtr.name} is unrecognized."
                )

            llm_phrase_grounding_batch_request = create_deferred_phrase_recursive_grounding_gpt_request(
                # used for logging and debugging
                deferred_at=timestamp,
                etld1=mfg_etld1,
                # context variables
                mfg_name=mfg_name,
                phrase_recursive_grounding_prompt=phrase_recursive_grounding_prompt,
                llm_phrase_recursive_grounding_request_id=pending_rtr.descend_req_id,
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
                f"gpt request for {mfg_etld1}:{field_type}"
            )

    return batch_requests


# TODO: needs fixing if used, not used right now
def _create_dummy_completed_phrase_recursive_grounding_batch_request(
    deferred_at: datetime,
    etld1: str,
    llm_phrase_recursive_grounding_request_id: GPTBatchRequestCustomID,
    model_params: GPTModelParams,
    eager: bool,
) -> GPTBatchRequest:
    if llm_phrase_recursive_grounding_request_id is None:
        raise ValueError(
            "_create_dummy_completed_phrase_recursive_grounding_batch_request: llm_phrase_recursive_grounding_request_id is None"
        )

    base_gpt_batch_request = create_base_gpt_batch_request(
        deferred_at=deferred_at,
        etld1=etld1,
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
            role="assistant", content="```json\n{}\n```"
        ),
    )

    return base_gpt_batch_request


def create_deferred_phrase_recursive_grounding_gpt_request(
    deferred_at: datetime,
    etld1: str,
    llm_phrase_recursive_grounding_request_id: str,
    phrase_recursive_grounding_prompt: Prompt,
    # context
    mfg_name: str,
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
    refactored_text = phrase_recursive_grounding_prompt.text.replace(
        "{{parent_material}}", parent_concept.name
    ).replace(
        "{{types_of_parent_material}}",
        json.dumps(
            list(child_concepts),
            cls=ConceptJSONEncoder,
        ),
    )
    context = f"Manufacturer name: {mfg_name}\n\n extracted phrases:\n{json.dumps(verified_phrases_w_og_summary)}"

    gpt_batch_request = create_base_gpt_batch_request(
        deferred_at=deferred_at,
        etld1=etld1,
        custom_id=llm_phrase_recursive_grounding_request_id,
        context=context,
        prompt_text=refactored_text,
        gpt_model=gpt_model,
        model_params=model_params,
        batch_id="Eager" if eager else None,
    )
    return gpt_batch_request
