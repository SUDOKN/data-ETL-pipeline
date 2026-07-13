from __future__ import annotations

import asyncio
import json
import logging
import traceback
from datetime import datetime
from typing import Optional

from core.models.db.gpt_batch_request import GPTBatchRequest
from core.models.field_types import (
    LLMGroundingResults,
    LLMPhraseRelationshipResults,
    RecursivelyTaggedConceptNode,
    MatchedLabelAndReasonPair,
)
from core.models.llm_model import LLM_Model, NO_MODEL
from core.models.prompt import Prompt
from core.models.gpt_batch_response_blob import (
    ChatCompletionChoiceMessage,
)
from core.models.deferred_concept_extraction import (
    ConceptExtractionRequestBundle,
    ConceptExtractionRequestMap,
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

logger = logging.getLogger(__name__)


def get_recursive_grounding_request_custom_id(
    mfg_etld1: str,
    field_type: LLMExtractedFieldTypeEnum,
    chunk_bounds: str,
    parent_level: int,
    parent_tagged_concept_name: str,
    llm_model: LLM_Model,
    model_params: GPTModelParams,
) -> GPTBatchRequestCustomID:
    return (
        f"{mfg_etld1}>{field_type.name}>llm_phrase_recursive_grounding>chunk>"
        f"{chunk_bounds}>l[{parent_level}]>{parent_tagged_concept_name}>{model_params.to_custom_id_segment(llm_model.name)}"
    )


def parse_llm_phrase_recursive_grounding_result(
    gpt_response: Optional[str],
) -> LLMGroundingResults:
    if not gpt_response:
        logger.error(f"Invalid gpt_response:{gpt_response}")
        raise ValueError(
            "parse_llm_phrase_recursive_grounding_result: Empty or invalid response from GPT"
        )

    try:
        gpt_response = gpt_response.replace("```", "").replace("json", "")
        raw_llm_recursive_grounding_result: dict[str, dict[str, str]] = json.loads(
            gpt_response
        )
        logger.debug(
            f"raw_llm_recursive_grounding_result:{json.dumps(raw_llm_recursive_grounding_result, indent=2)}"
        )
    except json.JSONDecodeError as e:
        raise ValueError(
            f"parse_llm_phrase_recursive_grounding_result: Invalid response from GPT:{gpt_response}"
        ) from e

    if not isinstance(raw_llm_recursive_grounding_result, dict):
        raise ValueError(
            "parse_llm_phrase_recursive_grounding_result: Expected raw_llm_recursive_grounding_result to be a dictionary"
        )

    logger.debug(
        f"raw_llm_recursive_grounding_result:{raw_llm_recursive_grounding_result}"
    )

    return raw_llm_recursive_grounding_result


async def parse_batch_request_result(
    mfg_etld1: str,
    field_type: LLMExtractedFieldTypeEnum,
    chunk_bounds: str,
    descend_req_id: GPTBatchRequestCustomID,
    completed_request_map: dict[GPTBatchRequestCustomID, GPTBatchRequest],
    deferred_at: datetime,
) -> LLMGroundingResults:
    if not descend_req_id:
        raise ValueError(
            f"phrase_recursive_grounding_node.parse_batch_request_result: phrase_recursive_grounding_request_id is None for chunk bounds {chunk_bounds} in {mfg_etld1}:{field_type.name}"
        )

    phrase_recursive_grounding_request = completed_request_map.get(descend_req_id)
    if not phrase_recursive_grounding_request:
        raise ValueError(
            f"phrase_recursive_grounding_node.parse_batch_request_result: Missing GPTBatchRequest for phrase_recursive_grounding request ID {descend_req_id} in {mfg_etld1}:{field_type.name}"
        )
    elif not phrase_recursive_grounding_request.response:
        raise ValueError(
            f"phrase_recursive_grounding_node.parse_batch_request_result: GPTBatchRequest for phrase_recursive_grounding request ID {descend_req_id} has no response_blob in {mfg_etld1}:{field_type.name}"
        )

    try:
        phrase_recursive_grounding_results = (
            parse_llm_phrase_recursive_grounding_result(
                phrase_recursive_grounding_request.response.result
            )
        )
        return phrase_recursive_grounding_results
    except Exception as e:
        await record_response_parse_error(
            gpt_batch_request=phrase_recursive_grounding_request,
            error_message=str(e),
            timestamp=deferred_at,
            traceback_str=traceback.format_exc(),
        )
        logger.error(
            f"phrase_recursive_grounding_node.parse_batch_request_result: Error parsing phrase_recursive_grounding results for manufacturer {mfg_etld1} from GPT response: {e}"
        )
        raise


def get_tagged_concepts_from_grounding_results(
    mfg_etld1: str,
    field_type: ConceptTypeEnum,
    chunk_bounds: str,
    initial: bool,
    grounding_results: LLMGroundingResults,
    match_label_to_concept_map: dict[str, Concept],
    llm_model: LLM_Model,
    model_params: GPTModelParams,
    level: int | None,  # None here means get all levels
) -> dict[str, RecursivelyTaggedConceptNode]:
    """
    LOGIC:

    Result formats:

    Initial Taggging
    {
        "phrase implying manufacturer's capability to process a listed material": {
            "<listed material>": "<brief reason grounded in the phrase and its summary>"
        },
        "phrase implying manufacturer's capability to process an unlisted material": {
            "<unlisted material>": "<brief reason grounded in the phrase and its summary>"
        },
        "phrase C implying manufacturer's capability to process multiple materials": {
            "<listed material>": "<brief reason grounded in the phrase and its summary>",
            "<another listed material>": "<brief reason grounded in the phrase and its summary>",
            "<unlisted material>": "<brief reason grounded in the phrase and its summary>"
        }
    }

    -- OR --

    Recursive Tagging
    {
        "phrase implying a listed type": {
            "<listed material>": "<brief reason citing the phrase and its summary>"
        },
        "phrase implying an unlisted sibling type": {
            "<unlisted sibling material>": "<brief reason citing the phrase and its summary>",
        },
        "phrase implying multiple types": {
            "<listed material>": "<brief reason citing the phrase and its summary>",
            "<another listed material>": "<brief reason citing the phrase and its summary>",
            "<unlisted sibling material>": "<brief reason citing the phrase and its summary>"
        },
        "phrase that cannot be narrowed further": {
            "None of the above": "<brief reason citing the phrase and its summary>"
        }
    }

    1.  Check if concept tagged is known or not
        No?
            Is the level passed == None?
                Ys? Ignore, can be parsed again from response in the reconcile node, no use for this stage
                No? Get/Create unrecognized tagged concept object with matched label and passed level,
                    Then add it to retval
        Ys?
            Find matched_concept_obj using matched label
            Then check is the level passed != None and != matched_concept.level?
                Ys? Ignore this matched_concept
                No? Get/Create a tagged concept object using the above matched_concept
                    Then add it to retval
                    Check if initial == True?
                        Ys? Add phrase to directly_tagged_phrase_reason_map
                        No? Add phrase to iteratively_tagged_phrase_reason_map

    2.  Basically, returns in-vocab concepts when initial, matches level if passed
        Otherwise, returns out-of-vocab concepts also but only when post initial and copies the level passed. The level in this case must be not None.
    """
    if not initial and level is None:
        raise ValueError(
            f"Cannot get_tagged_concepts_from_grounding_results for a post initial grounding stage with level None."
        )

    all_tagged_concepts: dict[str, RecursivelyTaggedConceptNode] = {}

    for (
        phrase,
        matched_concepts_w_reason,
    ) in grounding_results.items():
        for (
            matched_concept_label,
            reason,
        ) in matched_concepts_w_reason.items():

            if matched_concept_label not in match_label_to_concept_map:
                if level is None:  # initial must be true too automatically
                    continue

                unrecognized_tagged_concept_obj = all_tagged_concepts.get(
                    matched_concept_label
                )
                if not unrecognized_tagged_concept_obj:
                    unrecognized_tagged_concept_obj = RecursivelyTaggedConceptNode(
                        name=matched_concept_label,
                        level=level,
                        descend_req_id=get_recursive_grounding_request_custom_id(
                            mfg_etld1=mfg_etld1,
                            field_type=field_type,
                            chunk_bounds=chunk_bounds,
                            parent_level=level,
                            parent_tagged_concept_name=matched_concept_label,
                            llm_model=llm_model,
                            model_params=model_params,
                        ),
                        directly_tagged_phrase_reason_map={},
                        iteratively_tagged_phrase_reason_map={},
                        children=[],
                    )
                all_tagged_concepts[unrecognized_tagged_concept_obj.name] = (
                    unrecognized_tagged_concept_obj
                )
            else:
                matched_concept_obj = match_label_to_concept_map.get(
                    matched_concept_label
                )
                assert matched_concept_obj
                if level is not None and matched_concept_obj.level != level:
                    if initial:
                        logger.info(
                            f"Skipping matched concept matched_concept_obj:{matched_concept_obj.name}, l{matched_concept_obj.level}"
                            f"as it is not at the level:{level}."
                        )
                        continue
                    else:
                        raise ValueError(f"")

                tagged_concept_obj = all_tagged_concepts.get(matched_concept_obj.name)
                if not tagged_concept_obj:
                    tagged_concept_obj = RecursivelyTaggedConceptNode(
                        name=matched_concept_obj.name,
                        level=matched_concept_obj.level,
                        descend_req_id=get_recursive_grounding_request_custom_id(
                            mfg_etld1=mfg_etld1,
                            field_type=field_type,
                            chunk_bounds=chunk_bounds,
                            parent_level=matched_concept_obj.level,
                            parent_tagged_concept_name=matched_concept_obj.name,
                            llm_model=llm_model,
                            model_params=model_params,
                        ),
                        directly_tagged_phrase_reason_map={},
                        iteratively_tagged_phrase_reason_map={},
                        children=[],
                    )
                    all_tagged_concepts[matched_concept_obj.name] = tagged_concept_obj

                if initial:
                    tagged_concept_obj.directly_tagged_phrase_reason_map[phrase] = (
                        MatchedLabelAndReasonPair(
                            matched_label=matched_concept_label, reason=reason
                        )
                    )
                else:
                    # tagged_concept_obj.iteratively_tagged_phrase_reason_map.setdefault(phrase, [])
                    tagged_concept_obj.iteratively_tagged_phrase_reason_map[phrase] = (
                        MatchedLabelAndReasonPair(
                            matched_label=matched_concept_label, reason=reason
                        )
                    )

    return all_tagged_concepts


def get_flattened_embedded_concept_req_ids(
    extraction_bundle: ConceptExtractionRequestBundle,
    in_vocab: bool,
    match_label_to_concept_map: dict[str, Concept],
) -> list[GPTBatchRequestCustomID]:
    embedded_grounding_req_ids: list[GPTBatchRequestCustomID] = [
        tagged_concept.descend_req_id
        for tagged_concept in get_flattened_embedded_tagged_concepts(
            extraction_bundle=extraction_bundle,
            in_vocab=in_vocab,
            match_label_to_concept_map=match_label_to_concept_map,
        )
    ]

    return embedded_grounding_req_ids


def get_flattened_embedded_tagged_concepts(
    extraction_bundle: ConceptExtractionRequestBundle,
    in_vocab: bool,
    match_label_to_concept_map: dict[str, Concept],
) -> list[RecursivelyTaggedConceptNode]:
    if extraction_bundle.llm_phrase_recursive_grounding_root_req_nodes is None:
        raise ValueError(
            f"Cannot get get_flattened_embedded_tagged_concepts because .llm_phrase_recursive_grounding_root_req_nodes is None."
        )

    embedded_tagged_concepts: list[RecursivelyTaggedConceptNode] = []
    for req_node in extraction_bundle.llm_phrase_recursive_grounding_root_req_nodes:
        if in_vocab and req_node.name not in match_label_to_concept_map:
            # must be "None of the above" or unlisted concept
            continue
        embedded_tagged_concepts.extend(
            get_flattened_embedded_tagged_concepts_helper(req_node)
        )

    return embedded_tagged_concepts


def get_flattened_embedded_tagged_concepts_helper(
    curr_node: RecursivelyTaggedConceptNode,
) -> list[RecursivelyTaggedConceptNode]:
    retval: list[RecursivelyTaggedConceptNode] = []
    retval.append(curr_node)  # CAUTION: children are not removed from the object
    for nested_child_node in curr_node.children:
        nested_flattened_child_nodes = get_flattened_embedded_tagged_concepts_helper(
            nested_child_node
        )
        retval.extend(nested_flattened_child_nodes)

    return retval


def get_leaf_w_parent_from_tagged_concepts(
    parent_concept_nodes: list[RecursivelyTaggedConceptNode],
) -> tuple[set[RecursivelyTaggedConceptNode], set[RecursivelyTaggedConceptNode]]:
    parent_concepts: set[RecursivelyTaggedConceptNode] = set()  # in-vocab
    leaf_concepts: set[RecursivelyTaggedConceptNode] = set()  # out-of-vocab
    for parent_node in parent_concept_nodes:
        for child_node in parent_node.children:
            get_leaf_w_parent_from_tagged_concepts_helper(
                parent_concept_node=parent_node,
                curr_concept_node=child_node,
                parents_so_far=parent_concepts,
                leaves_so_far=leaf_concepts,
            )

    return parent_concepts, leaf_concepts


def get_leaf_w_parent_from_tagged_concepts_helper(
    parent_concept_node: RecursivelyTaggedConceptNode,
    curr_concept_node: RecursivelyTaggedConceptNode,
    parents_so_far: set[RecursivelyTaggedConceptNode],
    leaves_so_far: set[RecursivelyTaggedConceptNode],
) -> None:
    if not curr_concept_node.children:  # this is a leaf
        parents_so_far.add(parent_concept_node)
        leaves_so_far.add(curr_concept_node)
    else:
        for child_node in curr_concept_node.children:
            get_leaf_w_parent_from_tagged_concepts_helper(
                parent_concept_node=curr_concept_node,
                curr_concept_node=child_node,
                parents_so_far=parents_so_far,
                leaves_so_far=leaves_so_far,
            )


async def create_missing_phrase_recursive_grounding_requests(
    # used for logging and debugging
    mfg_etld1: str,
    mfg_name: str,
    field_type: ConceptTypeEnum,
    # context
    chunked_request_map: ConceptExtractionRequestMap,
    missing_phrase_recursive_grounding_req_ids: set[
        GPTBatchRequestCustomID
    ],  # spread across all chunks, can be partially covering a level in req tree
    phrase_recursive_grounding_prompt: Prompt,
    completed_recursive_grounding_req_map: dict[
        GPTBatchRequestCustomID, GPTBatchRequest
    ],
    llm_phrase_relationship_gpt_request_map: dict[
        GPTBatchRequestCustomID, GPTBatchRequest
    ],
    match_label_to_concept_map: dict[str, Concept],  # DO NOT MUTATE
    # req metadata
    deferred_at: datetime,
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
        tuple[str, LLMPhraseRelationshipResults, RecursivelyTaggedConceptNode]
    ] = []
    for chunk_bounds, bundle in chunked_request_map.items():
        chunk_recursive_grounding_node_req_ids = get_flattened_embedded_concept_req_ids(
            extraction_bundle=bundle,
            in_vocab=True,
            match_label_to_concept_map=match_label_to_concept_map,
        )
        missing_node_req_ids = missing_phrase_recursive_grounding_req_ids & set(
            chunk_recursive_grounding_node_req_ids
        )
        if missing_node_req_ids:
            missing_chunk_recursive_grounding_nodes = [
                chunk_recursive_grounding_node
                for chunk_recursive_grounding_node in get_flattened_embedded_tagged_concepts(
                    extraction_bundle=bundle,
                    in_vocab=True,
                    match_label_to_concept_map=match_label_to_concept_map,
                )
                if chunk_recursive_grounding_node.descend_req_id in missing_node_req_ids
            ]
            for node in missing_chunk_recursive_grounding_nodes:
                if node.children:
                    raise ValueError(
                        f"Cannot create missing batch requests for {mfg_etld1}:{mfg_name}:{chunk_bounds}. "
                        f"Can only create for deepest level nodes whose .children is not set"
                        f"as phrases are waiting to descend from the parent:{node.name}."
                    )

            llm_phrase_relationship_results = (
                await parse_phrase_relationhip_batch_req_result(
                    mfg_etld1=mfg_etld1,
                    field_type=field_type,
                    chunk_bounds=chunk_bounds,
                    extraction_bundle=bundle,
                    completed_request_map=llm_phrase_relationship_gpt_request_map,
                    deferred_at=deferred_at,
                )
            )
            phrases_referenced_in_node = set(
                node.directly_tagged_phrase_reason_map.keys()
            ) | set(node.iteratively_tagged_phrase_reason_map.keys())
            phrases_referenced_in_node_w_summary = {
                phrase: summary
                for phrase, summary in llm_phrase_relationship_results.items()
                if phrase in phrases_referenced_in_node
            }
            chunk_items.extend(
                [
                    (chunk_bounds, phrases_referenced_in_node_w_summary, node)
                    for node in missing_chunk_recursive_grounding_nodes
                ]
            )

    # Process chunks in batches to yield control periodically
    for i in range(0, len(chunk_items), BATCH_SIZE):
        batch = chunk_items[i : i + BATCH_SIZE]

        # Process current batch
        for chunk_bounds, phrases_w_summary, tagged_parent_concept_node in batch:
            """
            LOGIC

            Create a descend batch request from this parent by collecting phrases from both
            directly_tagged_phrase_reason_map and iteratively_tagged_phrase_reason_map

            The parent may be a leaf so it'll be interesting to pass an empty list of options
            forcing model to choose its own.
            """
            tagged_concept_obj = match_label_to_concept_map.get(
                tagged_parent_concept_node.name
            )
            if not tagged_concept_obj:
                raise ValueError(
                    f"Cannot create batch requests for recursive grounding for {mfg_etld1}:{mfg_name}:{chunk_bounds} "
                    f"as tagged_concept:{tagged_parent_concept_node.name} is unrecognized."
                )

            llm_phrase_grounding_batch_request = create_deferred_phrase_recursive_grounding_gpt_request(
                # used for logging and debugging
                deferred_at=deferred_at,
                etld1=mfg_etld1,
                # context variables
                mfg_name=mfg_name,
                phrase_recursive_grounding_prompt=phrase_recursive_grounding_prompt,
                llm_phrase_recursive_grounding_request_id=tagged_parent_concept_node.descend_req_id,
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
        context="No phrase relationship screening needed - no phrases found in text.",
        prompt=Prompt(
            name="dummy_phrase_relationship_screening_prompt",
            text="No phrase relationship screening needed - no phrases found in text by brute force or by LLM.",
            s3_version_id="dummy_s3_version_id",
            num_tokens=1,
        ),
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

    phrase_recursive_grounding_prompt.text.replace(
        "{{parent_material}}", parent_concept.name
    ).replace(
        "{{types_of_parent_material}}",
        json.dumps(
            child_concepts,
            cls=ConceptJSONEncoder,
        ),
    )

    context = f"Manufacturer name: {mfg_name}\n\n extracted phrases:\n{list(verified_phrases_w_og_summary)}"

    gpt_batch_request = create_base_gpt_batch_request(
        deferred_at=deferred_at,
        etld1=etld1,
        custom_id=llm_phrase_recursive_grounding_request_id,
        context=context,
        prompt=phrase_recursive_grounding_prompt,
        gpt_model=gpt_model,
        model_params=model_params,
        batch_id="Eager" if eager else None,
    )
    return gpt_batch_request
