import asyncio
import json
import logging
from datetime import datetime
from typing import Optional

from core.models.prompt import Prompt
from core.models.field_types import RawLLMMappingResult, RawLLMMappingResult
from core.models.db.gpt_batch_request import GPTBatchRequest
from core.models.deferred_concept_extraction import (
    DeferredConceptExtractionRequests,
)
from core.models.gpt_batch_response_blob import (
    ChatCompletionChoiceMessage,
)
from data_etl_app.models.skos_concept import Concept
from data_etl_app.models.pipeline_nodes.concept.concept_evidence_node import (
    ConceptEvidenceNode,
)
from data_etl_app.models.types_and_enums import ConceptTypeEnum
from litellm_proxy_app.models.llm_model import No_model
from litellm_proxy_app.models.llm_model import LLM_Model
from open_ai_key_app.models.gpt_model_params import GPTModelParams
from open_ai_key_app.models.field_types import GPTBatchRequestCustomID

from core.services.gpt_batch_request_service import (
    create_base_gpt_batch_request,
    get_dummy_gpt_batch_response,
)

from data_etl_app.utils.ground_truth_helper_util import (
    get_verified_evidence_phrases_from_raw_evidence_results,
)
from data_etl_app.utils.llm_mapping_helper import (
    create_deferred_mapping_gpt_request,
    get_matched_concepts_and_unmatched_keywords,
)

logger = logging.getLogger(__name__)


def parse_llm_mapping_result(gpt_response: Optional[str]) -> RawLLMMappingResult:
    if not gpt_response:
        logger.error(f"Invalid gpt_response:{gpt_response}")
        raise ValueError("parse_llm_mapping_result: Empty or invalid response from GPT")

    try:
        gpt_response = gpt_response.replace("```", "").replace("json", "")
        raw_gpt_mapping: RawLLMMappingResult = json.loads(
            gpt_response
        )  # from unknown --> known
        logger.debug(f"raw_gpt_mapping:{json.dumps(raw_gpt_mapping, indent=2)}")
    except:
        raise ValueError(
            f"parse_llm_mapping_result: Invalid response from GPT:{gpt_response}"
        )

    if not isinstance(raw_gpt_mapping, dict):
        raise ValueError(
            "parse_llm_mapping_result: Expected raw_gpt_mapping to be a dictionary"
        )

    logger.debug(f"raw_gpt_mapping:{raw_gpt_mapping}")

    for unknown, knowns_dict in raw_gpt_mapping.items():
        if not isinstance(unknown, str):
            raise ValueError(
                f"parse_llm_mapping_result: Expected unknown term to be a string, got {type(unknown)}"
            )
        if not isinstance(knowns_dict, dict) or not all(
            isinstance(known_label, str) and isinstance(matching_reason, str)
            for known_label, matching_reason in knowns_dict.items()
        ):
            raise ValueError(
                f"parse_llm_mapping_result: Expected known terms to be a dictionary of strings, got {type(knowns_dict)} with elements of types {[type(k) for k in knowns_dict]}"
            )

    return raw_gpt_mapping


async def create_missing_mapping_requests(
    deferred_at: datetime,
    mfg_etld1: str,
    concept_type: ConceptTypeEnum,  # used for logging and debugging
    extraction_requests: DeferredConceptExtractionRequests,
    missing_mapping_req_ids: set[GPTBatchRequestCustomID],
    known_concepts: set[Concept],  # DO NOT MUTATE
    mapping_prompt: Prompt,
    upstream_completed_batch_req_map: dict[GPTBatchRequestCustomID, GPTBatchRequest],
    llm_model: LLM_Model,
    eager: bool,
    model_params: GPTModelParams,
    BATCH_SIZE=100,
) -> list[GPTBatchRequest]:
    logger.info(
        f"create_missing_concept_mapping_requests: Generating GPTBatchRequests for {mfg_etld1}:{concept_type}"
    )

    # create chunk_items only for missing batch requests
    batch_requests: list[GPTBatchRequest] = []
    chunk_items = list(
        {
            chunk_bounds: bundle
            for chunk_bounds, bundle in extraction_requests.request_map.items()
            if bundle.llm_mapping_request_id in missing_mapping_req_ids
        }.items()
    )
    llm_evidence_gpt_request_map: dict[GPTBatchRequestCustomID, GPTBatchRequest] = (
        upstream_completed_batch_req_map
    )

    # Process chunks in batches to yield control periodically
    for i in range(0, len(chunk_items), BATCH_SIZE):
        batch = chunk_items[i : i + BATCH_SIZE]

        # Process current batch
        for chunk_bounds, extraction_bundle in batch:
            llm_evidence_results = await ConceptEvidenceNode.parse_batch_request_result(
                mfg_etld1=mfg_etld1,
                field_type=concept_type,
                chunk_bounds=chunk_bounds,
                extraction_bundle=extraction_bundle,
                completed_request_map=llm_evidence_gpt_request_map,
                deferred_at=deferred_at,
            )

            confirmed_keywords_w_evidence = (
                get_verified_evidence_phrases_from_raw_evidence_results(
                    llm_evidence_results=llm_evidence_results
                )
            )

            (
                _matched_concepts,  # _matched_concepts would be added later by reconcile node
                unmatched_keywords_w_evidence,
            ) = get_matched_concepts_and_unmatched_keywords(
                known_concepts, confirmed_keywords_w_evidence
            )

            llm_mapping_request_id = extraction_bundle.llm_mapping_request_id
            if not llm_mapping_request_id:
                raise ValueError(
                    f"concept_evidence_node.get_batch_request_result: llm_mapping_request_id is None for chunk bounds {chunk_bounds} in {mfg_etld1}:{concept_type}"
                )

            if not unmatched_keywords_w_evidence:
                # add a dummy response blob with empty dict
                logger.info(
                    f"All concepts matched via brute search for {mfg_etld1}:{concept_type} or none were found in the first place, creating dummy mapping request"
                )
                dummy_batch_request = _create_dummy_completed_mapping_batch_request(
                    deferred_at=deferred_at,
                    etld1=mfg_etld1,
                    llm_mapping_request_id=llm_mapping_request_id,
                    model_params=model_params,
                    eager=eager,
                )
                new_batch_request = dummy_batch_request
            else:
                mapping_batch_request = create_deferred_mapping_gpt_request(
                    deferred_at=deferred_at,
                    etld1=mfg_etld1,
                    llm_mapping_request_id=llm_mapping_request_id,
                    known_concepts=known_concepts,  # TODO: RAG known concepts instead of passing full set
                    unmatched_keywords_w_evidence=unmatched_keywords_w_evidence,
                    mapping_prompt=mapping_prompt,
                    gpt_model=llm_model,
                    model_params=model_params,
                    eager=eager,
                )
                new_batch_request = mapping_batch_request

            batch_requests.append(new_batch_request)

        # Yield control to event loop after each batch
        await asyncio.sleep(0)

        if (i + BATCH_SIZE) % 500 == 0:
            logger.info(
                f"Created {min(i + BATCH_SIZE, len(chunk_items))}/{len(chunk_items)} "
                f"gpt request for {mfg_etld1}:{concept_type}"
            )

    return batch_requests


def _create_dummy_completed_mapping_batch_request(
    deferred_at: datetime,
    etld1: str,
    llm_mapping_request_id: GPTBatchRequestCustomID,
    model_params: GPTModelParams,
    eager: bool,
) -> GPTBatchRequest:
    if llm_mapping_request_id is None:
        raise ValueError(
            "_create_dummy_completed_mapping_batch_request: llm_mapping_request_id is None"
        )

    base_gpt_batch_request = create_base_gpt_batch_request(
        deferred_at=deferred_at,
        etld1=etld1,
        custom_id=llm_mapping_request_id,
        context="No mapping needed - no concepts found in text.",
        prompt=Prompt(
            name="dummy_mapping_prompt",
            text="No mapping needed - all concepts matched via brute search or none were found in the first place.",
            s3_version_id="dummy_s3_version_id",
            num_tokens=1,
        ),
        gpt_model=No_model,
        model_params=model_params,
        batch_id="Eager" if eager else "dummy_mapping_batch_id",
    )

    base_gpt_batch_request.response = get_dummy_gpt_batch_response(
        deferred_at=deferred_at,
        request_custom_id=llm_mapping_request_id,
        dummy_chat_completion_id="dummy_mapping_completion_id",
        chat_completion_choice_message=ChatCompletionChoiceMessage(
            role="assistant", content="```json\n{}\n```"
        ),
    )

    return base_gpt_batch_request
