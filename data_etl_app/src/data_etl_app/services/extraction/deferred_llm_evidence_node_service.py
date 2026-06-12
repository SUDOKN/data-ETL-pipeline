from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime
from typing import Optional, Union

from core.models.prompt import Prompt
from core.models.gpt_batch_response_blob import (
    ChatCompletionChoiceMessage,
)
from core.models.db.gpt_batch_request import GPTBatchRequest
from core.models.deferred_concept_extraction import (
    DeferredConceptExtractionRequests,
)
from core.models.deferred_keyword_extraction import (
    DeferredKeywordExtractionRequests,
)
from data_etl_app.models.pipeline_nodes.search_node import SearchNode
from data_etl_app.models.types_and_enums import LLMExtractedFieldTypeEnum
from open_ai_key_app.models.gpt_model_params import GPTModelParams
from open_ai_key_app.models.field_types import GPTBatchRequestCustomID
from litellm_proxy_app.models.llm_model import No_model
from litellm_proxy_app.models.llm_model import LLM_Model

from core.services.gpt_batch_request_service import (
    create_base_gpt_batch_request,
    get_dummy_gpt_batch_response,
)
from data_etl_app.utils.ground_truth_helper_util import (
    merge_llm_and_brute_search_results,
)

logger = logging.getLogger(__name__)

# Evidence is the second phase for both keyword (search -> evidence -> reconcile) and
# concept (search -> evidence -> mapping -> reconcile) pipelines. Both deferred request
# types carry an ``llm_evidence_request_id`` on each chunk bundle, so the evidence phase
# can be driven by a single, shared implementation.
DeferredEvidenceExtractionRequests = Union[
    DeferredConceptExtractionRequests, DeferredKeywordExtractionRequests
]


def parse_llm_evidence_result(
    gpt_response: Optional[str],
) -> dict[str, str]:
    if not gpt_response:
        logger.error(f"Invalid gpt_response:{gpt_response}")
        raise ValueError(
            "parse_llm_evidence_result: Empty or invalid response from GPT"
        )

    try:
        gpt_response = gpt_response.replace("```", "").replace("json", "")
        raw_gpt_evidence: dict[str, str] = json.loads(gpt_response)
        logger.debug(f"raw_gpt_evidence:{json.dumps(raw_gpt_evidence, indent=2)}")
    except json.JSONDecodeError as e:
        raise ValueError(
            f"parse_llm_evidence_result: Invalid response from GPT:{gpt_response}"
        ) from e

    if not isinstance(raw_gpt_evidence, dict):
        raise ValueError(
            "parse_llm_evidence_result: Expected raw_gpt_evidence to be a dictionary"
        )

    logger.debug(f"raw_gpt_evidence:{raw_gpt_evidence}")

    return raw_gpt_evidence


async def create_missing_evidence_requests(
    mfg_etld1: str,
    mfg_name: str,
    field_type: LLMExtractedFieldTypeEnum,  # used for logging and debugging
    extraction_requests: DeferredEvidenceExtractionRequests,
    missing_evidence_req_ids: set[GPTBatchRequestCustomID],
    mfg_text: str,
    evidence_prompt: Prompt,
    upstream_completed_batch_req_map: dict[GPTBatchRequestCustomID, GPTBatchRequest],
    deferred_at: datetime,
    llm_model: LLM_Model,
    eager: bool,
    model_params: GPTModelParams,
    BATCH_SIZE=100,
) -> list[GPTBatchRequest]:
    logger.info(
        f"create_missing_evidence_requests: Generating GPTBatchRequests for {mfg_etld1}:{field_type}"
    )

    batch_requests: list[GPTBatchRequest] = []
    chunk_items = list(
        {
            chunk_bounds: bundle
            for chunk_bounds, bundle in extraction_requests.request_map.items()
            if bundle.llm_evidence_request_id in missing_evidence_req_ids
        }.items()
    )
    # Create lookup map: custom_id -> GPTBatchRequest
    llm_search_gpt_request_map: dict[GPTBatchRequestCustomID, GPTBatchRequest] = (
        upstream_completed_batch_req_map
    )
    if not llm_search_gpt_request_map:
        raise ValueError(
            f"create_missing_evidence_requests: No completed GPTBatchRequests found for {mfg_etld1}:{field_type} in upstream_completed_batch_req_map"
        )

    # Process chunks in batches to yield control periodically
    for i in range(0, len(chunk_items), BATCH_SIZE):
        batch = chunk_items[i : i + BATCH_SIZE]

        # Process current batch
        for chunk_bounds, extraction_bundle in batch:
            llm_search_results = await SearchNode.parse_batch_request_result(
                mfg_etld1=mfg_etld1,
                field_type=field_type,
                chunk_bounds=chunk_bounds,
                extraction_bundle=extraction_bundle,
                completed_request_map=llm_search_gpt_request_map,
                deferred_at=deferred_at,
            )
            llm_evidence_request_id = extraction_bundle.llm_evidence_request_id
            if not llm_evidence_request_id:
                raise ValueError(
                    f"create_missing_evidence_requests: llm_evidence_request_id is None for chunk bounds {chunk_bounds} in {mfg_etld1}:{field_type}"
                )

            # Combine LLM search with brute force results (concept pipelines only) to get the
            # final search results for the chunk, which are used as context for evidence
            # extraction. We include all brute force results as evidence candidates even if
            # they weren't identified by the LLM search, because the brute force results were
            # generated with high recall in mind and we don't want to miss out on any potential
            # evidence by filtering them with the LLM search results. Keyword pipelines have no
            # brute force phase
            all_search_results = merge_llm_and_brute_search_results(
                llm_search_results=llm_search_results,
                brute_search_results=extraction_bundle.brute,
            )

            if not all_search_results:
                # add a dummy response blob with empty dict
                logger.info(
                    f"No phrases found in text, for {mfg_etld1}:{field_type}, creating dummy evidence request"
                )
                dummy_batch_request = _create_dummy_completed_evidence_batch_request(
                    deferred_at=deferred_at,
                    etld1=mfg_etld1,
                    llm_evidence_request_id=llm_evidence_request_id,
                    model_params=model_params,
                    eager=eager,
                )
                new_batch_request = dummy_batch_request
            else:
                start, end = int(chunk_bounds.split(":")[0]), int(
                    chunk_bounds.split(":")[1]
                )
                logger.info(
                    f"Passing on candidates {all_search_results} to evidence phase for {mfg_etld1}:{field_type} chunk {chunk_bounds}"
                )
                evidence_batch_request = create_deferred_evidence_gpt_request(
                    deferred_at=deferred_at,
                    etld1=mfg_etld1,
                    llm_evidence_request_id=llm_evidence_request_id,
                    mfg_name=mfg_name,
                    mfg_text=mfg_text[start:end],
                    search_results=all_search_results,
                    evidence_prompt=evidence_prompt,
                    eager=eager,
                    gpt_model=llm_model,
                    model_params=model_params,
                )
                new_batch_request = evidence_batch_request

            batch_requests.append(new_batch_request)

        # Yield control to event loop after each batch
        await asyncio.sleep(0)

        if (i + BATCH_SIZE) % 500 == 0:
            logger.info(
                f"Created {min(i + BATCH_SIZE, len(chunk_items))}/{len(chunk_items)} "
                f"gpt request for {mfg_etld1}:{field_type}"
            )

    return batch_requests


def _create_dummy_completed_evidence_batch_request(
    deferred_at: datetime,
    etld1: str,
    llm_evidence_request_id: GPTBatchRequestCustomID,
    model_params: GPTModelParams,
    eager: bool,
) -> GPTBatchRequest:
    if llm_evidence_request_id is None:
        raise ValueError(
            "_create_dummy_completed_evidence_batch_request: llm_evidence_request_id is None"
        )

    base_gpt_batch_request = create_base_gpt_batch_request(
        deferred_at=deferred_at,
        etld1=etld1,
        custom_id=llm_evidence_request_id,
        context="No evidence needed - no phrases found in text.",
        prompt=Prompt(
            name="dummy_evidence_prompt",
            text="No evidence needed - no phrases found in text or all matched to existing ones.",
            s3_version_id="dummy_s3_version_id",
            num_tokens=1,
        ),
        gpt_model=No_model,
        model_params=model_params,
        batch_id="Eager" if eager else "dummy_evidence_batch_id",
    )

    base_gpt_batch_request.response = get_dummy_gpt_batch_response(
        deferred_at=deferred_at,
        request_custom_id=llm_evidence_request_id,
        dummy_chat_completion_id="dummy_completion_id",
        chat_completion_choice_message=ChatCompletionChoiceMessage(
            role="assistant", content="```json\n{}\n```"
        ),
    )

    return base_gpt_batch_request


def create_deferred_evidence_gpt_request(
    deferred_at: datetime,
    etld1: str,
    llm_evidence_request_id: str,
    mfg_name: str,
    mfg_text: str,
    search_results: set[str],
    evidence_prompt: Prompt,
    gpt_model: LLM_Model,
    eager: bool,
    model_params: GPTModelParams,
) -> GPTBatchRequest:
    logger.info(
        f"create_deferred_evidence_gpt_request: Generating GPTBatchRequest for {llm_evidence_request_id}"
    )
    context = f"Manufacturer: {mfg_name}\n\nscraped text:\n{mfg_text} \n\n extracted phrases:\n{list(search_results)}"

    gpt_batch_request = create_base_gpt_batch_request(
        deferred_at=deferred_at,
        etld1=etld1,
        custom_id=llm_evidence_request_id,
        context=context,
        prompt=evidence_prompt,
        gpt_model=gpt_model,
        model_params=model_params,
        batch_id="Eager" if eager else None,
    )
    return gpt_batch_request
