from __future__ import (
    annotations,
)  # This allows you to write self-referential types without quotes, because type annotations are no longer evaluated at function/class definition time — they're stored as strings automatically.

import asyncio
import json
import logging
from datetime import datetime
from typing import Optional

from core.models.gpt_batch_request_blob import (
    GPTBatchRequestBlob,
    GPTBatchRequestBlobBody,
)
from core.models.gpt_batch_request_blob import GPTBatchRequestBlob
from core.models.gpt_batch_response_blob import (
    GPTBatchResponseBlob,
    GPTBatchResponseBlobChoice,
    GPTBatchResponseBlobChoiceMessage,
    GPTBatchResponseBlobUsage,
    GPTBatchResponseBody,
    GPTResponseBlobBody,
)
from core.models.prompt import Prompt
from core.models.db.gpt_batch_request import GPTBatchRequest
from core.models.deferred_concept_extraction import (
    DeferredConceptExtractionRequests,
)
from data_etl_app.models.pipeline_nodes import ConceptSearchNode
from data_etl_app.models.skos_concept import ConceptJSONEncoder
from data_etl_app.models.types_and_enums import ConceptTypeEnum
from open_ai_key_app.models.field_types import GPTBatchRequestCustomID

from core.services.gpt_batch_request_service import (
    create_base_gpt_batch_request,
)

logger = logging.getLogger(__name__)


from open_ai_key_app.models.gpt_model import (
    GPT_4o_mini,
    LLM_Model,
    ModelParameters,
    DefaultModelParameters,
    No_model,
)


def parse_llm_concept_evidence_result(
    gpt_response: Optional[str],
) -> dict[str, str | None]:
    if not gpt_response:
        logger.error(f"Invalid gpt_response:{gpt_response}")
        raise ValueError(
            "parse_llm_concept_evidence_result: Empty or invalid response from GPT"
        )

    try:
        gpt_response = gpt_response.replace("```", "").replace("json", "")
        raw_gpt_mapping: dict[str, str | None] = json.loads(
            gpt_response
        )  # from unknown --> known
        logger.debug(f"raw_gpt_mapping:{json.dumps(raw_gpt_mapping, indent=2)}")
    except json.JSONDecodeError as e:
        raise ValueError(
            f"parse_llm_concept_evidence_result: Invalid response from GPT:{gpt_response}"
        ) from e

    if not isinstance(raw_gpt_mapping, dict):
        raise ValueError(
            "parse_llm_concept_evidence_result: Expected raw_gpt_mapping to be a dictionary"
        )

    logger.debug(f"raw_gpt_mapping:{raw_gpt_mapping}")

    return raw_gpt_mapping


async def create_missing_concept_evidence_requests(
    mfg_etld1: str,
    concept_type: ConceptTypeEnum,  # used for logging and debugging
    extraction_requests: DeferredConceptExtractionRequests,
    missing_evidence_req_ids: set[GPTBatchRequestCustomID],
    mfg_text: str,
    evidence_prompt: Prompt,
    upstream_completed_batch_req_map: dict[GPTBatchRequestCustomID, GPTBatchRequest],
    deferred_at: datetime,
    llm_model: LLM_Model,
    model_params: ModelParameters = DefaultModelParameters,
    BATCH_SIZE=100,
) -> list[GPTBatchRequest]:
    logger.info(
        f"create_missing_concept_evidence_requests: Generating GPTBatchRequests for {mfg_etld1}:{concept_type}"
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
            f"create_missing_concept_evidence_requests: No completed GPTBatchRequests found for {mfg_etld1}:{concept_type} in upstream_completed_batch_req_map"
        )

    # Process chunks in batches to yield control periodically
    for i in range(0, len(chunk_items), BATCH_SIZE):
        batch = chunk_items[i : i + BATCH_SIZE]

        # Process current batch
        for chunk_bounds, extraction_bundle in batch:
            llm_search_result = await ConceptSearchNode.parse_batch_request_result(
                mfg_etld1=mfg_etld1,
                field_type=concept_type,
                chunk_bounds=chunk_bounds,
                extraction_bundle=extraction_bundle,
                completed_request_map=llm_search_gpt_request_map,
                deferred_at=deferred_at,
            )
            llm_evidence_request_id = extraction_bundle.llm_evidence_request_id
            if not llm_evidence_request_id:
                raise ValueError(
                    f"concept_search_node.get_batch_request_result: llm_evidence_request_id is None for chunk bounds {chunk_bounds} in {mfg_etld1}:{concept_type}"
                )

            # combine with brute force results to get final search results for the chunk,
            # which will be used as context for evidence extraction we want to include all brute force results
            # as evidence even if they weren't identified by the LLM search, because the brute force
            # results were generated with high recall in mind and we don't want to miss out on any
            # potential evidence by filtering them with the LLM search results.
            # On the other hand, we want to leverage the LLM search to potentially identify additional
            # concepts that the brute force method missed, while still keeping the context manageable for
            # the evidence extraction step.
            all_search_results = llm_search_result | extraction_bundle.brute

            if not all_search_results:
                # add a dummy response blob with empty dict
                logger.info(
                    f"No concepts found in text, for {mfg_etld1}:{concept_type}, creating dummy evidence request"
                )
                dummy_batch_request = _create_dummy_completed_evidence_batch_request(
                    deferred_at=deferred_at,
                    llm_evidence_request_id=llm_evidence_request_id,
                )
                new_batch_request = dummy_batch_request
            else:
                evidence_batch_request = create_deferred_evidence_gpt_request(
                    deferred_at=deferred_at,
                    llm_evidence_request_id=llm_evidence_request_id,
                    mfg_text=mfg_text,
                    search_results=all_search_results,
                    evidence_prompt=evidence_prompt,
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
                f"gpt request for {mfg_etld1}:{concept_type}"
            )

    return batch_requests


def _create_dummy_completed_evidence_batch_request(
    deferred_at: datetime, llm_evidence_request_id: GPTBatchRequestCustomID
) -> GPTBatchRequest:
    if llm_evidence_request_id is None:
        raise ValueError(
            "_create_dummy_completed_evidence_batch_request: llm_evidence_request_id is None"
        )

    return GPTBatchRequest(
        created_at=deferred_at,
        updated_at=deferred_at,
        num_batches_paired_with=0,
        request=GPTBatchRequestBlob(
            custom_id=llm_evidence_request_id,
            body=GPTBatchRequestBlobBody(
                model=No_model.model_name,
                messages=[
                    {
                        "role": "system",
                        "content": "No evidence needed - no concepts were found in the text.",
                    }
                ],
                input_tokens=1,
                max_tokens=1,
            ),
        ),
        batch_id="empty_unmapped_unknowns",
        response_blob=GPTBatchResponseBlob(
            batch_id="empty_unmapped_unknowns",
            request_custom_id="no-request",
            response=GPTBatchResponseBody(
                status_code=200,
                body=GPTResponseBlobBody(
                    created=deferred_at,
                    choices=[
                        GPTBatchResponseBlobChoice(
                            index=0,
                            message=GPTBatchResponseBlobChoiceMessage(
                                role="assistant", content="```json\n{}\n```"
                            ),
                        )
                    ],
                    usage=GPTBatchResponseBlobUsage(
                        prompt_tokens=1,
                        completion_tokens=1,
                        total_tokens=2,
                    ),
                ),
            ),
        ),
    )


def create_deferred_evidence_gpt_request(
    deferred_at: datetime,
    llm_evidence_request_id: str,
    mfg_text: str,
    search_results: set[str],
    evidence_prompt: Prompt,
    gpt_model: LLM_Model = GPT_4o_mini,
    model_params: ModelParameters = DefaultModelParameters,
) -> GPTBatchRequest:
    logger.info(
        f"create_deferred_evidence_gpt_request: Generating GPTBatchRequest for {llm_evidence_request_id}"
    )
    context = json.dumps(
        {"text": mfg_text, "search_results": list(search_results)},
        cls=ConceptJSONEncoder,
    )
    gpt_batch_request = create_base_gpt_batch_request(
        deferred_at=deferred_at,
        custom_id=llm_evidence_request_id,
        context=context,
        prompt=evidence_prompt,
        gpt_model=gpt_model,
        model_params=model_params,
    )
    return gpt_batch_request
