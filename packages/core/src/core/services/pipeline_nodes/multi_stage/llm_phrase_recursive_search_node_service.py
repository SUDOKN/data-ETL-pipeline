from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from typing import Optional
import traceback

from packages.core.src.core.models.db.gpt_batch_request import GPTBatchRequest
from packages.core.src.core.models.extraction_schemas.search import LLMSearchResults
from packages.core.src.core.models.file_objects.prompt import Prompt
from litellm_proxy_app.models.llm_model import LLM_Model
from packages.core.src.core.models.deferred_extraction.deferred_phrase_extraction_requests import (
    LLMPhraseExtractionRequestMap,
    LLMPhraseExtractionRequestBundle,
)
from packages.core.src.core.models.pipeline_nodes.multi_stage.base.llm_phrase_search_node import (
    LLMPhraseSearchNode,
)
from packages.core.src.core.models.types_and_enums import (
    LLMExtractedFieldTypeEnum,
)
from packages.core.src.core.models.field_types import BatchRequestIDType
from open_ai_key_app.models.gpt_model_params import GPTModelParams

from packages.core.src.core.services.gpt_batch_request.gpt_batch_request_writes import (
    record_response_parse_error,
)
from packages.core.src.core.services.gpt_batch_request.gpt_batch_request_service import (
    create_base_gpt_batch_request,
)
from packages.core.src.core.services.pipeline_nodes.multi_stage.llm_phrase_search_node_service import (
    LLM_SEARCH_RESPONSE_SCHEMA,
    parse_llm_search_response,
)
from packages.core.src.core.utils.ground_truth_helper_util import (
    filter_non_overlapping_brute_results,
)

logger = logging.getLogger(__name__)


async def parse_recursive_search_round_result(
    mfg_etld1: str,
    field_type: LLMExtractedFieldTypeEnum,
    chunk_bounds: str,
    round_req_id: BatchRequestIDType,
    completed_request_map: dict[BatchRequestIDType, GPTBatchRequest],
    timestamp: datetime,
) -> LLMSearchResults:
    """Parse the phrases returned by a single recursive search round.

    Returns an empty set when the request or its response is missing, so callers
    can tolerate partially completed rounds without raising.
    """
    round_req = completed_request_map.get(round_req_id)
    if not round_req or not round_req.response:
        return set()
    try:
        phrase_relationship_results = parse_llm_search_response(
            round_req.response.result
        )
        return phrase_relationship_results
    except Exception as e:
        await record_response_parse_error(
            gpt_batch_request=round_req,
            error_message=str(e),
            timestamp=timestamp,
            traceback_str=traceback.format_exc(),
        )
        logger.error(
            f"recursive_search_node.parse_recursive_search_round_result: Error parsing recursive search results for manufacturer {mfg_etld1} from GPT response: {e}"
        )
        raise


async def get_all_recursive_round_results(
    mfg_etld1: str,
    field_type: LLMExtractedFieldTypeEnum,
    chunk_bounds: str,
    extraction_bundle: LLMPhraseExtractionRequestBundle,
    completed_request_map: dict[BatchRequestIDType, GPTBatchRequest],
    timestamp: datetime,
) -> LLMSearchResults:
    """Union of phrases across every recursive round embedded for the chunk."""
    all_results: set[str] = set()
    for round_req_id in extraction_bundle.llm_phrase_recursive_search_req_ids:
        all_results |= await parse_recursive_search_round_result(
            mfg_etld1=mfg_etld1,
            field_type=field_type,
            chunk_bounds=chunk_bounds,
            round_req_id=round_req_id,
            completed_request_map=completed_request_map,
            timestamp=timestamp,
        )
    return all_results


async def build_llm_phrase_search_results(
    mfg_etld1: str,
    field_type: LLMExtractedFieldTypeEnum,
    chunk_bounds: str,
    extraction_bundle: LLMPhraseExtractionRequestBundle,
    completed_search_req_map: dict[BatchRequestIDType, GPTBatchRequest],
    completed_recursive_search_req_map: dict[BatchRequestIDType, GPTBatchRequest],
    timestamp: datetime,
    brute_search_results: Optional[LLMSearchResults] = None,
) -> dict[int, LLMSearchResults]:
    """Build the per-round search results stat.

    Round 0 is reserved for brute-force search survivors (concept-type fields
    only; always empty for keyword-type fields, which have no brute-force
    phase) — specifically, the subset of *brute_search_results* that is NOT
    already a substring of any LLM-found phrase (the same overlap filter used
    to decide what's passed on to phrase_relationship). Round 1 is the first
    LLM search; round N (N>=2) is recursive round N-1. Each round's set is
    independent (NOT cumulative). Returns a dict mapping each search round
    index to the phrases found independently that round.
    """
    first_search_results = await LLMPhraseSearchNode.get_result(
        mfg_etld1=mfg_etld1,
        field_type=field_type,
        chunk_bounds=chunk_bounds,
        extraction_bundle=extraction_bundle,
        completed_request_map=completed_search_req_map,
        timestamp=timestamp,
    )

    rounds: dict[int, LLMSearchResults] = {1: first_search_results}
    for round_index, round_req_id in enumerate(
        extraction_bundle.llm_phrase_recursive_search_req_ids, start=2
    ):
        rounds[round_index] = await parse_recursive_search_round_result(
            mfg_etld1=mfg_etld1,
            field_type=field_type,
            chunk_bounds=chunk_bounds,
            round_req_id=round_req_id,
            completed_request_map=completed_recursive_search_req_map,
            timestamp=timestamp,
        )

    all_llm_phrases: set[str] = set(first_search_results)
    for round_index in range(
        2, 2 + len(extraction_bundle.llm_phrase_recursive_search_req_ids)
    ):
        all_llm_phrases |= rounds[round_index]

    rounds[0] = filter_non_overlapping_brute_results(
        llm_search_results=all_llm_phrases,
        brute_search_results=brute_search_results or set(),
    )

    return rounds


def get_new_phrases_for_latest_round(
    accumulated_before_latest: LLMSearchResults,
    latest_round_results: LLMSearchResults,
) -> LLMSearchResults:
    """Phrases in the latest round that were not already accumulated (case-insensitive)."""
    lowered_accumulated = {p.lower() for p in accumulated_before_latest}
    return {p for p in latest_round_results if p.lower() not in lowered_accumulated}


def _build_recursive_search_context(
    chunk_text: str,
    already_extracted_phrases: LLMSearchResults,
) -> str:
    return (
        f"scraped text:\n{chunk_text}\n\n"
        f"already extracted phrases:\n{list(already_extracted_phrases)}"
    )


async def create_missing_phrase_recursive_search_requests(
    timestamp: datetime,
    field_type: LLMExtractedFieldTypeEnum,  # used for logging and debugging
    missing_recursive_search_req_ids: set[BatchRequestIDType],
    chunked_request_map: LLMPhraseExtractionRequestMap,
    mfg_etld1: str,
    mfg_text: str,
    recursive_search_prompt: Prompt,
    first_search_gpt_request_map: dict[BatchRequestIDType, GPTBatchRequest],
    completed_recursive_search_req_map: dict[BatchRequestIDType, GPTBatchRequest],
    llm_model: LLM_Model,
    model_params: GPTModelParams,
    eager: bool,
    BATCH_SIZE=100,
) -> list[GPTBatchRequest]:
    """Create GPT batch requests for the missing recursive search rounds.

    For each missing round the exclusion set shown to the LLM is the compounding
    union of the first search results and every prior recursive round for that chunk
    (LLM phrases only; brute-force results are intentionally excluded).
    """
    logger.info(
        f"create_missing_phrase_recursive_search_requests: Generating GPTBatchRequests for {mfg_etld1}:{field_type.name}"
    )

    batch_requests: list[GPTBatchRequest] = []

    chunk_round_items: list[tuple[str, LLMPhraseExtractionRequestBundle, int]] = []
    for chunk_bounds, extraction_bundle in chunked_request_map.items():
        for round_index, round_req_id in enumerate(
            extraction_bundle.llm_phrase_recursive_search_req_ids
        ):
            if round_req_id in missing_recursive_search_req_ids:
                chunk_round_items.append((chunk_bounds, extraction_bundle, round_index))

    for i in range(0, len(chunk_round_items), BATCH_SIZE):
        batch = chunk_round_items[i : i + BATCH_SIZE]

        for chunk_bounds, extraction_bundle, round_index in batch:
            round_req_id = extraction_bundle.llm_phrase_recursive_search_req_ids[
                round_index
            ]

            # First search results for this chunk (LLM only).
            first_search_results = await LLMPhraseSearchNode.get_result(
                mfg_etld1=mfg_etld1,
                field_type=field_type,
                chunk_bounds=chunk_bounds,
                extraction_bundle=extraction_bundle,
                completed_request_map=first_search_gpt_request_map,
                timestamp=timestamp,
            )

            # Compounding union of every prior recursive round for this chunk.
            already_extracted: set[str] = set(first_search_results)
            for (
                prior_round_req_id
            ) in extraction_bundle.llm_phrase_recursive_search_req_ids[:round_index]:
                already_extracted |= await parse_recursive_search_round_result(
                    mfg_etld1=mfg_etld1,
                    field_type=field_type,
                    chunk_bounds=chunk_bounds,
                    round_req_id=prior_round_req_id,
                    completed_request_map=completed_recursive_search_req_map,
                    timestamp=timestamp,
                )

            start, end = int(chunk_bounds.split(":")[0]), int(
                chunk_bounds.split(":")[1]
            )
            context = _build_recursive_search_context(
                chunk_text=mfg_text[start:end],
                already_extracted_phrases=already_extracted,
            )

            recursive_batch_request = create_base_gpt_batch_request(
                deferred_at=timestamp,
                etld1=mfg_etld1,
                custom_id=round_req_id,
                context=context,
                prompt_text=recursive_search_prompt.text,
                gpt_model=llm_model,
                model_params=model_params.with_response_format(
                    LLM_SEARCH_RESPONSE_SCHEMA
                ),
                batch_id="Eager" if eager else None,
            )
            batch_requests.append(recursive_batch_request)

        # Yield control to the event loop after each batch.
        await asyncio.sleep(0)

        if (i + BATCH_SIZE) % 500 == 0:
            logger.info(
                f"Created {min(i + BATCH_SIZE, len(chunk_round_items))}/{len(chunk_round_items)} "
                f"recursive search gpt requests for {mfg_etld1}:{field_type.name} (Eager: {eager})"
            )

    return batch_requests
