from __future__ import (
    annotations,
)  # This allows you to write self-referential types without quotes, because type annotations are no longer evaluated at function/class definition time — they're stored as strings automatically.

import asyncio
import logging
from datetime import datetime
from typing import Optional

from core.models.field_types import MfgETLDType
from core.models.prompt import Prompt

from core.models.deferred_keyword_extraction import (
    DeferredKeywordExtraction,
)
from data_etl_app.models.types_and_enums import KeywordTypeEnum
from data_etl_app.services.knowledge.prompt_service import get_prompt_service
from data_etl_app.utils.chunk_util import (
    ChunkingStrat,
    get_chunks_respecting_line_boundaries,
)

from core.models.db.gpt_batch_request import GPTBatchRequest
from core.services.gpt_batch_request_service import (
    create_gpt_batch_request,
    find_gpt_batch_request_ids_only,
)

logger = logging.getLogger(__name__)

from open_ai_key_app.models.gpt_model import (
    GPTModel,
    GPT_4o_mini,
    ModelParameters,
    DefaultModelParameters,
)


async def get_missing_keyword_search_requests(
    deferred_at: datetime,
    keyword_type: KeywordTypeEnum,
    deferred_keyword_extraction: Optional[DeferredKeywordExtraction],
    mfg_etld1: str,
    mfg_text: str,
) -> tuple[DeferredKeywordExtraction, list[GPTBatchRequest]]:
    """Add mapping requests for the given concept type to the deferred concept extraction stats."""
    keyword_search_functions = {
        KeywordTypeEnum.products: get_missing_product_search_requests,
    }

    keyword_search_function = keyword_search_functions.get(keyword_type)
    if not keyword_search_function:
        raise ValueError(f"Unsupported keyword type: {keyword_type}")

    return await keyword_search_function(
        deferred_at=deferred_at,
        deferred_keyword_extraction=deferred_keyword_extraction,
        mfg_etld1=mfg_etld1,
        mfg_text=mfg_text,
    )


async def get_missing_product_search_requests(
    deferred_at: datetime,
    deferred_keyword_extraction: Optional[DeferredKeywordExtraction],
    mfg_etld1: MfgETLDType,
    mfg_text: str,
) -> tuple[DeferredKeywordExtraction, list[GPTBatchRequest]]:
    """
    Extract products for a manufacturer's text.
    """

    prompt_service = await get_prompt_service()
    return await _get_keyword_search_requests(
        deferred_at,
        mfg_etld1=mfg_etld1,
        mfg_text=mfg_text,
        deferred_keyword_extraction=deferred_keyword_extraction,
        keyword_type=KeywordTypeEnum.products.name,
        search_prompt=prompt_service.extract_any_product_prompt,
        chunk_strategy=ChunkingStrat(overlap=0.15, max_tokens=5000),
        gpt_model=GPT_4o_mini,
        model_params=DefaultModelParameters,
    )


async def _get_keyword_search_requests(
    deferred_at: datetime,
    mfg_etld1: MfgETLDType,
    mfg_text: str,
    deferred_keyword_extraction: Optional[DeferredKeywordExtraction],
    keyword_type: str,  # used for logging/debug
    search_prompt: Prompt,
    chunk_strategy: ChunkingStrat,
    gpt_model: GPTModel = GPT_4o_mini,
    model_params: ModelParameters = DefaultModelParameters,
) -> tuple[DeferredKeywordExtraction, list[GPTBatchRequest]]:
    logger.info(
        f"_get_missing_keyword_search_requests: Generating GPTBatchRequest for {mfg_etld1}:{keyword_type}"
    )

    batch_requests: list[GPTBatchRequest] = []

    if not deferred_keyword_extraction:
        # if not, create fresh DeferredKeywordExtraction
        deferred_keyword_extraction = DeferredKeywordExtraction(
            extract_prompt_version_id=search_prompt.s3_version_id,
            chunk_request_id_map={},
        )

        # expensive operation for large texts
        chunk_map = await get_chunks_respecting_line_boundaries(
            mfg_text, chunk_strategy.max_tokens, chunk_strategy.overlap
        )
        chunk_items = list(chunk_map.items())

    else:
        if (
            not deferred_keyword_extraction.extract_prompt_version_id
            == search_prompt.s3_version_id
        ):
            raise ValueError(
                f"_extract_concept_data_deferred: Prompt version mismatch for {mfg_etld1}:{keyword_type}, deferred_keyword_extraction.extract_prompt_version_id={deferred_keyword_extraction.extract_prompt_version_id} != search_prompt.s3_version_id={search_prompt.s3_version_id}"
            )

        # if yes lookup all chunk batch requests IDs inside chunk_request_id_map
        llm_search_req_ids_to_lookup = set()

        for (
            _chunk_bounds,
            llm_search_request_id,
        ) in deferred_keyword_extraction.chunk_request_id_map.items():
            llm_search_req_ids_to_lookup.add(llm_search_request_id)

        gpt_req_ids_missing = llm_search_req_ids_to_lookup - (
            await find_gpt_batch_request_ids_only(list(llm_search_req_ids_to_lookup))
        )

        # create chunk_items only for missing batch requests
        chunk_items = []
        for (
            chunk_bounds,
            llm_search_request_id,
        ) in deferred_keyword_extraction.chunk_request_id_map.items():
            if llm_search_request_id in gpt_req_ids_missing:
                start = chunk_bounds.split(":")[0]
                end = chunk_bounds.split(":")[1]
                chunk_items.append((chunk_bounds, mfg_text[int(start) : int(end)]))

    # Process chunks in batches to yield control periodically
    BATCH_SIZE = 100  # Process 100 chunks at a time

    for i in range(0, len(chunk_items), BATCH_SIZE):
        batch = chunk_items[i : i + BATCH_SIZE]

        # Process current batch
        for chunk_bounds, chunk_text in batch:
            custom_id = f"{mfg_etld1}>{keyword_type}>llm_search>chunk>{chunk_bounds}"
            llm_batch_request = create_gpt_batch_request(
                deferred_at=deferred_at,
                custom_id=custom_id,
                text=chunk_text,
                prompt=search_prompt,
                gpt_model=gpt_model,
                model_params=model_params,
            )

            batch_requests.append(llm_batch_request)
            deferred_keyword_extraction.chunk_request_id_map[chunk_bounds] = custom_id

        # Yield control to event loop after each batch
        await asyncio.sleep(0)

        if (i + BATCH_SIZE) % 500 == 0:
            logger.info(
                f"Created {min(i + BATCH_SIZE, len(chunk_items))}/{len(chunk_items)} "
                f"gpt request for {mfg_etld1}:{keyword_type}"
            )

    return (
        deferred_keyword_extraction,
        batch_requests,
    )
