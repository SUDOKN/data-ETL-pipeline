from __future__ import (
    annotations,
)  # This allows you to write self-referential types without quotes, because type annotations are no longer evaluated at function/class definition time — they're stored as strings automatically.

import asyncio
import logging
from datetime import datetime

from core.models.prompt import Prompt

from data_etl_app.models.deferred_keyword_extraction import (
    DeferredKeywordExtraction,
    DeferredKeywordExtractionStats,
)
from data_etl_app.services.llm_powered.search.llm_search_service import (
    llm_search_deferred,
)
from data_etl_app.services.knowledge.prompt_service import get_prompt_service
from data_etl_app.utils.chunk_util import (
    ChunkingStrat,
    get_chunks_respecting_line_boundaries,
)

from open_ai_key_app.models.db.gpt_batch_request import GPTBatchRequest

logger = logging.getLogger(__name__)

from open_ai_key_app.models.gpt_model import (
    GPTModel,
    GPT_4o_mini,
    ModelParameters,
    DefaultModelParameters,
)


async def extract_products_deferred(
    deferred_at: datetime,
    mfg_etld1: str,
    mfg_text: str,
) -> tuple[DeferredKeywordExtraction, list[GPTBatchRequest]]:
    """
    Extract products for a manufacturer's text.
    """

    prompt_service = await get_prompt_service()
    return await _extract_keyword_data_deferred(
        deferred_at,
        mfg_etld1=mfg_etld1,
        keyword_type="products",
        text=mfg_text,
        search_prompt=prompt_service.extract_any_product_prompt,
        chunk_strategy=ChunkingStrat(overlap=0.15, max_tokens=5000),
        gpt_model=GPT_4o_mini,
        model_params=DefaultModelParameters,
    )


async def _extract_keyword_data_deferred(
    deferred_at: datetime,
    mfg_etld1: str,
    keyword_type: str,  # used for logging/debug
    text: str,
    search_prompt: Prompt,
    chunk_strategy: ChunkingStrat,
    gpt_model: GPTModel = GPT_4o_mini,
    model_params: ModelParameters = DefaultModelParameters,
) -> tuple[DeferredKeywordExtraction, list[GPTBatchRequest]]:

    logger.info(
        f"_extract_keyword_data_deferred: Generating GPTBatchRequest for {mfg_etld1}:{keyword_type}"
    )

    # 1) Chunk
    chunk_map = await get_chunks_respecting_line_boundaries(
        text, chunk_strategy.max_tokens, chunk_strategy.overlap
    )

    num_chunks = len(chunk_map)
    logger.debug(f"{mfg_etld1}:{keyword_type}: Processing {num_chunks} chunks")

    # 2) Process chunks in batches to avoid overwhelming event loop
    # Process 10 chunks at a time to balance concurrency and memory

    CHUNK_BATCH_SIZE = 10
    chunk_items = list(chunk_map.items())
    chunk_batch_request_map = {}
    batch_requests: list[GPTBatchRequest] = []

    for i in range(0, num_chunks, CHUNK_BATCH_SIZE):
        batch_items = chunk_items[i : i + CHUNK_BATCH_SIZE]

        # Process this batch of chunks concurrently
        tasks = [
            llm_search_deferred(
                deferred_at=deferred_at,
                custom_id=f"{mfg_etld1}>{keyword_type}>chunk>{b}",
                text=t,
                prompt=search_prompt,
                gpt_model=gpt_model,
                model_params=model_params,
            )
            for b, t in batch_items
        ]

        # Wait for this batch to complete
        results = await asyncio.gather(*tasks)

        # Collect results
        for custom_id, batch_request in results:
            # Extract chunk bounds from custom_id (format: "etld1>keyword>chunk>start:end")
            chunk_bounds = custom_id.split(">")[-1]
            chunk_batch_request_map[chunk_bounds] = custom_id
            batch_requests.append(batch_request)

        # Yield control to event loop between batches to allow other manufacturers to run
        if i + CHUNK_BATCH_SIZE < num_chunks:
            await asyncio.sleep(0)

    logger.info(
        f"{mfg_etld1}:{keyword_type}: Generated {len(batch_requests)} batch requests"
    )

    return (
        DeferredKeywordExtraction(
            deferred_stats=DeferredKeywordExtractionStats(
                extract_prompt_version_id=search_prompt.s3_version_id,
                chunk_batch_request_id_map=chunk_batch_request_map,
            )
        ),
        batch_requests,
    )
