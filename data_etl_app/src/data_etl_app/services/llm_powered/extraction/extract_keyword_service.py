from __future__ import (
    annotations,
)  # This allows you to write self-referential types without quotes, because type annotations are no longer evaluated at function/class definition time — they're stored as strings automatically.
import asyncio
import logging
from datetime import datetime

from core.models.prompt import Prompt

from core.models.keyword_extraction_results import (
    KeywordExtractionChunkStats,
    KeywordExtractionResults,
    KeywordExtractionStats,
)
from data_etl_app.services.llm_powered.search.llm_search_service import llm_search
from data_etl_app.services.knowledge.prompt_service import get_prompt_service
from data_etl_app.services.chunking_strat import PRODUCT_CHUNKING_STRAT, ChunkingStrat
from data_etl_app.utils.chunk_util import (
    get_chunks_respecting_line_boundaries,
)

logger = logging.getLogger(__name__)

from open_ai_key_app.models.gpt_model import (
    GPTModel,
    GPT_4o_mini,
    ModelParameters,
    DefaultModelParameters,
)


async def extract_products(
    extraction_timestamp: datetime,
    mfg_etld1: str,
    text: str,
) -> KeywordExtractionResults:
    """
    Extract products for a manufacturer's text.
    """
    prompt_service = await get_prompt_service()
    return await _extract_keyword_data(
        extraction_timestamp,
        "products",
        mfg_etld1,
        text,
        prompt_service.extract_any_product_prompt,
        PRODUCT_CHUNKING_STRAT,
        gpt_model=GPT_4o_mini,
        model_params=DefaultModelParameters,
    )


async def _extract_keyword_data(
    extraction_timestamp: datetime,
    keyword_type: str,  # used for logging/debug
    mfg_etld1: str,
    text: str,
    search_prompt: Prompt,
    chunk_strategy: ChunkingStrat,
    gpt_model: GPTModel = GPT_4o_mini,
    model_params: ModelParameters = DefaultModelParameters,
) -> KeywordExtractionResults:
    logger.info(
        f"Extracting {keyword_type} (NO BRUTE) for {mfg_etld1} at {extraction_timestamp} "
    )

    # 1) Chunk
    chunk_map = await get_chunks_respecting_line_boundaries(
        text=text,
        soft_limit_tokens=chunk_strategy.max_tokens,
        overlap_ratio=chunk_strategy.overlap,
        max_chunks=chunk_strategy.max_chunks,
    )

    # 2) LLM search per chunk (no brute)
    async def _process_chunk(bounds: str, text_chunk: str):
        chunk_result = await llm_search(
            text_chunk,
            search_prompt.text,
            gpt_model,
            model_params,
            True,  # dedupe/normalize
        )
        return bounds, chunk_result

    tasks = [asyncio.create_task(_process_chunk(b, t)) for b, t in chunk_map.items()]
    chunk_results = await asyncio.gather(*tasks)

    final_result_set: set[str] = set()
    stats = KeywordExtractionStats(
        extract_prompt_version_id=search_prompt.s3_version_id,
        chunked_stats={},  # per-chunk stats
    )

    # Seed per-chunk stats (brute is always empty)
    for bounds, chunk_result in chunk_results:
        stats.chunked_stats[bounds] = KeywordExtractionChunkStats(results=chunk_result)
        final_result_set |= chunk_result  # Aggregate to final results

    return KeywordExtractionResults(
        extracted_at=extraction_timestamp,
        results=final_result_set,
        stats=stats,
    )
