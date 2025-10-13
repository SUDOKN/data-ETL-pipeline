from __future__ import (
    annotations,
)  # This allows you to write self-referential types without quotes, because type annotations are no longer evaluated at function/class definition time — they're stored as strings automatically.
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
) -> DeferredKeywordExtraction:
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
) -> DeferredKeywordExtraction:
    logger.info(
        f"_extract_keyword_data_deferred: Generating GPTBatchRequest for {mfg_etld1}:{keyword_type}"
    )

    # 1) Chunk
    chunk_map = get_chunks_respecting_line_boundaries(
        text, chunk_strategy.max_tokens, chunk_strategy.overlap
    )

    # 2) LLM search per chunk (no brute)
    chunk_batch_request_map = {
        b: await llm_search_deferred(
            deferred_at=deferred_at,
            custom_id=f"{mfg_etld1}>{keyword_type}>chunk>{b}",
            text=t,
            prompt=search_prompt,
            gpt_model=gpt_model,
            model_params=model_params,
        )
        for b, t in chunk_map.items()
    }

    return DeferredKeywordExtraction(
        deferred_stats=DeferredKeywordExtractionStats(
            extract_prompt_version_id=search_prompt.s3_version_id,
            chunk_batch_request_id_map=chunk_batch_request_map,
        )
    )
