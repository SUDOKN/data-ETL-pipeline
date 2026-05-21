import logging
from datetime import datetime

from core.models.search_stage_results import (
    SearchStageMetadata,
)
from core.models.db.manufacturer import Manufacturer
from core.models.db.deferred_manufacturer import DeferredManufacturer
from core.models.deferred_search_requests import (
    DeferredSearchRequests,
    SearchRequestBundle,
)
from core.models.prompt import Prompt
from data_etl_app.models.pipeline_nodes.base_node import (
    BaseNode,
)
from data_etl_app.models.pipeline_nodes.keyword.keyword_search_node import (
    KeywordSearchNode,
)
from data_etl_app.models.pipeline_nodes.prefill_node import PrefillNode
from data_etl_app.models.types_and_enums import KeywordTypeEnum, PipelineContext
from data_etl_app.models.chunking_strat import ChunkingStrat
from open_ai_key_app.models.gpt_model import LLM_Model
from scraper_app.models.scraped_text_file import ScrapedTextFile


from data_etl_app.utils.chunk_util import get_chunks_respecting_line_boundaries

logger = logging.getLogger(__name__)


class KeywordExtractionPrefillNode(PrefillNode[KeywordTypeEnum]):
    next_node: KeywordSearchNode

    def __init__(
        self,
        field_type: KeywordTypeEnum,
        llm_model: LLM_Model,
        chunk_strategy: ChunkingStrat,
        search_prompt: Prompt,
        next_node: KeywordSearchNode,
    ):
        super().__init__(
            field_type=field_type,
            llm_model=llm_model,
            chunk_strategy=chunk_strategy,
            next_node=next_node,
        )
        self.search_prompt: Prompt = search_prompt

    async def execute(
        self,
        mfg: Manufacturer,
        deferred_mfg: DeferredManufacturer,
        scraped_text_file: ScrapedTextFile,
        timestamp: datetime,
        pipeline_context: PipelineContext,
        eager: bool,
    ):
        if not bool(getattr(deferred_mfg, self.field_type.name)):
            chunk_map = await get_chunks_respecting_line_boundaries(
                text=scraped_text_file.text,
                soft_limit_tokens=self.chunk_strategy.max_tokens,
                overlap_ratio=self.chunk_strategy.overlap,
                max_chunks=self.chunk_strategy.max_chunks,
            )

            deferred_keyword_extraction = DeferredSearchRequests(
                metadata=SearchStageMetadata(
                    model=self.llm_model.model_name,
                    created_at=timestamp,
                    search_prompt_version_id=self.search_prompt.s3_version_id,
                ),
                request_map={
                    chunk_bounds: SearchRequestBundle(
                        llm_search_request_id=KeywordSearchNode.get_request_custom_id(
                            mfg_etld1=deferred_mfg.etld1,
                            field_type=self.field_type,
                            chunk_bounds=chunk_bounds,
                        ),
                    )
                    for chunk_bounds, _chunk_text in chunk_map.items()
                },
            )
            setattr(deferred_mfg, self.field_type.name, deferred_keyword_extraction)
            await deferred_mfg.save()

        await self.next_node.execute(
            mfg=mfg,
            deferred_mfg=deferred_mfg,
            scraped_text_file=scraped_text_file,
            timestamp=timestamp,
            pipeline_context=pipeline_context,
            eager=eager,
        )
