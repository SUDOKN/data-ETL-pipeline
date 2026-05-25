import logging
from datetime import datetime


from core.models.db.manufacturer import Manufacturer
from core.models.db.deferred_manufacturer import DeferredManufacturer
from core.models.deferred_single_stage_extraction_requests import (
    SingleStageExtractionRequestBundle,
    DeferredSingleStageExtractionRequests,
)
from core.models.prompt import Prompt
from core.models.single_stage_extraction_results import SingleStageMetadata
from data_etl_app.models.chunking_strat import ChunkingStrategy
from data_etl_app.models.pipeline_nodes.base_node import (
    PipelineContext,
)
from data_etl_app.models.pipeline_nodes.prefill_node import PrefillNode
from data_etl_app.models.pipeline_nodes.single_stage_extraction_node import (
    SingleStageExtractionNode,
)
from data_etl_app.models.types_and_enums import (
    SingleStageFieldTypeVar,
)
from open_ai_key_app.models.llm_model import LLM_Model
from open_ai_key_app.models.gpt_model_params import GPTModelParams
from scraper_app.models.scraped_text_file import ScrapedTextFile


from data_etl_app.utils.chunk_util import get_chunks_respecting_line_boundaries

logger = logging.getLogger(__name__)


class SingleStageExtractionPrefillNode(PrefillNode[SingleStageFieldTypeVar]):
    """Base class for single phase of extraction."""

    next_node: SingleStageExtractionNode

    def __init__(
        self,
        field_type: SingleStageFieldTypeVar,
        chunk_strategy: ChunkingStrategy,
        prompt: Prompt,
        next_node: SingleStageExtractionNode,
    ):
        super().__init__(
            field_type=field_type,
            chunk_strategy=chunk_strategy,
            next_node=next_node,
        )
        self.prompt: Prompt = prompt

    async def execute(
        self,
        mfg: Manufacturer,
        deferred_mfg: DeferredManufacturer,
        scraped_text_file: ScrapedTextFile,
        timestamp: datetime,
        pipeline_context: PipelineContext,
        llm_model: LLM_Model,
        model_params: GPTModelParams,
        eager: bool,
    ):
        if not bool(getattr(deferred_mfg, self.field_type.name)):
            chunk_map = await get_chunks_respecting_line_boundaries(
                text=scraped_text_file.text,
                soft_limit_tokens=self.chunk_strategy.max_tokens_per_chunk,
                overlap_ratio=self.chunk_strategy.overlap,
                max_chunks=self.chunk_strategy.max_chunks,
                llm_model=llm_model,
            )

            deferred_basic_extraction = DeferredSingleStageExtractionRequests(
                metadata=SingleStageMetadata(
                    created_at=timestamp,
                    model=llm_model.model_name,
                    model_params=model_params,
                    chunk_strat=self.chunk_strategy,
                    prompt_version_id=self.prompt.s3_version_id,
                ),
                request_map={
                    chunk_bounds: SingleStageExtractionRequestBundle(
                        llm_request_id=SingleStageExtractionNode.get_request_custom_id(
                            mfg_etld1=deferred_mfg.etld1,
                            field_type=self.field_type,
                            chunk_bounds=chunk_bounds,
                            llm_model=llm_model,
                            model_params=model_params,
                        ),
                    )
                    for chunk_bounds, _chunk_text in chunk_map.items()
                },
            )
            setattr(deferred_mfg, self.field_type.name, deferred_basic_extraction)
            await deferred_mfg.save()

        await self.next_node.execute(
            mfg=mfg,
            deferred_mfg=deferred_mfg,
            scraped_text_file=scraped_text_file,
            timestamp=timestamp,
            pipeline_context=pipeline_context,
            llm_model=llm_model,
            model_params=model_params,
            eager=eager,
        )
