from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING


from core.models.db.manufacturer import Manufacturer
from core.models.db.deferred_manufacturer import DeferredManufacturer
from core.models.deferred_extraction.deferred_single_stage_extraction_requests import (
    SingleStageExtractionRequestBundle,
    DeferredSingleStageExtractionRequests,
)
from core.models.file_objects.prompt import Prompt
from core.models.extraction_results.single_stage_extraction_results import (
    LLMSingleStageExtractionMetadata,
)
from data_etl_app.models.chunking_strat import ChunkingStrategy
from data_etl_app.models.pipeline_nodes.base.base_node import (
    PipelineContext,
)
from data_etl_app.models.pipeline_nodes.base.base_prefill_node import PrefillNode
from data_etl_app.models.pipeline_nodes.single_stage.basic_fields.single_stage_extraction_node import (
    SingleStageExtractionNode,
)
from data_etl_app.models.types_and_enums import (
    SingleStageFieldTypeVar,
)

if TYPE_CHECKING:
    from scraper_app.models.scraped_text_file import ScrapedTextFile


from data_etl_app.utils.chunk_util import get_chunks_respecting_line_boundaries

logger = logging.getLogger(__name__)


class SingleStageExtractionPrefillNode(PrefillNode[SingleStageFieldTypeVar]):
    """Base class for single phase of extraction."""

    next_node: SingleStageExtractionNode

    def __init__(
        self,
        field_type: SingleStageFieldTypeVar,
        next_node: SingleStageExtractionNode,
        extraction_metadata: LLMSingleStageExtractionMetadata,
        chunk_strategy: ChunkingStrategy,
        prompt: Prompt,
    ):
        super().__init__(
            field_type=field_type,
            chunk_strategy=chunk_strategy,
            next_node=next_node,
        )
        self.prompt: Prompt = prompt
        self.extraction_metadata = extraction_metadata

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
                soft_limit_tokens=self.chunk_strategy.max_tokens_per_chunk,
                overlap_ratio=self.chunk_strategy.overlap,
                max_chunks=self.chunk_strategy.max_chunks,
                llm_model=self.extraction_metadata.single_stage.llm_model,
            )

            deferred_basic_extraction = DeferredSingleStageExtractionRequests(
                metadata=self.extraction_metadata,
                chunked_request_map={
                    chunk_bounds: SingleStageExtractionRequestBundle(
                        llm_request_id=None,
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
            eager=eager,
        )
