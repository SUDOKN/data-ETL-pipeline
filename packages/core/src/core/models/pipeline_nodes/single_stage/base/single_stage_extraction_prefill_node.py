from __future__ import annotations

import logging
from datetime import datetime

from core.models.extraction_subject import (
    AbstractExtractionSubject,
    AbstractDeferredExtractionSubject,
)
from core.models.deferred_extraction.deferred_single_stage_extraction_requests import (
    SingleStageExtractionRequestBundle,
    DeferredSingleStageExtractionRequests,
)
from llm_providers.models.file_objects.prompt import Prompt
from core.models.extraction_results.single_stage_extraction_results import (
    LLMSingleStageExtractionMetadata,
)
from core.models.chunking_strat import ChunkingStrategy
from core.models.pipeline_nodes.base.base_node import (
    PipelineContext,
)
from core.models.pipeline_nodes.base.base_prefill_node import (
    PrefillNode,
)
from core.models.pipeline_nodes.single_stage.base.single_stage_extraction_node import (
    SingleStageExtractionNode,
)
from core.models.field_types import (
    SingleStageFieldTypeVar,
)
from scraper.models.s3.scraped_text_file import ScrapedTextFile


from llm_providers.utils.chunk_util import (
    get_chunks_respecting_line_boundaries,
)

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
        subject: AbstractExtractionSubject,
        deferred_subject: AbstractDeferredExtractionSubject,
        scraped_text_file: ScrapedTextFile,
        timestamp: datetime,
        pipeline_context: PipelineContext,
        eager: bool,
    ):
        if not bool(getattr(deferred_subject, self.field_type.name)):
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
            setattr(deferred_subject, self.field_type.name, deferred_basic_extraction)
            await deferred_subject.save()
        else:
            deferred_basic_extraction: DeferredSingleStageExtractionRequests = getattr(
                deferred_subject, self.field_type.name
            )
            # Single-stage fields carry a prompt pin like every other stage, and
            # went unchecked until 2026-08-13 — so a resumed business_desc or
            # is_manufacturer replayed under an edited prompt with no signal at
            # all, not even the partial one the multi-stage guard gave.
            self.raise_if_metadata_is_stale(
                subject_unique_id=subject.subject_unique_id,
                stored_metadata_dump=deferred_basic_extraction.metadata.model_dump(),
                latest_metadata_dump=self.extraction_metadata.model_dump(),
            )

        await self.next_node.execute(
            subject=subject,
            deferred_subject=deferred_subject,
            scraped_text_file=scraped_text_file,
            timestamp=timestamp,
            pipeline_context=pipeline_context,
            eager=eager,
        )
