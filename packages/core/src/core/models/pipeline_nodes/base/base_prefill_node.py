from __future__ import annotations

import logging
from abc import abstractmethod
from datetime import datetime

from packages.core.src.core.models.base.extraction_subject import (
    AbstractExtractionSubject,
    AbstractDeferredExtractionSubject,
)

from packages.core.src.core.models.chunking_strat import ChunkingStrategy
from packages.core.src.core.models.pipeline_nodes.base.base_node import (
    BaseNode,
    PipelineContext,
    ResultT,
)
from packages.core.src.core.models.pipeline_nodes.base.base_llm_extraction_node import (
    BaseLLMExtractionNode,
)
from packages.core.src.core.models.types_and_enums import (
    LLMExtractedFieldTypeVar,
)
from apps.data_etl_app.src.data_etl_app.models.scraped_text_file import ScrapedTextFile

logger = logging.getLogger(__name__)


class PrefillNode(BaseNode[LLMExtractedFieldTypeVar, None]):
    next_node: BaseLLMExtractionNode

    def __init__(
        self,
        field_type: LLMExtractedFieldTypeVar,
        chunk_strategy: ChunkingStrategy,
        next_node: BaseLLMExtractionNode,
    ) -> None:
        super().__init__(field_type=field_type, next_node=next_node)
        self.chunk_strategy = chunk_strategy

    @abstractmethod
    async def execute(
        self,
        subject: AbstractExtractionSubject,
        deferred_subject: AbstractDeferredExtractionSubject,
        scraped_text_file: ScrapedTextFile,
        timestamp: datetime,
        pipeline_context: PipelineContext,
        eager: bool,  # if True, dispatch all batch requests immediately and then check for completion, basically a sync execution of the entire phase
    ) -> None:
        await self.next_node.execute(
            subject=subject,
            deferred_subject=deferred_subject,
            scraped_text_file=scraped_text_file,
            pipeline_context=pipeline_context,
            timestamp=timestamp,
            eager=eager,
        )
