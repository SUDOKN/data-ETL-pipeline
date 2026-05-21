import logging
from abc import abstractmethod
from datetime import datetime

from core.models.db.manufacturer import Manufacturer
from core.models.db.deferred_manufacturer import DeferredManufacturer

from data_etl_app.models.chunking_strat import ChunkingStrat
from data_etl_app.models.pipeline_nodes.base_node import (
    BaseNode,
    LLMExtractedFieldTypeVar,
)
from data_etl_app.models.pipeline_nodes.llm_extraction_node import (
    LLMExtractionNode,
)
from data_etl_app.models.types_and_enums import (
    LLMExtractedFieldTypeVar,
    PipelineContext,
)
from scraper_app.models.scraped_text_file import ScrapedTextFile
from open_ai_key_app.models.gpt_model import LLM_Model

logger = logging.getLogger(__name__)


class PrefillNode(BaseNode[LLMExtractedFieldTypeVar]):
    def __init__(
        self,
        field_type: LLMExtractedFieldTypeVar,
        llm_model: LLM_Model,
        chunk_strategy: ChunkingStrat,
        next_node: LLMExtractionNode,
    ) -> None:
        super().__init__(field_type=field_type, next_node=next_node)
        self.llm_model = llm_model
        self.chunk_strategy = chunk_strategy

    @abstractmethod
    async def execute(
        self,
        mfg: Manufacturer,
        deferred_mfg: DeferredManufacturer,
        scraped_text_file: ScrapedTextFile,
        timestamp: datetime,
        pipeline_context: PipelineContext,
        eager: bool,  # if True, dispatch all batch requests immediately and then check for completion, basically a sync execution of the entire phase
    ) -> None:
        pass
