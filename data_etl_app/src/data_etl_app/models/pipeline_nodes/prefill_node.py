import logging
from abc import abstractmethod
from datetime import datetime

from core.models.db.manufacturer import Manufacturer
from core.models.db.deferred_manufacturer import DeferredManufacturer

from data_etl_app.models.chunking_strat import ChunkingStrategy
from data_etl_app.models.pipeline_nodes.base_node import (
    BaseNode,
    PipelineContext,
    LLMExtractedFieldTypeVar,
)
from data_etl_app.models.pipeline_nodes.llm_extraction_node import (
    LLMExtractionNode,
)
from data_etl_app.models.types_and_enums import (
    LLMExtractedFieldTypeVar,
)
from scraper_app.models.scraped_text_file import ScrapedTextFile
from litellm_proxy_app.models.llm_model import LLM_Model
from open_ai_key_app.models.gpt_model_params import GPTModelParams

logger = logging.getLogger(__name__)


class PrefillNode(BaseNode[LLMExtractedFieldTypeVar]):
    def __init__(
        self,
        field_type: LLMExtractedFieldTypeVar,
        chunk_strategy: ChunkingStrategy,
        next_node: LLMExtractionNode,
    ) -> None:
        super().__init__(field_type=field_type, next_node=next_node)
        self.chunk_strategy = chunk_strategy

    @abstractmethod
    async def execute(
        self,
        mfg: Manufacturer,
        deferred_mfg: DeferredManufacturer,
        scraped_text_file: ScrapedTextFile,
        timestamp: datetime,
        pipeline_context: PipelineContext,
        llm_model: LLM_Model,
        model_params: GPTModelParams,
        eager: bool,  # if True, dispatch all batch requests immediately and then check for completion, basically a sync execution of the entire phase
    ) -> None:
        pass
