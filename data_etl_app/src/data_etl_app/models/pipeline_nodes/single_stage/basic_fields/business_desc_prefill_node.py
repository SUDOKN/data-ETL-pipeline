import logging

from core.models.prompt import Prompt
from core.models.single_stage_extraction_results import LLMSingleStageExtractionMetadata
from data_etl_app.models.pipeline_nodes.single_stage.basic_fields.business_desc_extraction_node import (
    BusinessDescExtractionNode,
)
from data_etl_app.models.pipeline_nodes.single_stage.basic_fields.single_stage_extraction_prefill_node import (
    SingleStageExtractionPrefillNode,
)
from data_etl_app.models.types_and_enums import (
    BasicFieldTypeEnum,
)
from data_etl_app.models.chunking_strat import ChunkingStrategy
from core.models.llm_model import LLM_Model

logger = logging.getLogger(__name__)


class BusinessDescPrefillNode(SingleStageExtractionPrefillNode):

    def __init__(
        self,
        chunk_strategy: ChunkingStrategy,
        next_node: BusinessDescExtractionNode,
        prompt: Prompt,
        business_desc_extraction_metadata: LLMSingleStageExtractionMetadata,
    ):
        super().__init__(
            field_type=BasicFieldTypeEnum.business_desc,
            chunk_strategy=chunk_strategy,
            prompt=prompt,
            next_node=next_node,
            extraction_metadata=business_desc_extraction_metadata,
        )
