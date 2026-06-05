import logging

from core.models.prompt import Prompt
from data_etl_app.models.pipeline_nodes.basic_field.business_desc_extraction_node import (
    BusinessDescExtractionNode,
)
from data_etl_app.models.pipeline_nodes.single_stage_extraction_prefill_node import (
    SingleStageExtractionPrefillNode,
)
from data_etl_app.models.types_and_enums import (
    BasicFieldTypeEnum,
)
from data_etl_app.models.chunking_strat import ChunkingStrategy
from litellm_proxy_app.models.llm_model import LLM_Model

logger = logging.getLogger(__name__)


class BusinessDescPrefillNode(SingleStageExtractionPrefillNode):

    def __init__(
        self,
        chunk_strategy: ChunkingStrategy,
        prompt: Prompt,
        next_node: BusinessDescExtractionNode,
    ):
        super().__init__(
            field_type=BasicFieldTypeEnum.business_desc,
            chunk_strategy=chunk_strategy,
            prompt=prompt,
            next_node=next_node,
        )
