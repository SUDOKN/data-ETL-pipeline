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
from data_etl_app.models.chunking_strat import ChunkingStrat
from open_ai_key_app.models.gpt_model import LLM_Model

logger = logging.getLogger(__name__)


class BusinessDescPrefillNode(SingleStageExtractionPrefillNode):

    def __init__(
        self,
        llm_model: LLM_Model,
        chunk_strategy: ChunkingStrat,
        prompt: Prompt,
        next_node: BusinessDescExtractionNode,
    ):
        super().__init__(
            field_type=BasicFieldTypeEnum.business_desc,
            llm_model=llm_model,
            chunk_strategy=chunk_strategy,
            prompt=prompt,
            next_node=next_node,
        )
