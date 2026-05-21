import logging

from core.models.prompt import Prompt
from data_etl_app.models.pipeline_nodes.basic_field.address_extraction_node import (
    AddressExtractionNode,
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


class AddressPrefillNode(SingleStageExtractionPrefillNode):
    next_node: AddressExtractionNode

    def __init__(
        self,
        llm_model: LLM_Model,
        chunk_strategy: ChunkingStrat,
        prompt: Prompt,
        next_node: AddressExtractionNode,
    ):
        super().__init__(
            field_type=BasicFieldTypeEnum.addresses,
            llm_model=llm_model,
            prompt=prompt,
            chunk_strategy=chunk_strategy,
            next_node=next_node,
        )
