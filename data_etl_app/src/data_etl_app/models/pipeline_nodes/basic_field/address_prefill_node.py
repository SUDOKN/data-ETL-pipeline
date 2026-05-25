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
from data_etl_app.models.chunking_strat import ChunkingStrategy

logger = logging.getLogger(__name__)


class AddressPrefillNode(SingleStageExtractionPrefillNode):

    def __init__(
        self,
        chunk_strategy: ChunkingStrategy,
        prompt: Prompt,
        next_node: AddressExtractionNode,
    ):
        super().__init__(
            field_type=BasicFieldTypeEnum.addresses,
            prompt=prompt,
            chunk_strategy=chunk_strategy,
            next_node=next_node,
        )
