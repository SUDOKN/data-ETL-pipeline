from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from core.models.file_objects.prompt import Prompt
from core.models.extraction_results.single_stage_extraction_results import (
    LLMSingleStageExtractionMetadata,
)
from data_etl_app.models.pipeline_nodes.single_stage.basic_fields.single_stage_extraction_prefill_node import (
    SingleStageExtractionPrefillNode,
)
from data_etl_app.models.types_and_enums import (
    BasicFieldTypeEnum,
)
from data_etl_app.models.chunking_strat import ChunkingStrategy

if TYPE_CHECKING:
    from data_etl_app.models.pipeline_nodes.single_stage.basic_fields.address_extraction_node import (
        AddressExtractionNode,
    )

logger = logging.getLogger(__name__)


class AddressPrefillNode(SingleStageExtractionPrefillNode):

    def __init__(
        self,
        chunk_strategy: ChunkingStrategy,
        next_node: AddressExtractionNode,
        prompt: Prompt,
        address_extraction_metadata: LLMSingleStageExtractionMetadata,
    ):
        super().__init__(
            field_type=BasicFieldTypeEnum.addresses,
            next_node=next_node,
            chunk_strategy=chunk_strategy,
            prompt=prompt,
            extraction_metadata=address_extraction_metadata,
        )
