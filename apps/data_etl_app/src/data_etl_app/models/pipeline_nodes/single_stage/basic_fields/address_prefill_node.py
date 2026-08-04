from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from packages.core.src.core.models.file_objects.prompt import Prompt
from packages.core.src.core.models.extraction_results.single_stage_extraction_results import (
    LLMSingleStageExtractionMetadata,
)
from packages.core.src.core.models.pipeline_nodes.single_stage.base.single_stage_extraction_prefill_node import (
    SingleStageExtractionPrefillNode,
)
from packages.core.src.core.models.types_and_enums import (
    BasicFieldTypeEnum,
)
from packages.core.src.core.models.chunking_strat import ChunkingStrategy

if TYPE_CHECKING:
    from apps.data_etl_app.src.data_etl_app.models.pipeline_nodes.single_stage.basic_fields.address_extraction_node import (
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
