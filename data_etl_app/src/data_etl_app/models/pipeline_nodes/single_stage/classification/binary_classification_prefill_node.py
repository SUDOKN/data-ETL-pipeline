import logging

from core.models.prompt import Prompt
from core.models.single_stage_extraction_results import LLMSingleStageExtractionMetadata
from data_etl_app.models.pipeline_nodes.single_stage.classification.binary_classification_node import (
    BinaryClassificationNode,
)
from data_etl_app.models.pipeline_nodes.single_stage.basic_fields.single_stage_extraction_prefill_node import (
    SingleStageExtractionPrefillNode,
)
from data_etl_app.models.types_and_enums import (
    BinaryClassificationTypeEnum,
)
from data_etl_app.models.chunking_strat import ChunkingStrategy

logger = logging.getLogger(__name__)


class BinaryClassificationPrefillNode(SingleStageExtractionPrefillNode):

    def __init__(
        self,
        binary_field_type: BinaryClassificationTypeEnum,
        next_node: BinaryClassificationNode,
        chunk_strategy: ChunkingStrategy,
        prompt: Prompt,
        extraction_metadata: LLMSingleStageExtractionMetadata,
    ):
        super().__init__(
            field_type=binary_field_type,
            next_node=next_node,
            chunk_strategy=chunk_strategy,
            prompt=prompt,
            extraction_metadata=extraction_metadata,
        )
