import logging

from core.models.prompt import Prompt
from data_etl_app.models.pipeline_nodes.classification.binary_classification_node import (
    BinaryClassificationNode,
)
from data_etl_app.models.pipeline_nodes.single_stage_extraction_prefill_node import (
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
        chunk_strategy: ChunkingStrategy,
        prompt: Prompt,
        next_node: BinaryClassificationNode,
    ):
        super().__init__(
            field_type=binary_field_type,
            chunk_strategy=chunk_strategy,
            prompt=prompt,
            next_node=next_node,
        )
