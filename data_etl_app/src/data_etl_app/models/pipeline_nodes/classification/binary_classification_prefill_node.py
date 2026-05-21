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
from data_etl_app.models.chunking_strat import ChunkingStrat
from open_ai_key_app.models.gpt_model import LLM_Model

logger = logging.getLogger(__name__)


class BinaryClassificationPrefillNode(SingleStageExtractionPrefillNode):

    def __init__(
        self,
        binary_field_type: BinaryClassificationTypeEnum,
        llm_model: LLM_Model,
        chunk_strategy: ChunkingStrat,
        prompt: Prompt,
        next_node: BinaryClassificationNode,
    ):
        super().__init__(
            field_type=binary_field_type,
            llm_model=llm_model,
            chunk_strategy=chunk_strategy,
            prompt=prompt,
            next_node=next_node,
        )
