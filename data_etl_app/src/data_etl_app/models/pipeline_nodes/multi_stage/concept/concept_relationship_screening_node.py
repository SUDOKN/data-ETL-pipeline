from __future__ import annotations
import logging

from core.models.db.gpt_batch_request import GPTBatchRequest
from core.models.file_objects.prompt import Prompt
from data_etl_app.models.pipeline_nodes.base.base_node import PipelineContext
from data_etl_app.models.types_and_enums import ConceptTypeEnum
from data_etl_app.models.pipeline_nodes.multi_stage.llm_phrase_relationship_screening_node import (
    LLMPhraseRelationshipScreeningNode,
)
from data_etl_app.models.pipeline_nodes.multi_stage.concept.concept_initial_grounding_node import (
    ConceptInitialGroundingNode,
)
from data_etl_app.models.types_and_enums import (
    ConceptTypeEnum,
)
from open_ai_key_app.models.field_types import GPTBatchRequestCustomID

logger = logging.getLogger(__name__)


class ConceptRelationshipScreeningNode(
    LLMPhraseRelationshipScreeningNode[ConceptTypeEnum]
):

    def __init__(
        self,
        concept_type: ConceptTypeEnum,
        next_node: ConceptInitialGroundingNode,
        phrase_relationship_screening_prompt: Prompt,
    ):
        super().__init__(
            field_type=concept_type,
            phrase_relationship_screening_prompt=phrase_relationship_screening_prompt,
            next_node=next_node,
        )

    def get_upstream_phrase_relationship_map(
        self, pipeline_context: PipelineContext
    ) -> dict[GPTBatchRequestCustomID, GPTBatchRequest]:
        from data_etl_app.models.pipeline_nodes.multi_stage.concept.concept_relationship_node import (
            ConceptRelationshipNode,
        )

        return pipeline_context[ConceptRelationshipNode]
