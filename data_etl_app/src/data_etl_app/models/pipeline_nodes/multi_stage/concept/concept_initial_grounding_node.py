from __future__ import annotations
import logging
from typing import TYPE_CHECKING

from core.models.db.gpt_batch_request import GPTBatchRequest
from core.models.file_objects.prompt import Prompt
from core.models.llm_model import LLM_Model
from data_etl_app.models.skos_concept import Concept
from data_etl_app.models.pipeline_nodes.base.base_node import PipelineContext
from data_etl_app.models.types_and_enums import ConceptTypeEnum
from data_etl_app.models.pipeline_nodes.multi_stage.llm_phrase_initial_grounding_node import (
    LLMPhraseInitialGroundingNode,
)
from data_etl_app.models.types_and_enums import (
    ConceptTypeEnum,
)
from open_ai_key_app.models.field_types import GPTBatchRequestCustomID
from open_ai_key_app.models.gpt_model_params import GPTModelParams

if TYPE_CHECKING:
    from data_etl_app.models.pipeline_nodes.multi_stage.concept.concept_recursive_grounding_node import (
        ConceptRecursiveGroundingNode,
    )

logger = logging.getLogger(__name__)


class ConceptInitialGroundingNode(LLMPhraseInitialGroundingNode):

    def __init__(
        self,
        concept_type: ConceptTypeEnum,
        next_node: ConceptRecursiveGroundingNode,
        phrase_initial_grounding_prompt: Prompt,
        known_concepts: set[Concept],
    ):
        super().__init__(
            field_type=concept_type,
            next_node=next_node,
            phrase_initial_grounding_prompt=phrase_initial_grounding_prompt,
            known_concepts=known_concepts,
        )

    def get_upstream_phrase_relationship_map(
        self, pipeline_context: PipelineContext
    ) -> dict[GPTBatchRequestCustomID, GPTBatchRequest]:
        from data_etl_app.models.pipeline_nodes.multi_stage.concept.concept_relationship_node import (
            ConceptRelationshipNode,
        )

        return pipeline_context[ConceptRelationshipNode]

    def get_upstream_phrase_screening_map(
        self, pipeline_context: PipelineContext
    ) -> dict[GPTBatchRequestCustomID, GPTBatchRequest]:
        from data_etl_app.models.pipeline_nodes.multi_stage.concept.concept_relationship_screening_node import (
            ConceptRelationshipScreeningNode,
        )

        return pipeline_context[ConceptRelationshipScreeningNode]
