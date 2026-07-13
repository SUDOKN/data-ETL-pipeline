from __future__ import annotations
import logging

from core.models.db.gpt_batch_request import GPTBatchRequest
from core.models.prompt import Prompt
from data_etl_app.models.skos_concept import Concept
from data_etl_app.models.pipeline_nodes.base.base_node import PipelineContext
from data_etl_app.models.pipeline_nodes.multi_stage.llm_phrase_recursive_grounding_node import (
    LLMPhraseRecursiveGroundingNode,
)
from data_etl_app.models.pipeline_nodes.multi_stage.concept.concept_reconcile_node import (
    ConceptReconcileNode,
)
from data_etl_app.models.types_and_enums import (
    ConceptTypeEnum,
)
from open_ai_key_app.models.field_types import GPTBatchRequestCustomID

logger = logging.getLogger(__name__)


class ConceptRecursiveGroundingNode(LLMPhraseRecursiveGroundingNode):

    def __init__(
        self,
        concept_type: ConceptTypeEnum,
        next_node: ConceptReconcileNode,
        phrase_recursive_grounding_prompt: Prompt,
        known_concepts: set[Concept],
    ):
        super().__init__(
            field_type=concept_type,
            next_node=next_node,
            phrase_recursive_grounding_prompt=phrase_recursive_grounding_prompt,
            known_concepts=known_concepts,
        )

    def get_upstream_initial_grounding_map(
        self, pipeline_context: PipelineContext
    ) -> dict[GPTBatchRequestCustomID, GPTBatchRequest]:
        from data_etl_app.models.pipeline_nodes.multi_stage.concept.concept_initial_grounding_node import (
            ConceptInitialGroundingNode,
        )

        return pipeline_context[ConceptInitialGroundingNode]

    def get_upstream_phrase_relationship_map(
        self, pipeline_context: PipelineContext
    ) -> dict[GPTBatchRequestCustomID, GPTBatchRequest]:
        from data_etl_app.models.pipeline_nodes.multi_stage.concept.concept_relationship_node import (
            ConceptRelationshipNode,
        )

        return pipeline_context[ConceptRelationshipNode]
