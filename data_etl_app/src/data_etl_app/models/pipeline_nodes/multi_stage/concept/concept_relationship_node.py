from __future__ import annotations
import logging

from core.models.db.gpt_batch_request import GPTBatchRequest
from core.models.file_objects.prompt import Prompt
from data_etl_app.models.types_and_enums import ConceptTypeEnum
from data_etl_app.models.pipeline_nodes.base.base_node import PipelineContext
from data_etl_app.models.pipeline_nodes.multi_stage.concept.concept_phrase_search_node import (
    ConceptPhraseSearchNode,
)
from data_etl_app.models.pipeline_nodes.multi_stage.concept.concept_recursive_search_node import (
    ConceptRecursiveSearchNode,
)
from data_etl_app.models.pipeline_nodes.multi_stage.concept.concept_relationship_screening_node import (
    ConceptRelationshipScreeningNode,
)
from data_etl_app.models.pipeline_nodes.multi_stage.llm_phrase_relationship_node import (
    LLMPhraseRelationshipNode,
)
from open_ai_key_app.models.field_types import GPTBatchRequestCustomID

logger = logging.getLogger(__name__)


class ConceptRelationshipNode(LLMPhraseRelationshipNode[ConceptTypeEnum]):
    """Phase 2: LLM distills concepts from the phrases with context of the text.

    Thin wrapper over the shared ``PhraseRelationshipNode`` that narrows the constructor types and
    points the phrase_relationship phase at the upstream ``ConceptSearchNode`` results.
    """

    def __init__(
        self,
        concept_type: ConceptTypeEnum,
        phrase_relationship_prompt: Prompt,
        next_node: ConceptRelationshipScreeningNode,
    ):
        super().__init__(
            field_type=concept_type,
            phrase_relationship_prompt=phrase_relationship_prompt,
            next_node=next_node,
        )

    def get_upstream_phrase_search_map(
        self, pipeline_context: PipelineContext
    ) -> dict[GPTBatchRequestCustomID, GPTBatchRequest]:
        return pipeline_context[ConceptPhraseSearchNode]

    def get_upstream_recursive_search_map(
        self, pipeline_context: PipelineContext
    ) -> dict[GPTBatchRequestCustomID, GPTBatchRequest]:
        return pipeline_context[ConceptRecursiveSearchNode]
