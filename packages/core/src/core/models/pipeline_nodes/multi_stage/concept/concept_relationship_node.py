from __future__ import annotations
import logging
from typing import TYPE_CHECKING

from llm_providers.db_models.gpt_batch_request import (
    GPTBatchRequest,
)
from llm_providers.models.file_objects.prompt import Prompt
from core.models.types_and_enums import ConceptTypeEnum
from core.models.pipeline_nodes.base.base_node import PipelineContext
from core.models.pipeline_nodes.multi_stage.concept.concept_phrase_search_node import (
    ConceptPhraseSearchNode,
)
from core.models.pipeline_nodes.multi_stage.concept.concept_recursive_search_node import (
    ConceptRecursiveSearchNode,
)
from core.models.pipeline_nodes.multi_stage.base.llm_phrase_relationship_node import (
    LLMPhraseRelationshipNode,
)
from llm_providers.field_types import BatchRequestIDType

if TYPE_CHECKING:
    from core.models.pipeline_nodes.multi_stage.concept.concept_relationship_screening_node import (
        ConceptRelationshipScreeningNode,
    )

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
    ) -> dict[BatchRequestIDType, GPTBatchRequest]:
        return pipeline_context[ConceptPhraseSearchNode]

    def get_upstream_recursive_search_map(
        self, pipeline_context: PipelineContext
    ) -> dict[BatchRequestIDType, GPTBatchRequest]:
        return pipeline_context[ConceptRecursiveSearchNode]
