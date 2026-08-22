from __future__ import annotations
import logging
from typing import TYPE_CHECKING, Optional

from llm_providers.db_models.gpt_batch_request import (
    GPTBatchRequest,
)
from llm_providers.models.file_objects.prompt import Prompt
from core.models.skos_concept import Concept
from core.models.pipeline_nodes.base.base_node import PipelineContext
from core.models.field_types import ConceptFieldType
from core.models.pipeline_nodes.multi_stage.base.llm_phrase_oov_grounding_node import (
    LLMPhraseOovGroundingNode,
)
from llm_providers.field_types import BatchRequestIDType

if TYPE_CHECKING:
    from core.models.pipeline_nodes.multi_stage.concept.concept_relationship_screening_node import (
        ConceptRelationshipScreeningNode,
    )

logger = logging.getLogger(__name__)


class ConceptOovGroundingNode(LLMPhraseOovGroundingNode):

    def __init__(
        self,
        concept_type: ConceptFieldType,
        next_node: ConceptRelationshipScreeningNode,
        phrase_oov_grounding_prompt: Optional[Prompt],
        known_concepts: set[Concept],
    ):
        super().__init__(
            field_type=concept_type,
            next_node=next_node,
            phrase_oov_grounding_prompt=phrase_oov_grounding_prompt,
            known_concepts=known_concepts,
        )

    def get_upstream_phrase_relationship_map(
        self, pipeline_context: PipelineContext
    ) -> dict[BatchRequestIDType, GPTBatchRequest]:
        from core.models.pipeline_nodes.multi_stage.concept.concept_relationship_node import (
            ConceptRelationshipNode,
        )

        return pipeline_context[ConceptRelationshipNode]

    def get_upstream_in_vocab_grounding_map(
        self, pipeline_context: PipelineContext
    ) -> dict[BatchRequestIDType, GPTBatchRequest]:
        from core.models.pipeline_nodes.multi_stage.concept.concept_initial_grounding_node import (
            ConceptInitialGroundingNode,
        )

        return pipeline_context[ConceptInitialGroundingNode]
