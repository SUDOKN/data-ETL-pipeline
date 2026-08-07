from __future__ import annotations
import logging
from typing import TYPE_CHECKING

from llm_providers.db_models.gpt_batch_request import (
    GPTBatchRequest,
)
from llm_providers.models.file_objects.prompt import Prompt
from llm_providers.models.llm_model import LLM_Model
from core.models.skos_concept import Concept
from core.models.pipeline_nodes.base.base_node import PipelineContext
from core.models.field_types import ConceptFieldType
from core.models.pipeline_nodes.multi_stage.base.llm_phrase_initial_grounding_node import (
    LLMPhraseInitialGroundingNode,
)
from core.models.field_types import (
    ConceptFieldType,
)
from llm_providers.field_types import BatchRequestIDType
from llm_providers.models.open_ai.gpt_model_params import (
    GPTModelParams,
)

if TYPE_CHECKING:
    from core.models.pipeline_nodes.multi_stage.concept.concept_iterative_grounding_node import (
        ConceptIterativeGroundingNode,
    )

logger = logging.getLogger(__name__)


class ConceptInitialGroundingNode(LLMPhraseInitialGroundingNode):

    def __init__(
        self,
        concept_type: ConceptFieldType,
        next_node: ConceptIterativeGroundingNode,
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
    ) -> dict[BatchRequestIDType, GPTBatchRequest]:
        from core.models.pipeline_nodes.multi_stage.concept.concept_relationship_node import (
            ConceptRelationshipNode,
        )

        return pipeline_context[ConceptRelationshipNode]

    def get_upstream_phrase_screening_map(
        self, pipeline_context: PipelineContext
    ) -> dict[BatchRequestIDType, GPTBatchRequest]:
        from core.models.pipeline_nodes.multi_stage.concept.concept_relationship_screening_node import (
            ConceptRelationshipScreeningNode,
        )

        return pipeline_context[ConceptRelationshipScreeningNode]
