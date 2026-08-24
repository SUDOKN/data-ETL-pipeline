from __future__ import annotations
import logging
from typing import TYPE_CHECKING

from llm_providers.db_models.gpt_batch_request import (
    GPTBatchRequest,
)
from llm_providers.models.file_objects.prompt import Prompt
from core.models.skos_concept import Concept
from core.models.pipeline_nodes.base.base_node import PipelineContext
from core.models.field_types import ConceptFieldType
from core.models.pipeline_nodes.multi_stage.base.llm_phrase_initial_grounding_node import (
    LLMPhraseInitialGroundingNode,
)
from llm_providers.field_types import BatchRequestIDType

if TYPE_CHECKING:
    from core.models.pipeline_nodes.multi_stage.concept.concept_oov_grounding_node import (
        ConceptOovGroundingNode,
    )

logger = logging.getLogger(__name__)


class ConceptInitialGroundingNode(LLMPhraseInitialGroundingNode):

    def __init__(
        self,
        concept_type: ConceptFieldType,
        next_node: ConceptOovGroundingNode,
        phrase_initial_grounding_prompt: Prompt,
        known_concepts: set[Concept],
    ):
        super().__init__(
            field_type=concept_type,
            next_node=next_node,
            phrase_initial_grounding_prompt=phrase_initial_grounding_prompt,
            known_concepts=known_concepts,
        )

    def get_upstream_mention_collection_map(
        self, pipeline_context: PipelineContext
    ) -> dict[BatchRequestIDType, GPTBatchRequest]:
        from core.models.pipeline_nodes.multi_stage.concept.concept_mention_collection_node import (
            ConceptMentionCollectionNode,
        )

        return pipeline_context[ConceptMentionCollectionNode]

    def get_upstream_synthesis_map(
        self, pipeline_context: PipelineContext
    ) -> dict[BatchRequestIDType, GPTBatchRequest]:
        from core.models.pipeline_nodes.multi_stage.concept.concept_synthesis_node import (
            ConceptSynthesisNode,
        )

        return pipeline_context[ConceptSynthesisNode]
