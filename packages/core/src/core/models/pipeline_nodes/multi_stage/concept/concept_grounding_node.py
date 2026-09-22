from __future__ import annotations

import logging

from llm_providers.db_models.gpt_batch_request import GPTBatchRequest
from llm_providers.field_types import BatchRequestIDType
from llm_providers.models.file_objects.prompt import Prompt

from core.models.field_types import ConceptFieldType
from core.models.pipeline_nodes.base.base_llm_extraction_node import (
    BaseLLMExtractionNode,
)
from core.models.pipeline_nodes.base.base_node import PipelineContext
from core.models.pipeline_nodes.base.base_reconcile_node import ReconcileNode
from core.models.pipeline_nodes.multi_stage.base.llm_phrase_grounding_node import (
    LLMPhraseGroundingNode,
)
from core.models.skos_concept import Concept

logger = logging.getLogger(__name__)


class ConceptGroundingNode(LLMPhraseGroundingNode):
    """The Step 2 one-call grounding stage of a concept field, straight off
    the synthesis stage. Its successor is the proposal pass (a later substep);
    until the cutover the type stays the base's."""

    def __init__(
        self,
        concept_type: ConceptFieldType,
        next_node: BaseLLMExtractionNode | ReconcileNode,
        phrase_grounding_prompt: Prompt,
        known_concepts: set[Concept],
    ):
        super().__init__(
            field_type=concept_type,
            next_node=next_node,
            phrase_grounding_prompt=phrase_grounding_prompt,
            known_concepts=known_concepts,
        )

    def get_upstream_synthesis_map(
        self, pipeline_context: PipelineContext
    ) -> dict[BatchRequestIDType, GPTBatchRequest]:
        from core.models.pipeline_nodes.multi_stage.concept.concept_synthesis_node import (
            ConceptSynthesisNode,
        )

        return pipeline_context[ConceptSynthesisNode]
