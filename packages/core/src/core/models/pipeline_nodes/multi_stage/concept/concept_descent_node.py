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
from core.models.pipeline_nodes.multi_stage.base.llm_phrase_descent_node import (
    LLMPhraseDescentNode,
)
from core.models.skos_concept import Concept

logger = logging.getLogger(__name__)


class ConceptDescentNode(LLMPhraseDescentNode):
    """Screening and descent in depth waves for a concept field, after the
    proposal pass; its successor is the reconcile node (the cutover)."""

    def __init__(
        self,
        concept_type: ConceptFieldType,
        next_node: BaseLLMExtractionNode | ReconcileNode,
        phrase_descent_prompt: Prompt,
        phrase_unit_screening_prompt: Prompt,
        known_concepts: set[Concept],
    ):
        super().__init__(
            field_type=concept_type,
            next_node=next_node,
            phrase_descent_prompt=phrase_descent_prompt,
            phrase_unit_screening_prompt=phrase_unit_screening_prompt,
            known_concepts=known_concepts,
        )

    def get_upstream_synthesis_map(self, pipeline_context: PipelineContext) -> dict[BatchRequestIDType, GPTBatchRequest]:
        from core.models.pipeline_nodes.multi_stage.concept.concept_synthesis_node import ConceptSynthesisNode

        return pipeline_context[ConceptSynthesisNode]

    def get_upstream_grounding_map(self, pipeline_context: PipelineContext) -> dict[BatchRequestIDType, GPTBatchRequest]:
        from core.models.pipeline_nodes.multi_stage.concept.concept_grounding_node import ConceptGroundingNode

        return pipeline_context[ConceptGroundingNode]

    def get_upstream_proposal_map(self, pipeline_context: PipelineContext) -> dict[BatchRequestIDType, GPTBatchRequest]:
        from core.models.pipeline_nodes.multi_stage.concept.concept_proposal_node import ConceptProposalNode

        return pipeline_context[ConceptProposalNode]
