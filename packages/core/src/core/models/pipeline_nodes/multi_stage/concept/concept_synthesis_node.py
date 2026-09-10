from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from llm_providers.db_models.gpt_batch_request import GPTBatchRequest
from llm_providers.field_types import BatchRequestIDType
from llm_providers.models.file_objects.prompt import Prompt

from core.models.field_types import ConceptFieldType
from core.models.pipeline_nodes.base.base_node import PipelineContext
from core.models.pipeline_nodes.multi_stage.base.llm_phrase_synthesis_node import (
    LLMPhraseSynthesisNode,
)
from core.models.pipeline_nodes.multi_stage.concept.concept_phrase_search_node import (
    ConceptPhraseSearchNode,
)
from core.models.pipeline_nodes.multi_stage.concept.concept_recursive_search_node import (
    ConceptRecursiveSearchNode,
)

if TYPE_CHECKING:
    from core.models.pipeline_nodes.multi_stage.concept.concept_initial_grounding_node import (
        ConceptInitialGroundingNode,
    )

logger = logging.getLogger(__name__)


class ConceptSynthesisNode(LLMPhraseSynthesisNode[ConceptFieldType]):
    """v3 phase 3.2 for concept fields: one synthesis per group per chunk. Thin
    wrapper that points the stage at the concept search nodes' maps (the
    sent-forms pooling; the fold itself is pure code)."""

    def __init__(
        self,
        concept_type: ConceptFieldType,
        phrase_synthesis_prompt: Prompt,
        next_node: ConceptInitialGroundingNode,
    ):
        super().__init__(
            field_type=concept_type,
            phrase_synthesis_prompt=phrase_synthesis_prompt,
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
