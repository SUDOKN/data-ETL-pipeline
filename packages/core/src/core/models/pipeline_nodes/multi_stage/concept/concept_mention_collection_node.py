from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from llm_providers.db_models.gpt_batch_request import GPTBatchRequest
from llm_providers.field_types import BatchRequestIDType
from llm_providers.models.file_objects.prompt import Prompt

from core.models.field_types import ConceptFieldType
from core.models.pipeline_nodes.base.base_node import PipelineContext
from core.models.pipeline_nodes.multi_stage.base.llm_phrase_mention_collection_node import (
    LLMPhraseMentionCollectionNode,
)
from core.models.pipeline_nodes.multi_stage.concept.concept_phrase_search_node import (
    ConceptPhraseSearchNode,
)
from core.models.pipeline_nodes.multi_stage.concept.concept_recursive_search_node import (
    ConceptRecursiveSearchNode,
)

if TYPE_CHECKING:
    from core.models.pipeline_nodes.multi_stage.concept.concept_synthesis_node import (
        ConceptSynthesisNode,
    )

logger = logging.getLogger(__name__)


class ConceptMentionCollectionNode(LLMPhraseMentionCollectionNode[ConceptFieldType]):
    """v3 phase 3 for concept fields: mentions per sub-window, folded per chunk.
    Thin wrapper that points the stage at the concept search nodes' maps."""

    def __init__(
        self,
        concept_type: ConceptFieldType,
        phrase_mention_collection_prompt: Prompt,
        next_node: ConceptSynthesisNode,
    ):
        super().__init__(
            field_type=concept_type,
            phrase_mention_collection_prompt=phrase_mention_collection_prompt,
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
