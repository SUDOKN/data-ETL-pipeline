from __future__ import annotations
import logging
from typing import TYPE_CHECKING

from llm_providers.db_models.gpt_batch_request import (
    GPTBatchRequest,
)
from llm_providers.models.file_objects.prompt import Prompt
from core.models.pipeline_nodes.base.base_node import PipelineContext
from core.models.pipeline_nodes.multi_stage.base.llm_phrase_recursive_search_node import (
    LLMPhraseRecursiveSearchNode,
)
from core.models.pipeline_nodes.multi_stage.concept.concept_phrase_search_node import (
    ConceptPhraseSearchNode,
)
from core.models.types_and_enums import (
    ConceptTypeEnum,
)
from llm_providers.field_types import BatchRequestIDType

if TYPE_CHECKING:
    from core.models.pipeline_nodes.multi_stage.concept.concept_relationship_node import (
        ConceptRelationshipNode,
    )

logger = logging.getLogger(__name__)


class ConceptRecursiveSearchNode(LLMPhraseRecursiveSearchNode[ConceptTypeEnum]):
    """Recursive concept phrase search.

    Sits between :class:`ConceptPhraseSearchNode` (round 1) and the relationship
    phase, compounding excluded phrases across rounds until convergence.
    """

    def __init__(
        self,
        concept_type: ConceptTypeEnum,
        next_node: ConceptRelationshipNode,
        second_search_prompt: Prompt,
    ):
        super().__init__(
            field_type=concept_type,
            next_node=next_node,
            recursive_search_prompt=second_search_prompt,
        )

    def get_upstream_first_search_map(
        self, pipeline_context: PipelineContext
    ) -> dict[BatchRequestIDType, GPTBatchRequest]:
        return pipeline_context[ConceptPhraseSearchNode]
