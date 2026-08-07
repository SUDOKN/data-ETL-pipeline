from __future__ import annotations
import logging
from typing import TYPE_CHECKING

from llm_providers.models.file_objects.prompt import Prompt
from core.models.pipeline_nodes.multi_stage.base.llm_phrase_search_node import (
    LLMPhraseSearchNode,
)
from core.models.field_types import (
    ConceptFieldType,
)

if TYPE_CHECKING:
    from core.models.pipeline_nodes.multi_stage.concept.concept_recursive_search_node import (
        ConceptRecursiveSearchNode,
    )


logger = logging.getLogger(__name__)


class ConceptPhraseSearchNode(LLMPhraseSearchNode[ConceptFieldType]):
    """Phase 1: LLM Search for concepts"""

    def __init__(
        self,
        concept_type: ConceptFieldType,
        next_node: ConceptRecursiveSearchNode,
        search_prompt: Prompt,
    ):
        super().__init__(
            field_type=concept_type,
            phrase_search_prompt=search_prompt,
            next_node=next_node,
        )
