from __future__ import annotations
import logging
from typing import TYPE_CHECKING

from core.models.file_objects.prompt import Prompt
from data_etl_app.models.pipeline_nodes.multi_stage.base.llm_phrase_search_node import (
    LLMPhraseSearchNode,
)
from core.models.types_and_enums import (
    ConceptTypeEnum,
)

if TYPE_CHECKING:
    from data_etl_app.models.pipeline_nodes.multi_stage.concept.concept_recursive_search_node import (
        ConceptRecursiveSearchNode,
    )


logger = logging.getLogger(__name__)


class ConceptPhraseSearchNode(LLMPhraseSearchNode[ConceptTypeEnum]):
    """Phase 1: LLM Search for concepts"""

    def __init__(
        self,
        concept_type: ConceptTypeEnum,
        next_node: ConceptRecursiveSearchNode,
        search_prompt: Prompt,
    ):
        super().__init__(
            field_type=concept_type,
            phrase_search_prompt=search_prompt,
            next_node=next_node,
        )
