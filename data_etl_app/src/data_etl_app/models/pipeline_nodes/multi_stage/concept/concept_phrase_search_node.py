from __future__ import annotations
import logging
from typing import TYPE_CHECKING

from core.models.prompt import Prompt
from data_etl_app.models.pipeline_nodes import (
    LLMPhraseSearchNode,
)
from data_etl_app.models.types_and_enums import (
    ConceptTypeEnum,
)

if TYPE_CHECKING:
    from data_etl_app.models.pipeline_nodes import (
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
