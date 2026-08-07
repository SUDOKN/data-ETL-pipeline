from __future__ import annotations
import logging
from typing import TYPE_CHECKING

from llm_providers.models.file_objects.prompt import Prompt
from core.models.pipeline_nodes.multi_stage.base.llm_phrase_search_node import (
    LLMPhraseSearchNode,
)
from core.models.field_types import ExtractionFieldType

if TYPE_CHECKING:
    from core.models.pipeline_nodes.multi_stage.keyword.keyword_recursive_search_node import (
        KeywordRecursiveSearchNode,
    )


logger = logging.getLogger(__name__)


class KeywordPhraseSearchNode(LLMPhraseSearchNode[ExtractionFieldType]):
    """Phase 1: LLM Search for keywords"""

    def __init__(
        self,
        field_type: ExtractionFieldType,
        search_prompt: Prompt,
        next_node: KeywordRecursiveSearchNode,
    ):
        super().__init__(
            field_type=field_type,
            phrase_search_prompt=search_prompt,
            next_node=next_node,
        )
