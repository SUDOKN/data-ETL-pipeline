from __future__ import annotations
import logging
from typing import TYPE_CHECKING

from packages.core.src.core.models.file_objects.prompt import Prompt
from packages.core.src.core.models.pipeline_nodes.multi_stage.base.llm_phrase_search_node import (
    LLMPhraseSearchNode,
)
from packages.core.src.core.models.types_and_enums import KeywordTypeEnum

if TYPE_CHECKING:
    from packages.core.src.core.models.pipeline_nodes.multi_stage.keyword.keyword_recursive_search_node import (
        KeywordRecursiveSearchNode,
    )


logger = logging.getLogger(__name__)


class KeywordPhraseSearchNode(LLMPhraseSearchNode[KeywordTypeEnum]):
    """Phase 1: LLM Search for keywords"""

    def __init__(
        self,
        field_type: KeywordTypeEnum,
        search_prompt: Prompt,
        next_node: KeywordRecursiveSearchNode,
    ):
        super().__init__(
            field_type=field_type,
            phrase_search_prompt=search_prompt,
            next_node=next_node,
        )
