from __future__ import annotations
import logging
from typing import TYPE_CHECKING

from core.models.prompt import Prompt
from data_etl_app.models.pipeline_nodes.multi_stage.llm_phrase_search_node import (
    LLMPhraseSearchNode,
)
from data_etl_app.models.types_and_enums import KeywordTypeEnum

if TYPE_CHECKING:
    from data_etl_app.models.pipeline_nodes.multi_stage.keyword.keyword_relationship_node import (
        KeywordRelationshipNode,
    )


logger = logging.getLogger(__name__)


class KeywordSearchNode(LLMPhraseSearchNode[KeywordTypeEnum]):
    """Phase 1: LLM Search for keywords"""

    def __init__(
        self,
        field_type: KeywordTypeEnum,
        search_prompt: Prompt,
        next_node: KeywordRelationshipNode,
    ):
        super().__init__(
            field_type=field_type,
            phrase_search_prompt=search_prompt,
            next_node=next_node,
        )
