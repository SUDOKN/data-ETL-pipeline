from __future__ import annotations
import logging
from typing import TYPE_CHECKING

from core.models.prompt import Prompt
from data_etl_app.models.pipeline_nodes.search_node import SearchNode
from data_etl_app.models.types_and_enums import KeywordTypeEnum

if TYPE_CHECKING:
    from data_etl_app.models.pipeline_nodes.keyword.keyword_evidence_node import (
        KeywordEvidenceNode,
    )


logger = logging.getLogger(__name__)


class KeywordSearchNode(SearchNode[KeywordTypeEnum]):
    """Phase 1: LLM Search for keywords"""

    def __init__(
        self,
        field_type: KeywordTypeEnum,
        search_prompt: Prompt,
        next_node: KeywordEvidenceNode,
    ):
        super().__init__(
            field_type=field_type,
            search_prompt=search_prompt,
            next_node=next_node,
        )
