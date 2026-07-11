from __future__ import annotations
import logging
from typing import TYPE_CHECKING

from core.models.prompt import Prompt
from data_etl_app.models.types_and_enums import KeywordTypeEnum
from data_etl_app.models.pipeline_nodes.multi_stage.keyword.keyword_search_node import (
    KeywordSearchNode,
)
from data_etl_app.models.pipeline_nodes.llm_phrase_relationship_node import (
    LLMPhraseRelationshipNode,
)

if TYPE_CHECKING:
    from data_etl_app.models.pipeline_nodes.multi_stage.keyword.keyword_reconcile_node import (
        KeywordReconcileNode,
    )

logger = logging.getLogger(__name__)


class KeywordRelationshipNode(LLMPhraseRelationshipNode[KeywordTypeEnum]):
    """Phase 2: LLM distills keywords from the phrases with context of the text.

    Thin wrapper over the shared ``PhraseRelationshipNode`` that narrows the constructor types and
    points the phrase_relationship phase at the upstream ``KeywordSearchNode`` results.
    """

    def __init__(
        self,
        field_type: KeywordTypeEnum,
        phrase_relationship_prompt: Prompt,
        next_node: KeywordReconcileNode,
    ):
        super().__init__(
            field_type=field_type,
            phrase_relationship_prompt=phrase_relationship_prompt,
            next_node=next_node,
        )
