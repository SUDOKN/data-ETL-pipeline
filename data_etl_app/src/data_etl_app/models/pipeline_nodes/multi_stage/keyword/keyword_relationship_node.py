from __future__ import annotations
import logging
from typing import TYPE_CHECKING

from core.models.db.gpt_batch_request import GPTBatchRequest
from core.models.prompt import Prompt
from data_etl_app.models.pipeline_nodes.base.base_node import PipelineContext
from data_etl_app.models.types_and_enums import KeywordTypeEnum
from data_etl_app.models.pipeline_nodes.multi_stage.keyword.keyword_phrase_search_node import (
    KeywordPhraseSearchNode,
)
from data_etl_app.models.pipeline_nodes.multi_stage.keyword.keyword_recursive_search_node import (
    KeywordRecursiveSearchNode,
)
from data_etl_app.models.pipeline_nodes.multi_stage.llm_phrase_relationship_node import (
    LLMPhraseRelationshipNode,
)
from open_ai_key_app.models.field_types import GPTBatchRequestCustomID

if TYPE_CHECKING:
    from data_etl_app.models.pipeline_nodes.multi_stage.keyword.keyword_relationship_screening_node import (
        KeywordRelationshipScreeningNode,
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
        next_node: KeywordRelationshipScreeningNode,
    ):
        super().__init__(
            field_type=field_type,
            phrase_relationship_prompt=phrase_relationship_prompt,
            next_node=next_node,
        )

    def get_upstream_phrase_search_map(
        self, pipeline_context: PipelineContext
    ) -> dict[GPTBatchRequestCustomID, GPTBatchRequest]:
        return pipeline_context[KeywordPhraseSearchNode]

    def get_upstream_recursive_search_map(
        self, pipeline_context: PipelineContext
    ) -> dict[GPTBatchRequestCustomID, GPTBatchRequest]:
        return pipeline_context[KeywordRecursiveSearchNode]
