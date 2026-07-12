from __future__ import annotations
import logging
from typing import TYPE_CHECKING

from core.models.db.gpt_batch_request import GPTBatchRequest
from core.models.prompt import Prompt
from data_etl_app.models.pipeline_nodes.base.base_node import PipelineContext
from data_etl_app.models.pipeline_nodes.multi_stage.llm_phrase_recursive_search_node import (
    LLMPhraseRecursiveSearchNode,
)
from data_etl_app.models.pipeline_nodes.multi_stage.keyword.keyword_phrase_search_node import (
    KeywordPhraseSearchNode,
)
from data_etl_app.models.types_and_enums import (
    KeywordTypeEnum,
)
from open_ai_key_app.models.field_types import GPTBatchRequestCustomID

if TYPE_CHECKING:
    from data_etl_app.models.pipeline_nodes.multi_stage.keyword.keyword_relationship_node import (
        KeywordRelationshipNode,
    )

logger = logging.getLogger(__name__)


class KeywordRecursiveSearchNode(LLMPhraseRecursiveSearchNode[KeywordTypeEnum]):
    """Recursive keyword phrase search.

    Sits between :class:`KeywordPhraseSearchNode` (round 0) and the relationship
    phase, compounding excluded phrases across rounds until convergence.
    """

    def __init__(
        self,
        field_type: KeywordTypeEnum,
        next_node: KeywordRelationshipNode,
        second_search_prompt: Prompt,
    ):
        super().__init__(
            field_type=field_type,
            next_node=next_node,
            recursive_search_prompt=second_search_prompt,
        )

    def get_upstream_first_search_map(
        self, pipeline_context: PipelineContext
    ) -> dict[GPTBatchRequestCustomID, GPTBatchRequest]:
        return pipeline_context[KeywordPhraseSearchNode]
