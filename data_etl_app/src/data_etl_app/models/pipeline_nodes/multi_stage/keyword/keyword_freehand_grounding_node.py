from __future__ import annotations
import logging
from typing import TYPE_CHECKING

from core.models.db.gpt_batch_request import GPTBatchRequest
from core.models.file_objects.prompt import Prompt
from data_etl_app.models.pipeline_nodes.base.base_node import PipelineContext
from data_etl_app.models.pipeline_nodes.multi_stage.llm_phrase_freehand_grounding_node import (
    LLMPhraseFreehandGroundingNode,
)
from data_etl_app.models.types_and_enums import KeywordTypeEnum
from open_ai_key_app.models.field_types import GPTBatchRequestCustomID

if TYPE_CHECKING:
    from data_etl_app.models.pipeline_nodes.multi_stage.keyword.keyword_reconcile_node import (
        KeywordReconcileNode,
    )

logger = logging.getLogger(__name__)


class KeywordFreehandGroundingNode(LLMPhraseFreehandGroundingNode[KeywordTypeEnum]):
    def __init__(
        self,
        field_type: KeywordTypeEnum,
        next_node: KeywordReconcileNode,
        phrase_freehand_grounding_prompt: Prompt,
    ):
        super().__init__(
            field_type=field_type,
            phrase_freehand_grounding_prompt=phrase_freehand_grounding_prompt,
            next_node=next_node,
        )

    def get_upstream_phrase_relationship_map(
        self, pipeline_context: PipelineContext
    ) -> dict[GPTBatchRequestCustomID, GPTBatchRequest]:
        from data_etl_app.models.pipeline_nodes.multi_stage.keyword.keyword_relationship_node import (
            KeywordRelationshipNode,
        )

        return pipeline_context[KeywordRelationshipNode]

    def get_upstream_phrase_screening_map(
        self, pipeline_context: PipelineContext
    ) -> dict[GPTBatchRequestCustomID, GPTBatchRequest]:
        from data_etl_app.models.pipeline_nodes.multi_stage.keyword.keyword_relationship_screening_node import (
            KeywordRelationshipScreeningNode,
        )

        return pipeline_context[KeywordRelationshipScreeningNode]
