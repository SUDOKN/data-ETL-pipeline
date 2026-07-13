from __future__ import annotations
import logging
from typing import TYPE_CHECKING

from core.models.db.gpt_batch_request import GPTBatchRequest
from core.models.file_objects.prompt import Prompt
from data_etl_app.models.pipeline_nodes.base.base_node import PipelineContext
from data_etl_app.models.pipeline_nodes.multi_stage.llm_phrase_relationship_screening_node import (
    LLMPhraseRelationshipScreeningNode,
)
from data_etl_app.models.types_and_enums import KeywordTypeEnum
from open_ai_key_app.models.field_types import GPTBatchRequestCustomID

if TYPE_CHECKING:
    from data_etl_app.models.pipeline_nodes.multi_stage.keyword.keyword_freehand_grounding_node import (
        KeywordFreehandGroundingNode,
    )

logger = logging.getLogger(__name__)


class KeywordRelationshipScreeningNode(
    LLMPhraseRelationshipScreeningNode[KeywordTypeEnum]
):
    def __init__(
        self,
        field_type: KeywordTypeEnum,
        next_node: KeywordFreehandGroundingNode,
        phrase_relationship_screening_prompt: Prompt,
    ):
        super().__init__(
            field_type=field_type,
            phrase_relationship_screening_prompt=phrase_relationship_screening_prompt,
            next_node=next_node,
        )

    def get_upstream_phrase_relationship_map(
        self, pipeline_context: PipelineContext
    ) -> dict[GPTBatchRequestCustomID, GPTBatchRequest]:
        from data_etl_app.models.pipeline_nodes.multi_stage.keyword.keyword_relationship_node import (
            KeywordRelationshipNode,
        )

        return pipeline_context[KeywordRelationshipNode]
