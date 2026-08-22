from __future__ import annotations
import logging
from typing import TYPE_CHECKING

from llm_providers.db_models.gpt_batch_request import (
    GPTBatchRequest,
)
from llm_providers.models.file_objects.prompt import Prompt
from core.models.pipeline_nodes import PipelineContext
from data_etl_app.models.pipeline_nodes.multi_stage.keyword.equipment.equipment_phrase_search_node import (
    EquipmentPhraseSearchNode,
)
from data_etl_app.models.pipeline_nodes.multi_stage.keyword.equipment.equipment_recursive_search_node import (
    EquipmentRecursiveSearchNode,
)
from core.models.pipeline_nodes.multi_stage.keyword.keyword_mention_collection_node import (
    KeywordMentionCollectionNode,
)
from data_etl_app.models.types_and_enums import KeywordTypeEnum
from llm_providers.field_types import BatchRequestIDType

if TYPE_CHECKING:
    from data_etl_app.models.pipeline_nodes.multi_stage.keyword.equipment.equipment_freehand_grounding_node import (
        EquipmentFreehandGroundingNode,
    )

logger = logging.getLogger(__name__)


class EquipmentMentionCollectionNode(KeywordMentionCollectionNode):
    """v3 phase 3: collect mentions of the searched equipment forms."""

    def __init__(
        self,
        field_type: KeywordTypeEnum,
        phrase_mention_collection_prompt: Prompt,
        next_node: EquipmentFreehandGroundingNode,
    ):
        super().__init__(
            field_type=field_type,
            phrase_mention_collection_prompt=phrase_mention_collection_prompt,
            next_node=next_node,
        )

    def get_upstream_phrase_search_map(
        self, pipeline_context: PipelineContext
    ) -> dict[BatchRequestIDType, GPTBatchRequest]:
        return pipeline_context[EquipmentPhraseSearchNode]

    def get_upstream_recursive_search_map(
        self, pipeline_context: PipelineContext
    ) -> dict[BatchRequestIDType, GPTBatchRequest]:
        return pipeline_context[EquipmentRecursiveSearchNode]
