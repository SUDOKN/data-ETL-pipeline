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
from core.models.pipeline_nodes.multi_stage.keyword.keyword_recursive_search_node import (
    KeywordRecursiveSearchNode,
)
from data_etl_app.models.types_and_enums import KeywordTypeEnum
from llm_providers.field_types import BatchRequestIDType

if TYPE_CHECKING:
    from data_etl_app.models.pipeline_nodes.multi_stage.keyword.equipment.equipment_relationship_node import (
        EquipmentRelationshipNode,
    )

logger = logging.getLogger(__name__)


class EquipmentRecursiveSearchNode(KeywordRecursiveSearchNode):
    """Phase 2: recursive search for equipment phrases."""

    def __init__(
        self,
        field_type: KeywordTypeEnum,
        next_node: EquipmentRelationshipNode,
        second_search_prompt: Prompt,
    ):
        super().__init__(
            field_type=field_type,
            next_node=next_node,
            second_search_prompt=second_search_prompt,
        )

    def get_upstream_first_search_map(
        self, pipeline_context: PipelineContext
    ) -> dict[BatchRequestIDType, GPTBatchRequest]:
        return pipeline_context[EquipmentPhraseSearchNode]
