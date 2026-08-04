from __future__ import annotations
import logging
from typing import TYPE_CHECKING

from packages.core.src.core.models.db.gpt_batch_request import GPTBatchRequest
from packages.core.src.core.models.file_objects.prompt import Prompt
from data_etl_app.models.pipeline_nodes.base.base_node import PipelineContext
from apps.data_etl_app.src.data_etl_app.models.pipeline_nodes.multi_stage.keyword.equipment.equipment_phrase_search_node import (
    EquipmentPhraseSearchNode,
)
from apps.data_etl_app.src.data_etl_app.models.pipeline_nodes.multi_stage.keyword.equipment.equipment_recursive_search_node import (
    EquipmentRecursiveSearchNode,
)
from data_etl_app.models.pipeline_nodes.multi_stage.keyword.base.keyword_relationship_node import (
    KeywordRelationshipNode,
)
from packages.core.src.core.models.types_and_enums import KeywordTypeEnum
from packages.core.src.core.models.field_types import BatchRequestIDType

if TYPE_CHECKING:
    from apps.data_etl_app.src.data_etl_app.models.pipeline_nodes.multi_stage.keyword.equipment.equipment_relationship_screening_node import (
        EquipmentRelationshipScreeningNode,
    )

logger = logging.getLogger(__name__)


class EquipmentRelationshipNode(KeywordRelationshipNode):
    """Phase 3: distill searched phrases into equipment relationships."""

    def __init__(
        self,
        field_type: KeywordTypeEnum,
        phrase_relationship_prompt: Prompt,
        next_node: EquipmentRelationshipScreeningNode,
    ):
        super().__init__(
            field_type=field_type,
            phrase_relationship_prompt=phrase_relationship_prompt,
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
