from __future__ import annotations
import logging
from typing import TYPE_CHECKING

from llm_providers.db_models.gpt_batch_request import (
    GPTBatchRequest,
)
from llm_providers.models.file_objects.prompt import Prompt
from core.models.pipeline_nodes import PipelineContext
from core.models.pipeline_nodes.multi_stage.keyword.keyword_freehand_grounding_node import (
    KeywordFreehandGroundingNode,
)
from data_etl_app.models.types_and_enums import KeywordTypeEnum
from llm_providers.field_types import BatchRequestIDType

if TYPE_CHECKING:
    from data_etl_app.models.pipeline_nodes.multi_stage.keyword.equipment.equipment_reconcile_node import (
        EquipmentReconcileNode,
    )

logger = logging.getLogger(__name__)


class EquipmentFreehandGroundingNode(KeywordFreehandGroundingNode):
    """Phase 5: freehand grounding (tagging) of screened equipment phrases."""

    def __init__(
        self,
        field_type: KeywordTypeEnum,
        next_node: EquipmentReconcileNode,
        phrase_freehand_grounding_prompt: Prompt,
    ):
        super().__init__(
            field_type=field_type,
            phrase_freehand_grounding_prompt=phrase_freehand_grounding_prompt,
            next_node=next_node,
        )

    def get_upstream_phrase_relationship_map(
        self, pipeline_context: PipelineContext
    ) -> dict[BatchRequestIDType, GPTBatchRequest]:
        from data_etl_app.models.pipeline_nodes.multi_stage.keyword.equipment.equipment_relationship_node import (
            EquipmentRelationshipNode,
        )

        return pipeline_context[EquipmentRelationshipNode]

    def get_upstream_phrase_screening_map(
        self, pipeline_context: PipelineContext
    ) -> dict[BatchRequestIDType, GPTBatchRequest]:
        from data_etl_app.models.pipeline_nodes.multi_stage.keyword.equipment.equipment_relationship_screening_node import (
            EquipmentRelationshipScreeningNode,
        )

        return pipeline_context[EquipmentRelationshipScreeningNode]
