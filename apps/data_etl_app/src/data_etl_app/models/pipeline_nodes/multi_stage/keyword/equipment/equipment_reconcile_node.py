from __future__ import annotations

from packages.core.src.core.models.pipeline_nodes import PipelineContext
from packages.core.src.core.models.pipeline_nodes.multi_stage.keyword.keyword_reconcile_node import (
    KeywordReconcileNode,
)
from packages.core.src.core.models.types_and_enums import KeywordTypeEnum


class EquipmentReconcileNode(KeywordReconcileNode):
    """Phase 6: aggregate & write final equipment results.

    Reads/writes ``deferred_mfg.equipments`` / ``mfg.equipments`` (via
    ``field_type=KeywordTypeEnum.equipments``) and points the 5 upstream lookups
    at the ``Equipment*`` sibling node classes.
    """

    def __init__(self, field_type: KeywordTypeEnum) -> None:
        super().__init__(field_type=field_type)

    def get_upstream_search_map(self, pipeline_context: PipelineContext) -> dict:
        from apps.data_etl_app.src.data_etl_app.models.pipeline_nodes.multi_stage.keyword.equipment.equipment_phrase_search_node import (
            EquipmentPhraseSearchNode,
        )

        return pipeline_context[EquipmentPhraseSearchNode]

    def get_upstream_recursive_search_map(
        self, pipeline_context: PipelineContext
    ) -> dict:
        from apps.data_etl_app.src.data_etl_app.models.pipeline_nodes.multi_stage.keyword.equipment.equipment_recursive_search_node import (
            EquipmentRecursiveSearchNode,
        )

        return pipeline_context[EquipmentRecursiveSearchNode]

    def get_upstream_relationship_map(self, pipeline_context: PipelineContext) -> dict:
        from apps.data_etl_app.src.data_etl_app.models.pipeline_nodes.multi_stage.keyword.equipment.equipment_relationship_node import (
            EquipmentRelationshipNode,
        )

        return pipeline_context[EquipmentRelationshipNode]

    def get_upstream_screening_map(self, pipeline_context: PipelineContext) -> dict:
        from apps.data_etl_app.src.data_etl_app.models.pipeline_nodes.multi_stage.keyword.equipment.equipment_relationship_screening_node import (
            EquipmentRelationshipScreeningNode,
        )

        return pipeline_context[EquipmentRelationshipScreeningNode]

    def get_upstream_freehand_grounding_map(
        self, pipeline_context: PipelineContext
    ) -> dict:
        from apps.data_etl_app.src.data_etl_app.models.pipeline_nodes.multi_stage.keyword.equipment.equipment_freehand_grounding_node import (
            EquipmentFreehandGroundingNode,
        )

        return pipeline_context[EquipmentFreehandGroundingNode]
