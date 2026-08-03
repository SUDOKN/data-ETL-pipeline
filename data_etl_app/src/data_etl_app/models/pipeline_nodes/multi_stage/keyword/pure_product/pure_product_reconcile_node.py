from __future__ import annotations

from data_etl_app.models.pipeline_nodes.base.base_node import PipelineContext
from data_etl_app.models.pipeline_nodes.multi_stage.keyword.base.keyword_reconcile_node import (
    KeywordReconcileNode,
)
from core.models.types_and_enums import KeywordTypeEnum


class PureProductReconcileNode(KeywordReconcileNode):
    """Phase 6 for the pure-product branch.

    Reads/writes ``deferred_mfg.products`` / ``mfg.products`` (via
    ``field_type=KeywordTypeEnum.products``) and points the 5 upstream lookups
    at the ``PureProduct*`` sibling node classes.
    """

    def __init__(self, field_type: KeywordTypeEnum) -> None:
        super().__init__(field_type=field_type)

    def get_upstream_search_map(self, pipeline_context: PipelineContext) -> dict:
        from data_etl_app.models.pipeline_nodes.multi_stage.keyword.pure_product.pure_product_phrase_search_node import (
            PureProductPhraseSearchNode,
        )

        return pipeline_context[PureProductPhraseSearchNode]

    def get_upstream_recursive_search_map(
        self, pipeline_context: PipelineContext
    ) -> dict:
        from data_etl_app.models.pipeline_nodes.multi_stage.keyword.pure_product.pure_product_recursive_search_node import (
            PureProductRecursiveSearchNode,
        )

        return pipeline_context[PureProductRecursiveSearchNode]

    def get_upstream_relationship_map(self, pipeline_context: PipelineContext) -> dict:
        from data_etl_app.models.pipeline_nodes.multi_stage.keyword.pure_product.pure_product_relationship_node import (
            PureProductRelationshipNode,
        )

        return pipeline_context[PureProductRelationshipNode]

    def get_upstream_screening_map(self, pipeline_context: PipelineContext) -> dict:
        from data_etl_app.models.pipeline_nodes.multi_stage.keyword.pure_product.pure_product_relationship_screening_node import (
            PureProductRelationshipScreeningNode,
        )

        return pipeline_context[PureProductRelationshipScreeningNode]

    def get_upstream_freehand_grounding_map(
        self, pipeline_context: PipelineContext
    ) -> dict:
        from data_etl_app.models.pipeline_nodes.multi_stage.keyword.pure_product.pure_product_freehand_grounding_node import (
            PureProductFreehandGroundingNode,
        )

        return pipeline_context[PureProductFreehandGroundingNode]
