from __future__ import annotations

from core.models.pipeline_nodes import PipelineContext
from core.models.pipeline_nodes.multi_stage.keyword.keyword_reconcile_node import (
    KeywordReconcileNode,
)
from data_etl_app.models.types_and_enums import KeywordTypeEnum


class ContractProductReconcileNode(KeywordReconcileNode):
    """Phase 6 for the contract-manufacturing product branch.

    Reads/writes the SAME 5-phase shape as :class:`KeywordReconcileNode`, but
    points the upstream lookups at the ``Contract*`` node classes and, via
    ``field_type=KeywordTypeEnum.contract_products``, reads/writes
    ``deferred_subject.contract_products`` / ``subject.contract_products`` instead of
    ``products``.
    """

    def __init__(self, field_type: KeywordTypeEnum) -> None:
        super().__init__(field_type=field_type)

    def get_upstream_search_map(self, pipeline_context: PipelineContext) -> dict:
        from data_etl_app.models.pipeline_nodes.multi_stage.keyword.contract_product.contract_product_phrase_search_node import (
            ContractProductPhraseSearchNode,
        )

        return pipeline_context[ContractProductPhraseSearchNode]

    def get_upstream_recursive_search_map(
        self, pipeline_context: PipelineContext
    ) -> dict:
        from data_etl_app.models.pipeline_nodes.multi_stage.keyword.contract_product.contract_product_recursive_search_node import (
            ContractProductRecursiveSearchNode,
        )

        return pipeline_context[ContractProductRecursiveSearchNode]

    def get_upstream_relationship_map(self, pipeline_context: PipelineContext) -> dict:
        from data_etl_app.models.pipeline_nodes.multi_stage.keyword.contract_product.contract_product_relationship_node import (
            ContractProductRelationshipNode,
        )

        return pipeline_context[ContractProductRelationshipNode]

    def get_upstream_screening_map(self, pipeline_context: PipelineContext) -> dict:
        from data_etl_app.models.pipeline_nodes.multi_stage.keyword.contract_product.contract_product_relationship_screening_node import (
            ContractProductRelationshipScreeningNode,
        )

        return pipeline_context[ContractProductRelationshipScreeningNode]

    def get_upstream_freehand_grounding_map(
        self, pipeline_context: PipelineContext
    ) -> dict:
        from data_etl_app.models.pipeline_nodes.multi_stage.keyword.contract_product.contract_product_freehand_grounding_node import (
            ContractProductFreehandGroundingNode,
        )

        return pipeline_context[ContractProductFreehandGroundingNode]
