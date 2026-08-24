from __future__ import annotations
import logging
from typing import TYPE_CHECKING

from llm_providers.db_models.gpt_batch_request import (
    GPTBatchRequest,
)
from llm_providers.models.file_objects.prompt import Prompt
from core.models.pipeline_nodes import PipelineContext
from core.models.pipeline_nodes.multi_stage.keyword.keyword_relationship_screening_node import (
    KeywordRelationshipScreeningNode,
)
from data_etl_app.models.types_and_enums import KeywordTypeEnum
from llm_providers.field_types import BatchRequestIDType

if TYPE_CHECKING:
    from data_etl_app.models.pipeline_nodes.multi_stage.keyword.contract_product.contract_product_reconcile_node import (
        ContractProductReconcileNode,
    )

logger = logging.getLogger(__name__)


class ContractProductRelationshipScreeningNode(KeywordRelationshipScreeningNode):
    """Screening for the contract-manufacturing product branch (v2: downstream
    of the shared freehand grounding pass).

    Unlike the search/recursive-search/relationship phases, this phase is NOT
    shared with the pure-product branch: it uses the real
    ``field_type=KeywordTypeEnum.contract_products`` identity (own custom_id, own
    storage under ``deferred_subject.contract_products``) and the contract-specific
    screening prompt (``product_phrase_screening_contract``).
    """

    def __init__(
        self,
        field_type: KeywordTypeEnum,
        next_node: ContractProductReconcileNode,
        phrase_relationship_screening_prompt: Prompt,
    ):
        super().__init__(
            field_type=field_type,
            phrase_relationship_screening_prompt=phrase_relationship_screening_prompt,
            next_node=next_node,
        )

    def get_upstream_mention_collection_map(
        self, pipeline_context: PipelineContext
    ) -> dict[BatchRequestIDType, GPTBatchRequest]:
        from data_etl_app.models.pipeline_nodes.multi_stage.keyword.contract_product.contract_product_mention_collection_node import (
            ContractProductMentionCollectionNode,
        )

        return pipeline_context[ContractProductMentionCollectionNode]

    def get_upstream_synthesis_map(
        self, pipeline_context: PipelineContext
    ) -> dict[BatchRequestIDType, GPTBatchRequest]:
        from data_etl_app.models.pipeline_nodes.multi_stage.keyword.contract_product.contract_product_synthesis_node import (
            ContractProductSynthesisNode,
        )

        return pipeline_context[ContractProductSynthesisNode]

    def get_upstream_freehand_grounding_map(
        self, pipeline_context: PipelineContext
    ) -> dict[BatchRequestIDType, GPTBatchRequest]:
        from data_etl_app.models.pipeline_nodes.multi_stage.keyword.contract_product.contract_product_freehand_grounding_node import (
            ContractProductFreehandGroundingNode,
        )

        return pipeline_context[ContractProductFreehandGroundingNode]
