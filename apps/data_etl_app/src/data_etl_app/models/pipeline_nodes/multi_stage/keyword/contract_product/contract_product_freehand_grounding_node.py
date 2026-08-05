from __future__ import annotations
import logging
from typing import TYPE_CHECKING

from packages.llm_providers.src.llm_providers.db_models.gpt_batch_request import (
    GPTBatchRequest,
)
from packages.llm_providers.src.llm_providers.models.file_objects.prompt import Prompt
from packages.core.src.core.models.pipeline_nodes import PipelineContext
from packages.core.src.core.models.pipeline_nodes.multi_stage.base.llm_phrase_freehand_grounding_node import (
    LLMPhraseFreehandGroundingNode,
)
from packages.core.src.core.models.types_and_enums import KeywordTypeEnum
from packages.llm_providers.src.llm_providers.field_types import BatchRequestIDType

if TYPE_CHECKING:
    from apps.data_etl_app.src.data_etl_app.models.pipeline_nodes.multi_stage.keyword.contract_product.contract_product_reconcile_node import (
        ContractProductReconcileNode,
    )

logger = logging.getLogger(__name__)


class ContractProductFreehandGroundingNode(
    LLMPhraseFreehandGroundingNode[KeywordTypeEnum]
):
    """Phase 5 for the contract-manufacturing product branch.

    Reuses the SAME freehand grounding prompt as the pure-product branch
    (``product_phrase_freehand_grounding``), but runs as its own request/custom_id
    (own ``field_type=KeywordTypeEnum.contract_products`` identity) since its input
    (the contract-screened phrases) differs from the pure branch's.
    """

    def __init__(
        self,
        field_type: KeywordTypeEnum,
        next_node: ContractProductReconcileNode,
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
        from apps.data_etl_app.src.data_etl_app.models.pipeline_nodes.multi_stage.keyword.contract_product.contract_product_relationship_node import (
            ContractProductRelationshipNode,
        )

        return pipeline_context[ContractProductRelationshipNode]

    def get_upstream_phrase_screening_map(
        self, pipeline_context: PipelineContext
    ) -> dict[BatchRequestIDType, GPTBatchRequest]:
        from apps.data_etl_app.src.data_etl_app.models.pipeline_nodes.multi_stage.keyword.contract_product.contract_product_relationship_screening_node import (
            ContractProductRelationshipScreeningNode,
        )

        return pipeline_context[ContractProductRelationshipScreeningNode]
