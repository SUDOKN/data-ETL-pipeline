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
    from data_etl_app.models.pipeline_nodes.multi_stage.keyword.contract_product.contract_product_relationship_screening_node import (
        ContractProductRelationshipScreeningNode,
    )

logger = logging.getLogger(__name__)


class ContractProductFreehandGroundingNode(KeywordFreehandGroundingNode):
    """The contract-manufacturing product branch's enumeration pass (v2: ahead
    of screening).

    Reuses the SAME merged freehand grounding prompt as the pure-product branch
    (``product_phrase_freehand_grounding``), but runs as its own request/custom_id
    (own ``field_type=KeywordTypeEnum.contract_products`` identity) — the
    pure-vs-contract distinction lives only in screening now.
    """

    def __init__(
        self,
        field_type: KeywordTypeEnum,
        next_node: ContractProductRelationshipScreeningNode,
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
        from data_etl_app.models.pipeline_nodes.multi_stage.keyword.contract_product.contract_product_relationship_node import (
            ContractProductRelationshipNode,
        )

        return pipeline_context[ContractProductRelationshipNode]
