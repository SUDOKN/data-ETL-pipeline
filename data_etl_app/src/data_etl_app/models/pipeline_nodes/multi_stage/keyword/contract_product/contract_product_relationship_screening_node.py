from __future__ import annotations
import logging
from typing import TYPE_CHECKING

from core.models.db.gpt_batch_request import GPTBatchRequest
from core.models.file_objects.prompt import Prompt
from data_etl_app.models.pipeline_nodes.base.base_node import PipelineContext
from data_etl_app.models.pipeline_nodes.multi_stage.base.llm_phrase_relationship_screening_node import (
    LLMPhraseRelationshipScreeningNode,
)
from data_etl_app.models.types_and_enums import KeywordTypeEnum
from open_ai_key_app.models.field_types import GPTBatchRequestCustomID

if TYPE_CHECKING:
    from data_etl_app.models.pipeline_nodes.multi_stage.keyword.contract_product.contract_product_freehand_grounding_node import (
        ContractProductFreehandGroundingNode,
    )

logger = logging.getLogger(__name__)


class ContractProductRelationshipScreeningNode(
    LLMPhraseRelationshipScreeningNode[KeywordTypeEnum]
):
    """Phase 4 for the contract-manufacturing product branch.

    Unlike the search/recursive-search/relationship phases, this phase is NOT
    shared with the pure-product branch: it uses the real
    ``field_type=KeywordTypeEnum.contract_products`` identity (own custom_id, own
    storage under ``deferred_mfg.contract_products``) and the contract-specific
    screening prompt (``product_phrase_screening_contract``).
    """

    def __init__(
        self,
        field_type: KeywordTypeEnum,
        next_node: ContractProductFreehandGroundingNode,
        phrase_relationship_screening_prompt: Prompt,
    ):
        super().__init__(
            field_type=field_type,
            phrase_relationship_screening_prompt=phrase_relationship_screening_prompt,
            next_node=next_node,
        )

    def get_upstream_phrase_relationship_map(
        self, pipeline_context: PipelineContext
    ) -> dict[GPTBatchRequestCustomID, GPTBatchRequest]:
        from data_etl_app.models.pipeline_nodes.multi_stage.keyword.contract_product.contract_product_relationship_node import (
            ContractProductRelationshipNode,
        )

        return pipeline_context[ContractProductRelationshipNode]
