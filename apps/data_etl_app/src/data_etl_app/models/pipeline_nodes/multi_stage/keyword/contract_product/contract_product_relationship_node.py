from __future__ import annotations
import logging
from typing import TYPE_CHECKING

from packages.core.src.core.models.db.gpt_batch_request import GPTBatchRequest
from packages.core.src.core.models.deferred_extraction.deferred_phrase_extraction_requests import (
    LLMPhraseExtractionMetadata,
)
from packages.core.src.core.models.file_objects.prompt import Prompt
from data_etl_app.models.pipeline_nodes.base.base_node import PipelineContext
from apps.data_etl_app.src.data_etl_app.models.pipeline_nodes.multi_stage.keyword.contract_product.contract_product_phrase_search_node import (
    ContractProductPhraseSearchNode,
)
from apps.data_etl_app.src.data_etl_app.models.pipeline_nodes.multi_stage.keyword.contract_product.contract_product_recursive_search_node import (
    ContractProductRecursiveSearchNode,
)
from data_etl_app.models.pipeline_nodes.multi_stage.keyword.base.keyword_relationship_node import (
    KeywordRelationshipNode,
)
from packages.core.src.core.models.types_and_enums import (
    KeywordTypeEnum,
    LLMExtractedFieldTypeEnum,
)
from packages.core.src.core.models.field_types import BatchRequestIDType

if TYPE_CHECKING:
    from apps.data_etl_app.src.data_etl_app.models.pipeline_nodes.multi_stage.keyword.contract_product.contract_product_relationship_screening_node import (
        ContractProductRelationshipScreeningNode,
    )

logger = logging.getLogger(__name__)


class ContractProductRelationshipNode(KeywordRelationshipNode):
    """Phase 3 for the contract-manufacturing product branch.

    Shared with the pure-product branch the same way as
    :class:`ContractProductPhraseSearchNode` (custom_id computed as if
    field_type were ``KeywordTypeEnum.products``).
    """

    def __init__(
        self,
        field_type: KeywordTypeEnum,
        phrase_relationship_prompt: Prompt,
        next_node: ContractProductRelationshipScreeningNode,
    ):
        super().__init__(
            field_type=field_type,
            phrase_relationship_prompt=phrase_relationship_prompt,
            next_node=next_node,
        )

    def get_upstream_phrase_search_map(
        self, pipeline_context: PipelineContext
    ) -> dict[BatchRequestIDType, GPTBatchRequest]:
        return pipeline_context[ContractProductPhraseSearchNode]

    def get_upstream_recursive_search_map(
        self, pipeline_context: PipelineContext
    ) -> dict[BatchRequestIDType, GPTBatchRequest]:
        return pipeline_context[ContractProductRecursiveSearchNode]

    @staticmethod
    def get_request_custom_id(
        mfg_etld1: str,
        field_type: LLMExtractedFieldTypeEnum,
        chunk_bounds: str,
        metadata: LLMPhraseExtractionMetadata,
    ) -> BatchRequestIDType:
        return KeywordRelationshipNode.get_request_custom_id(
            mfg_etld1=mfg_etld1,
            field_type=KeywordTypeEnum.products,
            chunk_bounds=chunk_bounds,
            metadata=metadata,
        )
