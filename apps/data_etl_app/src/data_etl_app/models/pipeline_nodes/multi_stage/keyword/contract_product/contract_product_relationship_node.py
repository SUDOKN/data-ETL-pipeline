from __future__ import annotations
import logging
from typing import TYPE_CHECKING

from llm_providers.db_models.gpt_batch_request import (
    GPTBatchRequest,
)
from core.models.deferred_extraction.deferred_phrase_extraction_requests import (
    LLMPhraseExtractionMetadata,
)
from llm_providers.models.file_objects.prompt import Prompt
from core.models.pipeline_nodes import PipelineContext
from data_etl_app.models.pipeline_nodes.multi_stage.keyword.contract_product.contract_product_phrase_search_node import (
    ContractProductPhraseSearchNode,
)
from data_etl_app.models.pipeline_nodes.multi_stage.keyword.contract_product.contract_product_recursive_search_node import (
    ContractProductRecursiveSearchNode,
)
from core.models.pipeline_nodes.multi_stage.keyword.keyword_relationship_node import (
    KeywordRelationshipNode,
)
from core.models.field_types import ExtractionFieldType
from data_etl_app.models.types_and_enums import (
    KeywordTypeEnum,
)
from llm_providers.field_types import BatchRequestIDType

if TYPE_CHECKING:
    from data_etl_app.models.pipeline_nodes.multi_stage.keyword.contract_product.contract_product_relationship_screening_node import (
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
        subject_unique_id: str,
        field_type: ExtractionFieldType,
        chunk_bounds: str,
        metadata: LLMPhraseExtractionMetadata,
    ) -> BatchRequestIDType:
        return KeywordRelationshipNode.get_request_custom_id(
            subject_unique_id=subject_unique_id,
            field_type=KeywordTypeEnum.products,
            chunk_bounds=chunk_bounds,
            metadata=metadata,
        )
