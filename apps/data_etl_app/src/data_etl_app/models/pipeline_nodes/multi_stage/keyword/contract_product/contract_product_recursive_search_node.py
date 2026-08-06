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
from core.models.pipeline_nodes.multi_stage.keyword.keyword_recursive_search_node import (
    KeywordRecursiveSearchNode,
)
from core.models.types_and_enums import (
    KeywordTypeEnum,
    LLMExtractedFieldTypeEnum,
)
from llm_providers.field_types import BatchRequestIDType

if TYPE_CHECKING:
    from data_etl_app.models.pipeline_nodes.multi_stage.keyword.contract_product.contract_product_relationship_node import (
        ContractProductRelationshipNode,
    )

logger = logging.getLogger(__name__)


class ContractProductRecursiveSearchNode(KeywordRecursiveSearchNode):
    """Phase 2 for the contract-manufacturing product branch.

    Shared with the pure-product branch the same way as
    :class:`ContractProductPhraseSearchNode` (custom_id computed as if
    field_type were ``KeywordTypeEnum.products``).
    """

    def __init__(
        self,
        field_type: KeywordTypeEnum,
        next_node: ContractProductRelationshipNode,
        second_search_prompt: Prompt,
    ):
        super().__init__(
            field_type=field_type,
            next_node=next_node,
            second_search_prompt=second_search_prompt,
        )

    def get_upstream_first_search_map(
        self, pipeline_context: PipelineContext
    ) -> dict[BatchRequestIDType, GPTBatchRequest]:
        return pipeline_context[ContractProductPhraseSearchNode]

    @staticmethod
    def get_request_custom_id(
        mfg_etld1: str,
        field_type: LLMExtractedFieldTypeEnum,
        chunk_bounds: str,
        round_index: int,
        metadata: LLMPhraseExtractionMetadata,
    ) -> BatchRequestIDType:
        return KeywordRecursiveSearchNode.get_request_custom_id(
            subject_unique_id=mfg_etld1,
            field_type=KeywordTypeEnum.products,
            chunk_bounds=chunk_bounds,
            round_index=round_index,
            metadata=metadata,
        )
