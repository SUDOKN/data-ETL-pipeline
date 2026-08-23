from __future__ import annotations
import logging
from typing import TYPE_CHECKING

from llm_providers.db_models.gpt_batch_request import (
    GPTBatchRequest,
)
from llm_providers.models.file_objects.prompt import Prompt

from core.models.extraction_schemas.mention_collection import MentionWireItem
from core.models.pipeline_nodes import PipelineContext
from data_etl_app.models.pipeline_nodes.multi_stage.keyword.contract_product.contract_product_phrase_search_node import (
    ContractProductPhraseSearchNode,
)
from data_etl_app.models.pipeline_nodes.multi_stage.keyword.contract_product.contract_product_recursive_search_node import (
    ContractProductRecursiveSearchNode,
)
from core.models.pipeline_nodes.multi_stage.keyword.keyword_mention_collection_node import (
    KeywordMentionCollectionNode,
)
from data_etl_app.models.types_and_enums import KeywordTypeEnum
from llm_providers.field_types import BatchRequestIDType
from core.models.extraction_results.llm_phrase_extraction_results_v2 import (
    LLMPhraseExtractionMetadataV2,
)
from core.models.field_types import ExtractionFieldType

if TYPE_CHECKING:
    from data_etl_app.models.pipeline_nodes.multi_stage.keyword.contract_product.contract_product_synthesis_node import (
        ContractProductSynthesisNode,
    )

logger = logging.getLogger(__name__)


class ContractProductMentionCollectionNode(KeywordMentionCollectionNode):
    """v3 phase 3 for the contract-manufacturing product branch. Shared with the
    pure-product branch the same way as :class:`ContractProductPhraseSearchNode`
    (custom_id computed as if field_type were ``KeywordTypeEnum.products``)."""

    def __init__(
        self,
        field_type: KeywordTypeEnum,
        phrase_mention_collection_prompt: Prompt,
        next_node: ContractProductSynthesisNode,
    ):
        super().__init__(
            field_type=field_type,
            phrase_mention_collection_prompt=phrase_mention_collection_prompt,
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
        sub_bounds: str,
        group_index: int,
        metadata: LLMPhraseExtractionMetadataV2,
        group_items: list[MentionWireItem],
        retry_index: int | None = None,
    ) -> BatchRequestIDType:
        # Shared with the pure-product branch (same window, same forms, the same
        # field-agnostic static): computed as if field_type were `products`, so
        # the LLM is asked once and both branches read the one answer — the
        # same sharing the search/recursive nodes do.
        return KeywordMentionCollectionNode.get_request_custom_id(
            subject_unique_id=subject_unique_id,
            field_type=KeywordTypeEnum.products,
            chunk_bounds=chunk_bounds,
            sub_bounds=sub_bounds,
            group_index=group_index,
            metadata=metadata,
            group_items=group_items,
            retry_index=retry_index,
        )
