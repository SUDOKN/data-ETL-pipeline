from __future__ import annotations
import logging
from typing import TYPE_CHECKING

from packages.core.src.core.models.deferred_extraction.deferred_phrase_extraction_requests import (
    LLMPhraseExtractionMetadata,
)
from packages.core.src.core.models.file_objects.prompt import Prompt
from data_etl_app.models.pipeline_nodes.multi_stage.keyword.base.keyword_phrase_search_node import (
    KeywordPhraseSearchNode,
)
from packages.core.src.core.models.types_and_enums import (
    KeywordTypeEnum,
    LLMExtractedFieldTypeEnum,
)
from packages.core.src.core.models.field_types import BatchRequestIDType

if TYPE_CHECKING:
    from apps.data_etl_app.src.data_etl_app.models.pipeline_nodes.multi_stage.keyword.contract_product.contract_product_recursive_search_node import (
        ContractProductRecursiveSearchNode,
    )

logger = logging.getLogger(__name__)


class ContractProductPhraseSearchNode(KeywordPhraseSearchNode):
    """Phase 1 for the contract-manufacturing product branch.

    This phase is intentionally SHARED with the pure-product branch
    (:class:`KeywordPhraseSearchNode` bound to ``field_type=KeywordTypeEnum.products``):
    ``get_request_custom_id`` is overridden to always compute the id as if
    ``field_type`` were ``KeywordTypeEnum.products``, regardless of the actual
    ``field_type`` (``contract_products``) this node instance is constructed with.

    Because the underlying GPTBatchRequest is looked up/created purely by custom_id,
    whichever of the two top-level pipelines (``products`` / ``contract_products``)
    runs this phase first creates the request; the other finds it already complete
    and never calls the LLM again for phrase search.
    """

    def __init__(
        self,
        field_type: KeywordTypeEnum,
        search_prompt: Prompt,
        next_node: ContractProductRecursiveSearchNode,
    ):
        super().__init__(
            field_type=field_type,
            search_prompt=search_prompt,
            next_node=next_node,
        )

    @staticmethod
    def get_request_custom_id(
        mfg_etld1: str,
        field_type: LLMExtractedFieldTypeEnum,
        chunk_bounds: str,
        metadata: LLMPhraseExtractionMetadata,
    ) -> BatchRequestIDType:
        # Deliberately ignore the passed field_type and use the shared "products"
        # identity so this phase's custom_id matches the pure-product branch's.
        return KeywordPhraseSearchNode.get_request_custom_id(
            mfg_etld1=mfg_etld1,
            field_type=KeywordTypeEnum.products,
            chunk_bounds=chunk_bounds,
            metadata=metadata,
        )
