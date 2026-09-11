from __future__ import annotations
import logging
from typing import TYPE_CHECKING

from llm_providers.db_models.gpt_batch_request import (
    GPTBatchRequest,
)
from llm_providers.models.file_objects.prompt import Prompt

from core.models.extraction_schemas.synthesis import SynthesisRecordInput
from core.models.pipeline_nodes import PipelineContext
from data_etl_app.models.pipeline_nodes.multi_stage.keyword.contract_product.contract_product_phrase_search_node import (
    ContractProductPhraseSearchNode,
)
from data_etl_app.models.pipeline_nodes.multi_stage.keyword.contract_product.contract_product_recursive_search_node import (
    ContractProductRecursiveSearchNode,
)
from core.models.pipeline_nodes.multi_stage.keyword.keyword_synthesis_node import (
    KeywordSynthesisNode,
)
from data_etl_app.models.types_and_enums import KeywordTypeEnum
from llm_providers.field_types import BatchRequestIDType
from core.models.extraction_results.llm_phrase_extraction_results import (
    LLMPhraseExtractionMetadata,
)
from core.models.field_types import ExtractionFieldType

if TYPE_CHECKING:
    from data_etl_app.models.pipeline_nodes.multi_stage.keyword.contract_product.contract_product_freehand_grounding_node import (
        ContractProductFreehandGroundingNode,
    )

logger = logging.getLogger(__name__)


class ContractProductSynthesisNode(KeywordSynthesisNode):
    """v3 phase 3.2 for the contract-manufacturing product branch. Shared with
    the pure-product branch the same way as :class:`ContractProductPhraseSearchNode`
    (custom_id computed as if field_type were ``KeywordTypeEnum.products``):
    the same search answers fold to the same records, so the LLM is asked once
    and both branches read the one answer.

    INV-3 (the synthesis eval's contract byte-copy invariant) failed on run
    20260905T213127 — 510 (chunk, group) pairs where this branch's synthesis
    differed from products'. The cause was not here: because every id this
    node embeds is already answered (products asked first), the recursive
    base's eager loop used to exit before this node's assessment pass, so no
    retry ids were recorded on the contract bundle and the read path saw
    group answers only, while products' read path resolved group vs retry.
    Fixed 2026-09-10 in ``BaseLLMRecursiveExtractionNode.execute`` (the loop
    keeps going while a pass embeds new ids); this node's code is unchanged
    and stays a pure identity map onto products' requests."""

    def __init__(
        self,
        field_type: KeywordTypeEnum,
        phrase_synthesis_prompt: Prompt,
        next_node: ContractProductFreehandGroundingNode,
    ):
        super().__init__(
            field_type=field_type,
            phrase_synthesis_prompt=phrase_synthesis_prompt,
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
        group_index: int,
        metadata: LLMPhraseExtractionMetadata,
        group_records: list[SynthesisRecordInput],
        retry_index: int | None = None,
    ) -> BatchRequestIDType:
        return KeywordSynthesisNode.get_request_custom_id(
            subject_unique_id=subject_unique_id,
            field_type=KeywordTypeEnum.products,
            chunk_bounds=chunk_bounds,
            group_index=group_index,
            metadata=metadata,
            group_records=group_records,
            retry_index=retry_index,
        )
