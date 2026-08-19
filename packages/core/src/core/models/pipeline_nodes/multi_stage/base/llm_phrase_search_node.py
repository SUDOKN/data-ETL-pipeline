from __future__ import annotations

import logging
from datetime import datetime

from llm_providers.db_models.gpt_batch_request import (
    GPTBatchRequest,
)
from llm_providers.models.open_ai.gpt_batch_response_blob import (
    GPTBatchResponse,
)
from llm_providers.models.file_objects.prompt import Prompt
from core.models.extraction_schemas.search import LLMSearchResults
from core.models.deferred_extraction.deferred_phrase_extraction_requests import (
    LLMPhraseExtractionMetadata,
    LLMPhraseExtractionRequestMap,
    LLMPhraseExtractionRequestBundle,
)
from core.models.pipeline_nodes.base.base_node import (
    LLMExtractedFieldTypeVar,
    PipelineContext,
)
from core.models.pipeline_nodes.base.base_reconcile_node import (
    ReconcileNode,
)
from core.models.field_types import (
    ExtractionFieldType,
)
from core.models.pipeline_nodes.base.base_llm_extraction_node import (
    BaseLLMExtractionNode,
)
from llm_providers.field_types import BatchRequestIDType
from scraper.models.s3.scraped_text_file import ScrapedTextFile


from llm_providers.services.gpt_batch_request.gpt_batch_request_service import (
    dispatch_gpt_batch_request,
)
from core.services.pipeline_nodes.multi_stage.llm_phrase_search_node_service import (
    create_missing_phrase_search_requests,
    parse_batch_request_result,
)
from typing import ClassVar
from core.models.pipeline_nodes.base.pipeline_stage import PipelineStage

logger = logging.getLogger(__name__)


class LLMPhraseSearchNode(
    BaseLLMExtractionNode[LLMExtractedFieldTypeVar, LLMSearchResults]
):
    stage: ClassVar[PipelineStage] = PipelineStage.phrase_search

    def __new__(cls, *args, **kwargs):
        if cls is LLMPhraseSearchNode:
            raise TypeError(
                "LLMPhraseSearchNode is abstract and cannot be instantiated directly; use a concrete subclass instead."
            )
        return super().__new__(cls)

    def __init__(
        self,
        field_type: LLMExtractedFieldTypeVar,
        next_node: BaseLLMExtractionNode | ReconcileNode,
        phrase_search_prompt: Prompt,
    ):
        super().__init__(
            field_type=field_type,
            next_node=next_node,
        )
        self.phrase_search_prompt = phrase_search_prompt

    async def embed_request_ids(  # prefill folded into this function
        self,
        subject_unique_id: str,
        pipeline_context: PipelineContext,
        metadata: LLMPhraseExtractionMetadata,
        chunked_request_map: LLMPhraseExtractionRequestMap,
        timestamp: datetime,
    ):
        if not chunked_request_map:
            raise ValueError(
                f"Cannot embed req ids for llm phrase search node, "
                f"as chunked_request_map found empty for subject:{subject_unique_id}, field:{self.field_type.name}."
            )

        for (
            chunk_bounds,
            extraction_request_bundle,
        ) in chunked_request_map.items():
            if not extraction_request_bundle.search_sub_bounds:
                raise ValueError(
                    f"Cannot embed req ids for llm phrase search node, as search_sub_bounds "
                    f"is empty for subject:{subject_unique_id}>{chunk_bounds}, field:{self.field_type.name}. "
                    f"Sub-window geometry is written at prefill; an empty list means the "
                    f"deferred field predates search_divisor and must be re-deferred."
                )
            if not extraction_request_bundle.llm_phrase_search_req_ids:
                extraction_request_bundle.llm_phrase_search_req_ids = [
                    self.get_request_custom_id(
                        subject_unique_id=subject_unique_id,
                        field_type=self.field_type,
                        chunk_bounds=chunk_bounds,
                        sub_bounds=sub_bounds,
                        metadata=metadata,
                    )
                    for sub_bounds in extraction_request_bundle.search_sub_bounds
                ]

    def get_embedded_request_ids(
        self,
        subject_unique_id: str,
        chunked_request_map: LLMPhraseExtractionRequestMap,
    ) -> set[BatchRequestIDType]:
        llm_search_req_ids: set[BatchRequestIDType] = set()
        for (
            chunk_bounds,
            extraction_bundle,
        ) in chunked_request_map.items():
            if not extraction_bundle.llm_phrase_search_req_ids:
                raise ValueError(
                    f"Cannot get embedded request ids for subject_unique_id:{subject_unique_id}>{chunk_bounds} as "
                    f"llm_phrase_search_req_ids is empty in the extraction_bundle."
                )

            llm_search_req_ids.update(extraction_bundle.llm_phrase_search_req_ids)
        return llm_search_req_ids

    @staticmethod
    def get_request_custom_id(
        subject_unique_id: str,
        field_type: ExtractionFieldType,
        chunk_bounds: str,
        sub_bounds: str,
        metadata: LLMPhraseExtractionMetadata,
    ) -> BatchRequestIDType:
        return (
            f"{subject_unique_id}>{field_type.name}>llm_search>chunk>{chunk_bounds}>sub>{sub_bounds}>"
            f"{metadata.llm_phrase_search.to_custom_id_segment()}"
        )

    async def create_batch_requests(
        self,
        subject_unique_id: str,
        scraped_text_file: ScrapedTextFile,
        missing_request_ids: set[BatchRequestIDType],
        metadata: LLMPhraseExtractionMetadata,
        chunked_request_map: LLMPhraseExtractionRequestMap,
        pipeline_context: PipelineContext,
        timestamp: datetime,
        eager: bool,
    ) -> list[GPTBatchRequest]:
        """Create batch requests for concept search phase."""

        # create_missing_concept_search_requests only creates batch requests fresh or only missing ones,
        # for e.g., new subject or some batch requests failed earlier and were deleted to allow re-processing
        batch_requests = await create_missing_phrase_search_requests(
            deferred_at=timestamp,
            field_type=self.field_type,
            missing_search_req_ids=missing_request_ids,
            chunked_request_map=chunked_request_map,
            subject_unique_id=subject_unique_id,
            subject_text=scraped_text_file.text,
            search_prompt=self.phrase_search_prompt,
            llm_model=metadata.llm_phrase_search.llm_model,
            model_params=metadata.llm_phrase_search.model_params,
            eager=eager,
        )

        return batch_requests

    @staticmethod
    async def get_result(
        subject_unique_id: str,
        field_type: ExtractionFieldType,
        chunk_bounds: str,
        extraction_bundle: LLMPhraseExtractionRequestBundle,
        completed_request_map: dict[BatchRequestIDType, GPTBatchRequest],
        timestamp: datetime,  # for recording errors
    ) -> LLMSearchResults:
        return await parse_batch_request_result(
            subject_unique_id=subject_unique_id,
            field_type=field_type,
            chunk_bounds=chunk_bounds,
            extraction_bundle=extraction_bundle,
            all_phrase_search_req_responses_map=completed_request_map,
            deferred_at=timestamp,
        )

    async def dispatch_batch_request(
        self,
        gpt_batch_request: GPTBatchRequest,
        metadata: LLMPhraseExtractionMetadata,
    ) -> GPTBatchResponse:
        return await dispatch_gpt_batch_request(
            gpt_batch_request=gpt_batch_request,
            gpt_model=metadata.llm_phrase_search.llm_model,
        )
