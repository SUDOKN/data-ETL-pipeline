from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING, Optional

from core.models.db.deferred_manufacturer import DeferredManufacturer
from core.models.db.gpt_batch_request import GPTBatchRequest
from core.models.deferred_keyword_extraction import (
    DeferredKeywordExtractionRequests,
    KeywordExtractionRequestMap,
)
from core.models.field_types import LLMFreehandGroundingResults
from core.models.gpt_batch_response_blob import GPTBatchResponse
from core.models.keyword_extraction_results import KeywordExtractionMetadata
from core.models.prompt import Prompt
from core.services.gpt_batch_request_service import dispatch_gpt_batch_request
from data_etl_app.models.pipeline_nodes.base.base_llm_extraction_node import (
    BaseLLMExtractionNode,
)
from data_etl_app.models.pipeline_nodes.base.base_node import (
    LLMExtractedFieldTypeVar,
    PipelineContext,
)
from data_etl_app.models.pipeline_nodes.base.base_reconcile_node import ReconcileNode
from data_etl_app.models.types_and_enums import LLMExtractedFieldTypeEnum
from data_etl_app.services.extraction.deferred_llm_freehand_grounding_service import (
    create_missing_phrase_freehand_grounding_requests,
)
from open_ai_key_app.models.field_types import GPTBatchRequestCustomID

if TYPE_CHECKING:
    from scraper_app.models.scraped_text_file import ScrapedTextFile

logger = logging.getLogger(__name__)


class LLMPhraseFreehandGroundingNode(
    BaseLLMExtractionNode[LLMExtractedFieldTypeVar, LLMFreehandGroundingResults]
):
    def __init__(
        self,
        field_type: LLMExtractedFieldTypeVar,
        next_node: BaseLLMExtractionNode | ReconcileNode,
        phrase_freehand_grounding_prompt: Prompt,
    ):
        super().__init__(field_type=field_type, next_node=next_node)
        self.phrase_freehand_grounding_prompt = phrase_freehand_grounding_prompt

    def get_upstream_phrase_relationship_map(
        self, pipeline_context: PipelineContext
    ) -> dict[GPTBatchRequestCustomID, GPTBatchRequest]:
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement get_upstream_phrase_relationship_map"
        )

    def get_upstream_phrase_screening_map(
        self, pipeline_context: PipelineContext
    ) -> dict[GPTBatchRequestCustomID, GPTBatchRequest]:
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement get_upstream_phrase_screening_map"
        )

    async def embed_request_ids(
        self,
        mfg_etld1: str,
        pipeline_context: PipelineContext,
        metadata: KeywordExtractionMetadata,
        request_map: KeywordExtractionRequestMap,
        timestamp: datetime,
    ):
        if not request_map:
            raise ValueError(
                f"Cannot embed req ids for llm freehand grounding node, "
                f"as request_map found empty for mfg:{mfg_etld1}, field:{self.field_type.name}."
            )

        for chunk_bounds, extraction_request_bundle in request_map.items():
            if not extraction_request_bundle.llm_phrase_freehand_grounding_req_id:
                extraction_request_bundle.llm_phrase_freehand_grounding_req_id = (
                    self.get_request_custom_id(
                        mfg_etld1=mfg_etld1,
                        field_type=self.field_type,
                        chunk_bounds=chunk_bounds,
                        metadata=metadata,
                    )
                )

    def get_embedded_request_ids(
        self,
        mfg_etld1: str,
        request_map: KeywordExtractionRequestMap,
    ) -> set[GPTBatchRequestCustomID]:
        req_ids: set[GPTBatchRequestCustomID] = set()
        for chunk_bounds, extraction_bundle in request_map.items():
            if not extraction_bundle.llm_phrase_freehand_grounding_req_id:
                raise ValueError(
                    f"Cannot get embedded request ids for mfg_etld1:{mfg_etld1}>{chunk_bounds} as "
                    f"llm_phrase_freehand_grounding_req_id is absent in the extraction_bundle."
                )
            req_ids.add(extraction_bundle.llm_phrase_freehand_grounding_req_id)
        return req_ids

    @staticmethod
    def get_request_custom_id(
        mfg_etld1: str,
        field_type: LLMExtractedFieldTypeEnum,
        chunk_bounds: str,
        metadata: KeywordExtractionMetadata,
    ) -> GPTBatchRequestCustomID:
        return (
            f"{mfg_etld1}>{field_type.name}>llm_phrase_freehand_grounding>chunk>{chunk_bounds}>"
            f"{metadata.llm_phrase_freehand_grounding.model_params.to_custom_id_segment(metadata.llm_phrase_freehand_grounding.llm_model.name)}"
        )

    async def create_batch_requests(
        self,
        missing_request_ids: set[GPTBatchRequestCustomID],
        deferred_mfg: DeferredManufacturer,
        scraped_text_file: ScrapedTextFile,
        timestamp: datetime,
        pipeline_context: PipelineContext,
        eager: bool,
    ) -> list[GPTBatchRequest]:
        mfg_name = pipeline_context.mfg_name
        if not mfg_name:
            raise ValueError(
                f"phrase_freehand_grounding_node.create_batch_requests was called for {self.field_type.name} in {self.__class__.__name__} but pipeline_context.mfg_name is not set."
            )

        extraction_requests: Optional[DeferredKeywordExtractionRequests] = getattr(
            deferred_mfg, self.field_type.name
        )
        if not extraction_requests:
            raise ValueError(
                f"phrase_freehand_grounding_node.create_batch_requests was called for {self.field_type.name} in {self.__class__.__name__} but no deferred extraction exists."
            )
        metadata = extraction_requests.metadata.llm_phrase_freehand_grounding

        return await create_missing_phrase_freehand_grounding_requests(
            deferred_at=timestamp,
            mfg_etld1=deferred_mfg.etld1,
            mfg_name=mfg_name,
            field_type=self.field_type,
            missing_phrase_freehand_grounding_req_ids=missing_request_ids,
            chunked_request_map=extraction_requests.chunked_request_map,
            phrase_freehand_grounding_prompt=self.phrase_freehand_grounding_prompt,
            llm_phrase_relationship_gpt_request_map=self.get_upstream_phrase_relationship_map(
                pipeline_context
            ),
            llm_phrase_screening_gpt_request_map=self.get_upstream_phrase_screening_map(
                pipeline_context
            ),
            llm_model=metadata.llm_model,
            model_params=metadata.model_params,
            eager=eager,
        )

    async def dispatch_batch_request(
        self,
        gpt_batch_request: GPTBatchRequest,
        metadata: KeywordExtractionMetadata,
    ) -> GPTBatchResponse:
        return await dispatch_gpt_batch_request(
            gpt_batch_request=gpt_batch_request,
            gpt_model=metadata.llm_phrase_freehand_grounding.llm_model,
            model_params=metadata.llm_phrase_freehand_grounding.model_params,
        )
