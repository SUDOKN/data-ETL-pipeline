from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING, Optional

from core.models.db.deferred_manufacturer import DeferredManufacturer
from core.models.db.gpt_batch_request import GPTBatchRequest
from core.models.gpt_batch_response_blob import GPTBatchResponse
from core.models.prompt import Prompt
from core.models.field_types import LLMSearchResults
from core.models.deferred_phrase_extraction_requests import (
    DeferredLLMPhraseExtractionRequests,
    LLMPhraseExtractionMetadata,
    LLMPhraseExtractionRequestMap,
)
from data_etl_app.models.pipeline_nodes.base.base_node import (
    LLMExtractedFieldTypeVar,
    PipelineContext,
)
from data_etl_app.models.pipeline_nodes.base.base_reconcile_node import ReconcileNode
from data_etl_app.models.types_and_enums import (
    LLMExtractedFieldTypeEnum,
)
from data_etl_app.models.pipeline_nodes.base.base_llm_extraction_node import (
    BaseLLMExtractionNode,
)
from open_ai_key_app.models.field_types import GPTBatchRequestCustomID

if TYPE_CHECKING:
    from scraper_app.models.scraped_text_file import ScrapedTextFile


from core.services.gpt_batch_request_service import (
    dispatch_gpt_batch_request,
)
from data_etl_app.services.extraction.deferred_llm_phrase_search_node_service import (
    create_missing_phrase_search_requests,
)

logger = logging.getLogger(__name__)


class LLMPhraseSearchNode(
    BaseLLMExtractionNode[LLMExtractedFieldTypeVar, LLMSearchResults]
):
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
        mfg_etld1: str,
        pipeline_context: PipelineContext,
        metadata: LLMPhraseExtractionMetadata,
        request_map: LLMPhraseExtractionRequestMap,
        timestamp: datetime,
    ):
        if not request_map:
            raise ValueError(
                f"Cannot embed req ids for llm phrase search node, "
                f"as request_map found empty for mfg:{mfg_etld1}, field:{self.field_type.name}."
            )

        for (
            chunk_bounds,
            extraction_request_bundle,
        ) in request_map.items():
            if not extraction_request_bundle.llm_phrase_search_req_id:
                extraction_request_bundle.llm_phrase_search_req_id = (
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
        request_map: LLMPhraseExtractionRequestMap,
    ) -> set[GPTBatchRequestCustomID]:
        llm_search_req_ids: set[GPTBatchRequestCustomID] = set()
        for (
            chunk_bounds,
            extraction_bundle,
        ) in request_map.items():
            if not extraction_bundle.llm_phrase_search_req_id:
                raise ValueError(
                    f"Cannot get embedded request ids for mfg_etld1:{mfg_etld1}>{chunk_bounds} as "
                    f"llm_phrase_search_request is absent in the extraction_bundle."
                )

            llm_search_req_ids.add(extraction_bundle.llm_phrase_search_req_id)
        return llm_search_req_ids

    @staticmethod
    def get_request_custom_id(
        mfg_etld1: str,
        field_type: LLMExtractedFieldTypeEnum,
        chunk_bounds: str,
        metadata: LLMPhraseExtractionMetadata,
    ) -> GPTBatchRequestCustomID:
        return (
            f"{mfg_etld1}>{field_type.name}>llm_search>chunk>{chunk_bounds}>"
            f"{metadata.llm_phrase_search.model_params.to_custom_id_segment(metadata.llm_phrase_search.llm_model.name)}"
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
        """Create batch requests for concept search phase."""

        extraction_requests: Optional[DeferredLLMPhraseExtractionRequests] = getattr(
            deferred_mfg, self.field_type.name
        )
        if not extraction_requests:
            raise ValueError(
                f"create_batch_requests was called for {self.field_type.name} in {__class__.__name__} but no deferred concept extraction exists."
            )
        metadata = extraction_requests.metadata.llm_phrase_search

        # create_missing_concept_search_requests only creates batch requests fresh or only missing ones,
        # for e.g., new mfg or some batch requests failed earlier and were deleted to allow re-processing
        batch_requests = await create_missing_phrase_search_requests(
            deferred_at=timestamp,
            field_type=self.field_type,
            missing_search_req_ids=missing_request_ids,
            chunked_request_map=extraction_requests.chunked_request_map,
            mfg_etld1=deferred_mfg.etld1,
            mfg_text=scraped_text_file.text,
            search_prompt=self.phrase_search_prompt,
            llm_model=metadata.llm_model,
            model_params=metadata.model_params,
            eager=eager,
        )

        return batch_requests

    async def dispatch_batch_request(
        self,
        gpt_batch_request: GPTBatchRequest,
        metadata: LLMPhraseExtractionMetadata,
    ) -> GPTBatchResponse:
        return await dispatch_gpt_batch_request(
            gpt_batch_request=gpt_batch_request,
            gpt_model=metadata.llm_phrase_search.llm_model,
            model_params=metadata.llm_phrase_search.model_params,
        )
