from __future__ import annotations
import logging
from datetime import datetime
from typing import TYPE_CHECKING, Optional

from core.models.db.deferred_manufacturer import DeferredManufacturer
from core.models.db.gpt_batch_request import GPTBatchRequest
from core.models.batch_request_objects.gpt_batch_response_blob import GPTBatchResponse
from core.models.deferred_extraction.deferred_phrase_extraction_requests import (
    DeferredLLMPhraseExtractionRequests,
    LLMPhraseExtractionRequestMap,
    LLMPhraseExtractionMetadata,
)
from core.models.field_types import LLMPhraseRelationshipResults
from core.models.file_objects.prompt import Prompt
from data_etl_app.models.types_and_enums import LLMExtractedFieldTypeEnum
from data_etl_app.models.pipeline_nodes.base.base_node import (
    LLMExtractedFieldTypeVar,
    PipelineContext,
)
from data_etl_app.models.pipeline_nodes.base.base_reconcile_node import ReconcileNode
from data_etl_app.models.pipeline_nodes.base.base_llm_extraction_node import (
    BaseLLMExtractionNode,
)
from open_ai_key_app.models.field_types import GPTBatchRequestCustomID

if TYPE_CHECKING:
    from scraper_app.models.scraped_text_file import ScrapedTextFile

from core.services.gpt_batch_request_service import (
    dispatch_gpt_batch_request,
)
from data_etl_app.services.extraction.deferred_llm_phrase_relationship_node_service import (
    create_missing_phrase_relationship_requests,
)

logger = logging.getLogger(__name__)


class LLMPhraseRelationshipNode(
    BaseLLMExtractionNode[LLMExtractedFieldTypeVar, LLMPhraseRelationshipResults]
):

    def __init__(
        self,
        field_type: LLMExtractedFieldTypeVar,
        next_node: BaseLLMExtractionNode | ReconcileNode,
        phrase_relationship_prompt: Prompt,
    ):
        super().__init__(
            field_type=field_type,
            next_node=next_node,
        )
        self.phrase_relationship_prompt = phrase_relationship_prompt

    def get_upstream_phrase_search_map(
        self, pipeline_context: PipelineContext
    ) -> dict[GPTBatchRequestCustomID, GPTBatchRequest]:
        """Return the completed phrase-search request map from pipeline context."""
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement get_upstream_phrase_search_map"
        )

    def get_upstream_recursive_search_map(
        self, pipeline_context: PipelineContext
    ) -> dict[GPTBatchRequestCustomID, GPTBatchRequest]:
        """Return completed recursive-search requests when this pipeline has that phase."""
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement get_upstream_phrase_search_map"
        )

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
                f"Cannot embed req ids for llm phrase relationhip, "
                f"as request_map found empty for mfg:{mfg_etld1}, field:{self.field_type.name}."
            )

        for (
            chunk_bounds,
            extraction_request_bundle,
        ) in request_map.items():
            if not extraction_request_bundle.llm_phrase_relationship_req_id:
                extraction_request_bundle.llm_phrase_relationship_req_id = (
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
        llm_phrase_relationship_req_ids: set[GPTBatchRequestCustomID] = set()
        for (
            chunk_bounds,
            extraction_bundle,
        ) in request_map.items():
            if not extraction_bundle.llm_phrase_relationship_req_id:
                raise ValueError(
                    f"get_embedded_request_ids was called for {mfg_etld1}:{self.field_type.name} but "
                    f"extraction_bundle.llm_phrase_relationship_request is None for chunk bounds {chunk_bounds}."
                )

            llm_phrase_relationship_req_ids.add(
                extraction_bundle.llm_phrase_relationship_req_id
            )

        return llm_phrase_relationship_req_ids

    @staticmethod
    def get_request_custom_id(
        mfg_etld1: str,
        field_type: LLMExtractedFieldTypeEnum,
        chunk_bounds: str,
        metadata: LLMPhraseExtractionMetadata,
    ) -> GPTBatchRequestCustomID:
        return (
            f"{mfg_etld1}>{field_type.name}>llm_phrase_relationship>chunk>{chunk_bounds}>"
            f"{metadata.llm_phrase_relationship.model_params.to_custom_id_segment(metadata.llm_phrase_relationship.llm_model.name)}"
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
        """Create batch requests for the phrase_relationship phase."""
        mfg_name = pipeline_context.mfg_name
        if not mfg_name:
            raise ValueError(
                f"phrase_relationship_node.create_batch_requests was called for {self.field_type.name} in {self.__class__.__name__} but pipeline_context.mfg_name is not set. Ensure business_desc is extracted before phrase_relationship."
            )

        extraction_requests: Optional[DeferredLLMPhraseExtractionRequests] = getattr(
            deferred_mfg, self.field_type.name
        )
        if not extraction_requests:
            raise ValueError(
                f"phrase_relationship_node.create_batch_requests was called for {self.field_type.name} in {self.__class__.__name__} but no deferred extraction exists."
            )
        metadata = extraction_requests.metadata.llm_phrase_relationship

        # create_missing_phrase_relationship_requests only creates batch requests fresh or only missing ones,
        # for e.g., new mfg or some batch requests failed earlier and were deleted to allow re-processing
        batch_requests = await create_missing_phrase_relationship_requests(
            deferred_at=timestamp,
            mfg_etld1=deferred_mfg.etld1,
            mfg_name=mfg_name,
            field_type=self.field_type,
            missing_phrase_relationship_req_ids=missing_request_ids,
            extraction_requests=extraction_requests,
            mfg_text=scraped_text_file.text,
            phrase_relationship_prompt=self.phrase_relationship_prompt,
            llm_phrase_search_gpt_request_map=self.get_upstream_phrase_search_map(
                pipeline_context
            ),
            llm_phrase_recursive_search_gpt_request_map=self.get_upstream_recursive_search_map(
                pipeline_context
            ),
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
            gpt_model=metadata.llm_phrase_relationship.llm_model,
            model_params=metadata.llm_phrase_relationship.model_params,
        )
