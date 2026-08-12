from __future__ import annotations

import logging
from datetime import datetime

from llm_providers.db_models.gpt_batch_request import (
    GPTBatchRequest,
)
from core.models.deferred_extraction.deferred_keyword_extraction import (
    KeywordExtractionRequestMap,
    KeywordExtractionRequestBundle,
)
from core.models.extraction_schemas.grounding import (
    PhraseToTagAndRulesMap,
)
from llm_providers.models.open_ai.gpt_batch_response_blob import (
    GPTBatchResponse,
)
from core.models.extraction_results.keyword_extraction_results import (
    KeywordExtractionMetadata,
)
from llm_providers.models.file_objects.prompt import Prompt
from llm_providers.services.gpt_batch_request.gpt_batch_request_service import (
    dispatch_gpt_batch_request,
)
from core.models.pipeline_nodes.base.base_llm_extraction_node import (
    BaseLLMExtractionNode,
)
from core.models.pipeline_nodes.base.base_node import (
    LLMExtractedFieldTypeVar,
    PipelineContext,
)
from core.models.pipeline_nodes.base.base_reconcile_node import (
    ReconcileNode,
)
from core.models.field_types import ExtractionFieldType

from core.services.pipeline_nodes.multi_stage.llm_freehand_grounding_service import (
    create_missing_phrase_freehand_grounding_requests,
    get_freehand_grounding_result,
)
from llm_providers.field_types import BatchRequestIDType
from scraper.models.s3.scraped_text_file import ScrapedTextFile

logger = logging.getLogger(__name__)


class LLMPhraseFreehandGroundingNode(
    BaseLLMExtractionNode[LLMExtractedFieldTypeVar, PhraseToTagAndRulesMap]
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
    ) -> dict[BatchRequestIDType, GPTBatchRequest]:
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement get_upstream_phrase_relationship_map"
        )

    def get_upstream_phrase_screening_map(
        self, pipeline_context: PipelineContext
    ) -> dict[BatchRequestIDType, GPTBatchRequest]:
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement get_upstream_phrase_screening_map"
        )

    async def embed_request_ids(
        self,
        subject_unique_id: str,
        pipeline_context: PipelineContext,
        metadata: KeywordExtractionMetadata,
        chunked_request_map: KeywordExtractionRequestMap,
        timestamp: datetime,
    ):
        if not chunked_request_map:
            raise ValueError(
                f"Cannot embed req ids for llm freehand grounding node, "
                f"as chunked_request_map found empty for subject:{subject_unique_id}, field:{self.field_type.name}."
            )

        for chunk_bounds, extraction_request_bundle in chunked_request_map.items():
            if not extraction_request_bundle.llm_phrase_freehand_grounding_req_id:
                extraction_request_bundle.llm_phrase_freehand_grounding_req_id = (
                    self.get_request_custom_id(
                        subject_unique_id=subject_unique_id,
                        field_type=self.field_type,
                        chunk_bounds=chunk_bounds,
                        metadata=metadata,
                    )
                )

    def get_embedded_request_ids(
        self,
        subject_unique_id: str,
        chunked_request_map: KeywordExtractionRequestMap,
    ) -> set[BatchRequestIDType]:
        req_ids: set[BatchRequestIDType] = set()
        for chunk_bounds, extraction_bundle in chunked_request_map.items():
            if not extraction_bundle.llm_phrase_freehand_grounding_req_id:
                raise ValueError(
                    f"Cannot get embedded request ids for subject_unique_id:{subject_unique_id}>{chunk_bounds} as "
                    f"llm_phrase_freehand_grounding_req_id is absent in the extraction_bundle."
                )
            req_ids.add(extraction_bundle.llm_phrase_freehand_grounding_req_id)
        return req_ids

    @staticmethod
    def get_request_custom_id(
        subject_unique_id: str,
        field_type: ExtractionFieldType,
        chunk_bounds: str,
        metadata: KeywordExtractionMetadata,
    ) -> BatchRequestIDType:
        return (
            f"{subject_unique_id}>{field_type.name}>llm_phrase_freehand_grounding>chunk>{chunk_bounds}>"
            f"{metadata.llm_phrase_freehand_grounding.to_custom_id_segment()}"
        )

    async def create_batch_requests(
        self,
        subject_unique_id: str,
        scraped_text_file: ScrapedTextFile,
        missing_request_ids: set[BatchRequestIDType],
        metadata: KeywordExtractionMetadata,
        chunked_request_map: KeywordExtractionRequestMap,
        timestamp: datetime,
        pipeline_context: PipelineContext,
        eager: bool,
    ) -> list[GPTBatchRequest]:
        subject_name = pipeline_context.subject_name
        if not subject_name:
            raise ValueError(
                f"phrase_freehand_grounding_node.create_batch_requests was called for {self.field_type.name} in {self.__class__.__name__} but pipeline_context.subject_name is not set."
            )

        # extraction_requests: Optional[DeferredKeywordExtractionRequests] = getattr(
        #     deferred_subject, self.field_type.name
        # )
        # if not extraction_requests:
        #     raise ValueError(
        #         f"phrase_freehand_grounding_node.create_batch_requests was called for {self.field_type.name} in {self.__class__.__name__} but no deferred extraction exists."
        #     )

        return await create_missing_phrase_freehand_grounding_requests(
            deferred_at=timestamp,
            subject_unique_id=subject_unique_id,
            subject_name=subject_name,
            field_type=self.field_type,
            missing_phrase_freehand_grounding_req_ids=missing_request_ids,
            chunked_request_map=chunked_request_map,
            phrase_freehand_grounding_prompt=self.phrase_freehand_grounding_prompt,
            llm_phrase_relationship_gpt_request_map=self.get_upstream_phrase_relationship_map(
                pipeline_context
            ),
            llm_phrase_screening_gpt_request_map=self.get_upstream_phrase_screening_map(
                pipeline_context
            ),
            llm_model=metadata.llm_phrase_freehand_grounding.llm_model,
            model_params=metadata.llm_phrase_freehand_grounding.model_params,
            eager=eager,
        )

    @staticmethod
    async def get_result(
        subject_unique_id: str,
        field_type: ExtractionFieldType,
        chunk_bounds: str,
        extraction_bundle: KeywordExtractionRequestBundle,
        completed_request_map: dict[BatchRequestIDType, GPTBatchRequest],
        timestamp: datetime,  # for recording errors
    ) -> PhraseToTagAndRulesMap:
        return await get_freehand_grounding_result(
            subject_unique_id=subject_unique_id,
            field_type=field_type,
            chunk_bounds=chunk_bounds,
            extraction_bundle=extraction_bundle,
            completed_request_map=completed_request_map,
            timestamp=timestamp,
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
