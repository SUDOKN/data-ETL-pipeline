from __future__ import annotations
import asyncio
import logging
from datetime import datetime
from abc import abstractmethod

from typing import Union

from core.models.extraction_subject import (
    AbstractExtractionSubject,
    AbstractDeferredExtractionSubject,
)
from llm_providers.db_models.gpt_batch_request import (
    GPTBatchRequest,
)
from llm_providers.models.open_ai.gpt_batch_response_blob import (
    GPTBatchResponse,
)
from core.models.deferred_extraction.deferred_keyword_extraction import (
    DeferredKeywordExtractionRequests,
    KeywordExtractionRequestMap,
    KeywordExtractionRequestBundle,
)
from core.models.deferred_extraction.deferred_phrase_extraction_requests import (
    DeferredLLMPhraseExtractionRequests,
    LLMPhraseExtractionRequestMap,
    LLMPhraseExtractionMetadata,
    LLMPhraseExtractionRequestBundle,
)
from core.models.deferred_extraction.deferred_concept_extraction import (
    ConceptExtractionRequestMap,
    ConceptExtractionMetadata,
    DeferredConceptExtractionRequests,
    ConceptExtractionRequestBundle,
)
from core.models.deferred_extraction.deferred_single_stage_extraction_requests import (
    SingleStageExtractionRequestMap,
    LLMSingleStageExtractionMetadata,
    DeferredSingleStageExtractionRequests,
    SingleStageExtractionRequestBundle,
)
from core.models.extraction_results.keyword_extraction_results import (
    KeywordExtractionMetadata,
)
from core.models.extraction_results.single_stage_extraction_results import (
    LLMSingleStageExtractionMetadata,
)
from core.models.pipeline_nodes.base.base_node import (
    BaseNode,
    PipelineContext,
    ResultT,
)
from core.models.pipeline_nodes.base.base_reconcile_node import (
    ReconcileNode,
)
from core.models.types_and_enums import (
    LLMExtractedFieldTypeVar,
    LLMExtractedFieldTypeEnum,
)
from llm_providers.field_types import BatchRequestIDType
from scraper.models.s3.scraped_text_file import ScrapedTextFile

from llm_providers.services.gpt_batch_request.gpt_batch_request_queries import (
    find_completed_gpt_batch_request_ids_only,
    find_completed_gpt_batch_requests_by_custom_ids,
    find_gpt_batch_request_ids_only,
    find_incomplete_gpt_batch_requests_by_custom_ids,
)
from llm_providers.services.gpt_batch_request.gpt_batch_request_writes import (
    bulk_upsert_gpt_batch_requests_with_only_req_bodies,
    bulk_record_gpt_batch_responses,
)

logger = logging.getLogger(__name__)


DeferredExtractionRequests = Union[
    DeferredLLMPhraseExtractionRequests,
    DeferredConceptExtractionRequests,
    DeferredSingleStageExtractionRequests,
    DeferredKeywordExtractionRequests,
]
ExtractionRequestMap = Union[
    LLMPhraseExtractionRequestMap,
    ConceptExtractionRequestMap,
    KeywordExtractionRequestMap,
    SingleStageExtractionRequestMap,
]
ExtractionRequestBundle = Union[
    LLMPhraseExtractionRequestBundle,
    ConceptExtractionRequestBundle,
    KeywordExtractionRequestBundle,
    SingleStageExtractionRequestBundle,
]
ExtractionMetadata = Union[
    LLMPhraseExtractionMetadata,
    ConceptExtractionMetadata,
    KeywordExtractionMetadata,
    LLMSingleStageExtractionMetadata,
]


# Strategy Pattern
class BaseLLMExtractionNode(BaseNode[LLMExtractedFieldTypeVar, ResultT]):
    """
    Base class for single phase of extraction.

    Child classes will automatically make available their batch request result map as an attribute in the pipeline context.
    """

    def __init__(
        self,
        field_type: LLMExtractedFieldTypeVar,
        next_node: "BaseLLMExtractionNode | ReconcileNode",
    ):
        self.field_type: LLMExtractedFieldTypeVar = field_type
        self.next_node = next_node

    @abstractmethod
    async def embed_request_ids(  # prefill folded into this function
        self,
        subject_unique_id: str,
        pipeline_context: PipelineContext,
        metadata: ExtractionMetadata,
        chunked_request_map: ExtractionRequestMap,
        timestamp: datetime,
    ):
        pass

    @abstractmethod
    def get_embedded_request_ids(
        self,
        subject_unique_id: str,
        chunked_request_map: ExtractionRequestMap,
    ) -> set[BatchRequestIDType]:
        pass

    @staticmethod
    @abstractmethod
    def get_request_custom_id(
        subject_unique_id: str,
        field_type: LLMExtractedFieldTypeEnum,
        chunk_bounds: str,
        metadata: ExtractionMetadata,
    ) -> BatchRequestIDType:
        pass

    async def get_missing_req_ids(
        self,
        subject_unique_id: str,
        chunked_request_map: ExtractionRequestMap,
    ) -> set[BatchRequestIDType]:
        """Check if the DB is missing any GPT batch requests for this concept type."""
        # Check if all search requests exist
        req_ids_to_lookup: set[BatchRequestIDType] = self.get_embedded_request_ids(
            subject_unique_id=subject_unique_id,
            chunked_request_map=chunked_request_map,
        )
        if not req_ids_to_lookup:
            return set()

        req_ids_missing = req_ids_to_lookup - (
            await find_gpt_batch_request_ids_only(
                subject_unique_id, list(req_ids_to_lookup)
            )
            # maybe complete maybe not
        )
        if req_ids_missing:
            logger.info(f"Could not find batch req docs for {req_ids_missing}")
        else:
            logger.info(f"All req docs present.")

        # because otherwise even though deferred_address_extraction exists,
        # the batch request wasn't created for some reason
        # if empty set, then all requests exist;
        # if non-empty, those are the missing request IDs that need batch requests to be created for them
        return req_ids_missing

    async def are_all_requests_complete(
        self,
        subject_unique_id: str,
        chunked_request_map: ExtractionRequestMap,
    ) -> bool:
        # Check if all search requests are complete
        req_ids_to_lookup: set[BatchRequestIDType] = self.get_embedded_request_ids(
            subject_unique_id=subject_unique_id,
            chunked_request_map=chunked_request_map,
        )
        logger.info(
            f"Checking if all requests are complete for the following request IDs: {req_ids_to_lookup}"
        )
        incomplete_gpt_req_ids = req_ids_to_lookup - (
            await find_completed_gpt_batch_request_ids_only(
                subject_unique_id, list(req_ids_to_lookup)
            )
        )
        return not bool(incomplete_gpt_req_ids)

    async def get_completed_request_map(
        self,
        subject_unique_id: str,
        chunked_request_map: ExtractionRequestMap,
        all_requests_must_be_complete: bool = True,
    ) -> dict[BatchRequestIDType, GPTBatchRequest]:
        # Check if all search requests are complete
        req_ids_to_lookup: set[BatchRequestIDType] = self.get_embedded_request_ids(
            subject_unique_id=subject_unique_id,
            chunked_request_map=chunked_request_map,
        )
        incomplete_gpt_req_ids = req_ids_to_lookup - (
            await find_completed_gpt_batch_request_ids_only(
                subject_unique_id, list(req_ids_to_lookup)
            )
        )
        if incomplete_gpt_req_ids and all_requests_must_be_complete:
            raise ValueError(
                f"get_completed_request_map was called for {self.field_type.name} in {__class__.__name__} but not all requests are complete. Incomplete request IDs: {incomplete_gpt_req_ids}"
            )
        logger.info(
            f"[{subject_unique_id}] All requests are complete for {self.__class__.__name__} ('{self.field_type.name}'). Retrieving completed request map for {list(req_ids_to_lookup)}."
        )
        gpt_request_map = await find_completed_gpt_batch_requests_by_custom_ids(
            subject_unique_id, list(req_ids_to_lookup)
        )
        return gpt_request_map

    @abstractmethod  # Child classes must implement this method
    async def create_batch_requests(
        self,
        subject_unique_id: str,
        scraped_text_file: ScrapedTextFile,
        missing_request_ids: set[BatchRequestIDType],
        metadata: ExtractionMetadata,
        chunked_request_map: ExtractionRequestMap,
        timestamp: datetime,
        pipeline_context: PipelineContext,
        eager: bool,
    ) -> list[GPTBatchRequest]:
        """
        Create GPT batch requests needed for this extraction phase.
        Child classes must implement this method.
        """
        pass

    @staticmethod
    @abstractmethod
    async def get_result(
        subject_unique_id: str,
        field_type: LLMExtractedFieldTypeEnum,
        chunk_bounds: str,
        extraction_bundle: ExtractionRequestBundle,
        completed_request_map: dict[BatchRequestIDType, GPTBatchRequest],
        timestamp: datetime,  # for recording errors
    ) -> ResultT:
        pass

    @abstractmethod
    async def dispatch_batch_request(
        self,
        gpt_batch_request: GPTBatchRequest,
        metadata: ExtractionMetadata,
    ) -> GPTBatchResponse:
        pass

    async def execute(
        self,
        subject: AbstractExtractionSubject,
        deferred_subject: AbstractDeferredExtractionSubject,
        scraped_text_file: ScrapedTextFile,
        timestamp: datetime,
        pipeline_context: PipelineContext,
        eager: bool,  # if True, dispatch all batch requests immediately and then check for completion, basically a sync execution of the entire phase
    ) -> None:
        """Execute this extraction phase if needed, and proceed to the next phase."""
        logger.debug(
            f"[{subject.subject_unique_id}] 🔄 Executing {self.__class__.__name__} for field '{self.field_type.name}'"
        )

        extraction_requests: DeferredLLMPhraseExtractionRequests = getattr(
            deferred_subject, self.field_type.name
        )
        if not extraction_requests:
            raise ValueError(
                f"execute was called for {self.field_type.name} in {__class__.__name__} but no deferred concept extraction exists."
            )

        # prefills request map with required req ids
        # that need to have associated GPTBatchRequest docs in db
        await self.embed_request_ids(
            subject_unique_id=subject.subject_unique_id,
            pipeline_context=pipeline_context,
            metadata=extraction_requests.metadata,
            chunked_request_map=extraction_requests.chunked_request_map,
            timestamp=timestamp,
        )
        await deferred_subject.save()

        missing_req_ids = await self.get_missing_req_ids(
            subject_unique_id=subject.subject_unique_id,
            chunked_request_map=extraction_requests.chunked_request_map,
        )
        if missing_req_ids:
            logger.info(
                f"[{subject.subject_unique_id}] 🆕 {self.__class__.__name__}: Missing requests detected for '{self.field_type.name}'. Creating batch requests..."
            )
            batch_requests = await self.create_batch_requests(
                subject_unique_id=deferred_subject.subject_unique_id,
                scraped_text_file=scraped_text_file,
                missing_request_ids=missing_req_ids,
                metadata=extraction_requests.metadata,
                chunked_request_map=extraction_requests.chunked_request_map,
                timestamp=timestamp,
                pipeline_context=pipeline_context,
                eager=eager,
            )
            logger.info(
                f"[{subject.subject_unique_id}] ✅ Created {len(batch_requests)} batch requests for {self.__class__.__name__} ('{self.field_type.name}')"
                f"batch_requests custom_ids:{[br.request.custom_id for br in batch_requests]}"
            )
            await bulk_upsert_gpt_batch_requests_with_only_req_bodies(
                batch_requests=batch_requests,
                subject_unique_id=subject.subject_unique_id,
            )
        else:
            logger.debug(
                f"[{subject.subject_unique_id}] ✓ {self.__class__.__name__}: All requests already exist for '{self.field_type.name}'"
            )

        if eager:
            all_request_ids = self.get_embedded_request_ids(
                subject_unique_id=deferred_subject.subject_unique_id,
                chunked_request_map=extraction_requests.chunked_request_map,
            )
            incomplete_requests = (
                await find_incomplete_gpt_batch_requests_by_custom_ids(
                    deferred_subject.subject_unique_id, list(all_request_ids)
                )
            )
            logger.info(
                f"[{subject.subject_unique_id}] 🚀 Eager execution enabled. Dispatching {len(incomplete_requests)} batch requests for {self.__class__.__name__} ('{self.field_type.name}') immediately."
            )
            # execute all using asyncio.gather with dispatch_gpt_batch_request
            batch_response_blobs = await asyncio.gather(
                *[
                    self.dispatch_batch_request(
                        gpt_batch_request=req,
                        metadata=extraction_requests.metadata,
                    )
                    for req in incomplete_requests.values()
                ]
            )
            modified_count, failed_updates = await bulk_record_gpt_batch_responses(
                batch_requests=list(incomplete_requests.values()),
                response_blobs=batch_response_blobs,
                timestamp=timestamp,
            )
            logger.info(
                f"[{subject.subject_unique_id}] ✅ Eagerly dispatched {len(incomplete_requests)} batch requests for {self.__class__.__name__} ('{self.field_type.name}') with {modified_count} successful response recordings and {failed_updates} failed updates."
            )

        # check if all requests are complete
        if await self.are_all_requests_complete(
            subject_unique_id=subject.subject_unique_id,
            chunked_request_map=extraction_requests.chunked_request_map,
        ):
            logger.info(
                f"[{subject.subject_unique_id}] ✅ {self.__class__.__name__} is COMPLETE for '{self.field_type.name}'. "
                f"Proceeding to next phase: {self.next_node.__class__.__name__ if self.next_node else 'None'}"
            )
            completed_request_map = await self.get_completed_request_map(
                subject_unique_id=subject.subject_unique_id,
                chunked_request_map=extraction_requests.chunked_request_map,
            )
            pipeline_context[type(self)] = completed_request_map
            if self.next_node:
                # next phase isn't executed unless this phase is complete
                # chain of responsibility
                if isinstance(self.next_node, BaseLLMExtractionNode):
                    logger.info(
                        f"[{subject.subject_unique_id}] ➡️  Proceeding to next ExtractionNode: {self.next_node.__class__.__name__}"
                    )
                elif isinstance(self.next_node, ReconcileNode):
                    logger.info(
                        f"[{subject.subject_unique_id}] ➡️  Proceeding to ReconcileNode: {self.next_node.__class__.__name__}"
                    )
                await self.next_node.execute(
                    subject=subject,
                    deferred_subject=deferred_subject,
                    scraped_text_file=scraped_text_file,
                    pipeline_context=pipeline_context,
                    timestamp=timestamp,
                    eager=eager,
                )
        else:
            # phase not complete yet, so no batch requests to return and no next phase executed
            logger.debug(
                f"[{subject.subject_unique_id}] ⏸️  {self.__class__.__name__} is NOT complete for '{self.field_type.name}'. "
                f"Waiting for requests to complete. Cannot proceed to: {self.next_node.__class__.__name__ if self.next_node else 'None'}"
            )
