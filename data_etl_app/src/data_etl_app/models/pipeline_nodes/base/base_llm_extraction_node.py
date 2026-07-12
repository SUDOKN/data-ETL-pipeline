from __future__ import annotations
import asyncio
import logging
from datetime import datetime
from abc import abstractmethod

from typing import TypeVar, Generic, Union

from core.models.db.manufacturer import Manufacturer
from core.models.db.deferred_manufacturer import DeferredManufacturer
from core.models.db.gpt_batch_request import GPTBatchRequest
from core.models.gpt_batch_response_blob import (
    GPTBatchResponse,
)
from core.models.deferred_phrase_extraction_requests import (
    DeferredLLMPhraseExtractionRequests,
    LLMPhraseExtractionRequestMap,
    LLMPhraseExtractionMetadata,
)
from core.models.deferred_concept_extraction import (
    ConceptExtractionRequestMap,
    ConceptExtractionMetadata,
    DeferredConceptExtractionRequests,
)
from core.models.single_stage_extraction_results import LLMSingleStageExtractionMetadata
from core.models.deferred_single_stage_extraction_requests import (
    SingleStageExtractionRequestMap,
    LLMSingleStageExtractionMetadata,
    DeferredSingleStageExtractionRequests,
)
from data_etl_app.models.pipeline_nodes.base.base_node import (
    BaseNode,
    PipelineContext,
)
from data_etl_app.models.pipeline_nodes.base.base_reconcile_node import ReconcileNode
from data_etl_app.models.types_and_enums import (
    LLMExtractedFieldTypeVar,
    LLMExtractedFieldTypeEnum,
)
from open_ai_key_app.models.field_types import GPTBatchRequestCustomID
from scraper_app.models.scraped_text_file import ScrapedTextFile

from core.services.gpt_batch_request_queries import (
    find_completed_gpt_batch_request_ids_only,
    find_completed_gpt_batch_requests_by_custom_ids,
    find_gpt_batch_request_ids_only,
    find_incomplete_gpt_batch_requests_by_custom_ids,
)
from core.services.gpt_batch_request_writes import (
    bulk_upsert_gpt_batch_requests_with_only_req_bodies,
    bulk_record_gpt_batch_responses,
)

logger = logging.getLogger(__name__)

ResultT = TypeVar("ResultT")

DeferredExtractionRequests = Union[
    DeferredLLMPhraseExtractionRequests,
    DeferredConceptExtractionRequests,
    DeferredSingleStageExtractionRequests,
]
ExtractionRequestMap = Union[
    LLMPhraseExtractionRequestMap,
    ConceptExtractionRequestMap,
    SingleStageExtractionRequestMap,
]
ExtractionMetadata = Union[
    LLMPhraseExtractionMetadata,
    ConceptExtractionMetadata,
    LLMSingleStageExtractionMetadata,
]


# Strategy Pattern
class BaseLLMExtractionNode(
    BaseNode[LLMExtractedFieldTypeVar], Generic[LLMExtractedFieldTypeVar, ResultT]
):
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
        mfg_etld1: str,
        pipeline_context: PipelineContext,
        metadata: ExtractionMetadata,
        request_map: ExtractionRequestMap,
        timestamp: datetime,
    ):
        pass

    @abstractmethod
    def get_embedded_request_ids(
        self,
        mfg_etld1: str,
        request_map: ExtractionRequestMap,
    ) -> set[GPTBatchRequestCustomID]:
        pass

    @staticmethod
    @abstractmethod
    def get_request_custom_id(
        mfg_etld1: str,
        field_type: LLMExtractedFieldTypeEnum,
        chunk_bounds: str,
        metadata: ExtractionMetadata,
    ) -> GPTBatchRequestCustomID:
        pass

    async def get_missing_req_ids(
        self,
        mfg_etld1: str,
        request_map: ExtractionRequestMap,
    ) -> set[GPTBatchRequestCustomID]:
        """Check if the DB is missing any GPT batch requests for this concept type."""
        # Check if all search requests exist
        req_ids_to_lookup: set[GPTBatchRequestCustomID] = self.get_embedded_request_ids(
            mfg_etld1=mfg_etld1,
            request_map=request_map,
        )
        req_ids_missing = req_ids_to_lookup - (
            await find_gpt_batch_request_ids_only(mfg_etld1, list(req_ids_to_lookup))
            # maybe complete maybe not
        )

        # because otherwise even though deferred_address_extraction exists,
        # the batch request wasn't created for some reason
        # if empty set, then all requests exist;
        # if non-empty, those are the missing request IDs that need batch requests to be created for them
        return req_ids_missing

    async def are_all_requests_complete(
        self,
        mfg_etld1: str,
        request_map: ExtractionRequestMap,
    ) -> bool:
        # Check if all search requests are complete
        req_ids_to_lookup: set[GPTBatchRequestCustomID] = self.get_embedded_request_ids(
            mfg_etld1=mfg_etld1,
            request_map=request_map,
        )
        incomplete_gpt_req_ids = req_ids_to_lookup - (
            await find_completed_gpt_batch_request_ids_only(
                mfg_etld1, list(req_ids_to_lookup)
            )
        )
        return not bool(incomplete_gpt_req_ids)

    async def get_completed_request_map(
        self,
        mfg_etld1: str,
        request_map: ExtractionRequestMap,
        all_requests_must_be_complete: bool,
    ) -> dict[GPTBatchRequestCustomID, GPTBatchRequest]:
        # Check if all search requests are complete
        req_ids_to_lookup: set[GPTBatchRequestCustomID] = self.get_embedded_request_ids(
            mfg_etld1=mfg_etld1,
            request_map=request_map,
        )
        incomplete_gpt_req_ids = req_ids_to_lookup - (
            await find_completed_gpt_batch_request_ids_only(
                mfg_etld1, list(req_ids_to_lookup)
            )
        )
        if incomplete_gpt_req_ids and all_requests_must_be_complete:
            raise ValueError(
                f"get_completed_request_map was called for {self.field_type.name} in {__class__.__name__} but not all requests are complete. Incomplete request IDs: {incomplete_gpt_req_ids}"
            )

        gpt_request_map = await find_completed_gpt_batch_requests_by_custom_ids(
            mfg_etld1, list(req_ids_to_lookup)
        )
        return gpt_request_map

    @abstractmethod  # Child classes must implement this method
    async def create_batch_requests(
        self,
        missing_request_ids: set[GPTBatchRequestCustomID],
        deferred_mfg: DeferredManufacturer,
        scraped_text_file: ScrapedTextFile,
        timestamp: datetime,
        pipeline_context: PipelineContext,
        eager: bool,
    ) -> list[GPTBatchRequest]:
        """
        Create GPT batch requests needed for this extraction phase.
        Child classes must implement this method.
        """
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
        mfg: Manufacturer,
        deferred_mfg: DeferredManufacturer,
        scraped_text_file: ScrapedTextFile,
        timestamp: datetime,
        pipeline_context: PipelineContext,
        eager: bool,  # if True, dispatch all batch requests immediately and then check for completion, basically a sync execution of the entire phase
    ) -> None:
        """Execute this extraction phase if needed, and proceed to the next phase."""
        logger.debug(
            f"[{mfg.etld1}] 🔄 Executing {self.__class__.__name__} for field '{self.field_type.name}'"
        )

        extraction_requests: DeferredLLMPhraseExtractionRequests = getattr(
            deferred_mfg, self.field_type.name
        )
        if not extraction_requests:
            raise ValueError(
                f"execute was called for {self.field_type.name} in {__class__.__name__} but no deferred concept extraction exists."
            )

        # prefills request map with required req ids
        # that need to have associated GPTBatchRequest docs in db
        await self.embed_request_ids(
            mfg_etld1=mfg.etld1,
            pipeline_context=pipeline_context,
            metadata=extraction_requests.metadata,
            request_map=extraction_requests.chunked_request_map,
            timestamp=timestamp,
        )
        # await deferred_mfg.save()  # evaluate this

        missing_req_ids = await self.get_missing_req_ids(
            mfg_etld1=mfg.etld1,
            request_map=extraction_requests.chunked_request_map,
        )
        if missing_req_ids:
            logger.info(
                f"[{mfg.etld1}] 🆕 {self.__class__.__name__}: Missing requests detected for '{self.field_type.name}'. Creating batch requests..."
            )
            batch_requests = await self.create_batch_requests(
                missing_request_ids=missing_req_ids,
                deferred_mfg=deferred_mfg,
                scraped_text_file=scraped_text_file,
                timestamp=timestamp,
                pipeline_context=pipeline_context,
                eager=eager,
            )
            logger.info(
                f"[{mfg.etld1}] ✅ Created {len(batch_requests)} batch requests for {self.__class__.__name__} ('{self.field_type.name}')"
                f"batch_requests custom_ids:{[br.request.custom_id for br in batch_requests]}"
            )
            await bulk_upsert_gpt_batch_requests_with_only_req_bodies(
                batch_requests=batch_requests, mfg_etld1=mfg.etld1
            )
            # await deferred_mfg.save()
        else:
            logger.debug(
                f"[{mfg.etld1}] ✓ {self.__class__.__name__}: All requests already exist for '{self.field_type.name}'"
            )

        if eager:
            all_request_ids = self.get_embedded_request_ids(
                mfg_etld1=deferred_mfg.etld1,
                request_map=extraction_requests.chunked_request_map,
            )
            incomplete_requests = (
                await find_incomplete_gpt_batch_requests_by_custom_ids(
                    deferred_mfg.etld1, list(all_request_ids)
                )
            )
            logger.info(
                f"[{mfg.etld1}] 🚀 Eager execution enabled. Dispatching {len(incomplete_requests)} batch requests for {self.__class__.__name__} ('{self.field_type.name}') immediately."
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
                f"[{mfg.etld1}] ✅ Eagerly dispatched {len(incomplete_requests)} batch requests for {self.__class__.__name__} ('{self.field_type.name}') with {modified_count} successful response recordings and {failed_updates} failed updates."
            )

        # check if all requests are complete
        if await self.are_all_requests_complete(
            mfg_etld1=mfg.etld1,
            request_map=extraction_requests.chunked_request_map,
        ):
            logger.info(
                f"[{mfg.etld1}] ✅ {self.__class__.__name__} is COMPLETE for '{self.field_type.name}'. "
                f"Proceeding to next phase: {self.next_node.__class__.__name__ if self.next_node else 'None'}"
            )
            completed_request_map = await self.get_completed_request_map(
                mfg_etld1=mfg.etld1,
                request_map=extraction_requests.chunked_request_map,
                all_requests_must_be_complete=True,
            )
            pipeline_context[type(self)] = completed_request_map
            if self.next_node:
                # next phase isn't executed unless this phase is complete
                # chain of responsibility
                if isinstance(self.next_node, BaseLLMExtractionNode):
                    logger.info(
                        f"[{mfg.etld1}] ➡️  Proceeding to next ExtractionNode: {self.next_node.__class__.__name__}"
                    )
                elif isinstance(self.next_node, ReconcileNode):
                    logger.info(
                        f"[{mfg.etld1}] ➡️  Proceeding to ReconcileNode: {self.next_node.__class__.__name__}"
                    )
                await self.next_node.execute(
                    mfg=mfg,
                    deferred_mfg=deferred_mfg,
                    scraped_text_file=scraped_text_file,
                    pipeline_context=pipeline_context,
                    timestamp=timestamp,
                    eager=eager,
                )
        else:
            # phase not complete yet, so no batch requests to return and no next phase executed
            logger.debug(
                f"[{mfg.etld1}] ⏸️  {self.__class__.__name__} is NOT complete for '{self.field_type.name}'. "
                f"Waiting for requests to complete. Cannot proceed to: {self.next_node.__class__.__name__ if self.next_node else 'None'}"
            )
