import asyncio
import logging
from datetime import datetime
from abc import abstractmethod

from typing import Optional, TypeVar, Generic

from core.models.db.manufacturer import Manufacturer
from core.models.db.deferred_manufacturer import DeferredManufacturer
from core.models.db.gpt_batch_request import GPTBatchRequest
from core.models.deferred_concept_extraction import (
    DeferredConceptExtractionRequests,
    ConceptExtractionRequestBundle,
)
from data_etl_app.models.pipeline_nodes.base_node import (
    BaseNode,
    PipelineContext,
)
from data_etl_app.models.pipeline_nodes.reconcile_node import ReconcileNode
from data_etl_app.models.types_and_enums import (
    LLMExtractedFieldTypeVar,
    LLMExtractedFieldTypeEnum,
)
from open_ai_key_app.models.field_types import GPTBatchRequestCustomID
from litellm_proxy_app.models.llm_model import LLM_Model
from open_ai_key_app.models.gpt_model_params import GPTModelParams
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
from core.services.gpt_batch_request_service import (
    dispatch_gpt_batch_request,
)

logger = logging.getLogger(__name__)

ResultT = TypeVar("ResultT")


# Strategy Pattern
class LLMExtractionNode(
    BaseNode[LLMExtractedFieldTypeVar], Generic[LLMExtractedFieldTypeVar, ResultT]
):
    """Base class for single phase of extraction."""

    def __init__(
        self,
        field_type: LLMExtractedFieldTypeVar,
        next_node: "LLMExtractionNode | ReconcileNode",
    ):
        self.field_type: LLMExtractedFieldTypeVar = field_type
        self.next_node = next_node

    @abstractmethod
    def get_embedded_request_ids(
        self,
        mfg_etld1: str,
        extraction_requests: DeferredConceptExtractionRequests,
    ) -> set[GPTBatchRequestCustomID]:
        pass

    @staticmethod
    @abstractmethod
    def get_request_custom_id(
        mfg_etld1: str,
        field_type: LLMExtractedFieldTypeEnum,
        chunk_bounds: str,
        llm_model: LLM_Model,
        model_params: GPTModelParams,
    ) -> GPTBatchRequestCustomID:
        pass

    async def get_missing_req_ids(
        self,
        deferred_mfg: DeferredManufacturer,
    ) -> set[GPTBatchRequestCustomID]:
        """Check if the DB is missing any GPT batch requests for this concept type."""
        extraction_requests: Optional[DeferredConceptExtractionRequests] = getattr(
            deferred_mfg, self.field_type.name
        )
        if not extraction_requests:
            raise ValueError(
                f"get_missing_req_ids was called for {self.field_type.name} in {__class__.__name__} but no concept extraction requests exist."
            )

        # Check if all search requests exist
        req_ids_to_lookup: set[GPTBatchRequestCustomID] = self.get_embedded_request_ids(
            mfg_etld1=deferred_mfg.etld1,
            extraction_requests=extraction_requests,
        )
        req_ids_missing = req_ids_to_lookup - (
            await find_gpt_batch_request_ids_only(
                deferred_mfg.etld1, list(req_ids_to_lookup)
            )
            # maybe complete maybe not
        )

        # because otherwise even though deferred_address_extraction exists,
        # the batch request wasn't created for some reason
        # if empty set, then all requests exist;
        # if non-empty, those are the missing request IDs that need batch requests to be created for them
        return req_ids_missing

    async def are_all_requests_complete(
        self,
        deferred_mfg: DeferredManufacturer,
    ) -> bool:
        extraction_requests: Optional[DeferredConceptExtractionRequests] = getattr(
            deferred_mfg, self.field_type.name
        )
        if not extraction_requests:
            raise ValueError(
                f"are_all_requests_complete was called for {self.field_type.name} in {__class__.__name__} but no concept extraction requests exist."
            )

        # Check if all search requests are complete
        req_ids_to_lookup: set[GPTBatchRequestCustomID] = self.get_embedded_request_ids(
            mfg_etld1=deferred_mfg.etld1,
            extraction_requests=extraction_requests,
        )
        incomplete_gpt_req_ids = req_ids_to_lookup - (
            await find_completed_gpt_batch_request_ids_only(
                deferred_mfg.etld1, list(req_ids_to_lookup)
            )
        )
        return not bool(incomplete_gpt_req_ids)

    async def get_completed_request_map(
        self,
        deferred_mfg: DeferredManufacturer,
    ) -> dict[GPTBatchRequestCustomID, GPTBatchRequest]:
        extraction_requests: Optional[DeferredConceptExtractionRequests] = getattr(
            deferred_mfg, self.field_type.name
        )
        if not extraction_requests:
            raise ValueError(
                f"get_completed_request_map was called for {self.field_type.name} in {self.__class__.__name__} but no concept extraction requests exist."
            )

        # Check if all search requests are complete
        req_ids_to_lookup: set[GPTBatchRequestCustomID] = self.get_embedded_request_ids(
            mfg_etld1=deferred_mfg.etld1,
            extraction_requests=extraction_requests,
        )
        incomplete_gpt_req_ids = req_ids_to_lookup - (
            await find_completed_gpt_batch_request_ids_only(
                deferred_mfg.etld1, list(req_ids_to_lookup)
            )
        )
        if incomplete_gpt_req_ids:
            raise ValueError(
                f"get_completed_request_map was called for {self.field_type.name} in {__class__.__name__} but not all requests are complete. Incomplete request IDs: {incomplete_gpt_req_ids}"
            )

        gpt_request_map = await find_completed_gpt_batch_requests_by_custom_ids(
            deferred_mfg.etld1, list(req_ids_to_lookup)
        )
        return gpt_request_map

    @staticmethod
    @abstractmethod
    async def parse_batch_request_result(
        mfg_etld1: str,
        field_type: LLMExtractedFieldTypeEnum,
        chunk_bounds: str,
        extraction_bundle: ConceptExtractionRequestBundle,
        completed_request_map: dict[GPTBatchRequestCustomID, GPTBatchRequest],
        deferred_at: datetime,
    ) -> ResultT:
        pass

    @abstractmethod  # Child classes must implement this method
    async def create_batch_requests(
        self,
        missing_request_ids: set[GPTBatchRequestCustomID],
        deferred_mfg: DeferredManufacturer,
        scraped_text_file: ScrapedTextFile,
        timestamp: datetime,
        pipeline_context: PipelineContext,
        llm_model: LLM_Model,
        model_params: GPTModelParams,
        eager: bool,
    ) -> list[GPTBatchRequest]:
        """
        Create GPT batch requests needed for this extraction phase.
        Child classes must implement this method.
        """
        pass

    async def execute(
        self,
        mfg: Manufacturer,
        deferred_mfg: DeferredManufacturer,
        scraped_text_file: ScrapedTextFile,
        timestamp: datetime,
        pipeline_context: PipelineContext,
        llm_model: LLM_Model,
        model_params: GPTModelParams,
        eager: bool,  # if True, dispatch all batch requests immediately and then check for completion, basically a sync execution of the entire phase
    ) -> None:
        """Execute this extraction phase if needed, and proceed to the next phase."""
        logger.debug(
            f"[{mfg.etld1}] 🔄 Executing {self.__class__.__name__} for field '{self.field_type.name}'"
        )

        missing_req_ids = await self.get_missing_req_ids(deferred_mfg=deferred_mfg)
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
                llm_model=llm_model,
                model_params=model_params,
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
                extraction_requests=getattr(deferred_mfg, self.field_type.name),
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
                    dispatch_gpt_batch_request(
                        gpt_batch_request=req,
                        gpt_model=llm_model,
                        model_params=model_params,
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
            deferred_mfg=deferred_mfg,
        ):
            logger.info(
                f"[{mfg.etld1}] ✅ {self.__class__.__name__} is COMPLETE for '{self.field_type.name}'. "
                f"Proceeding to next phase: {self.next_node.__class__.__name__ if self.next_node else 'None'}"
            )
            completed_batch_req_map = await self.get_completed_request_map(
                deferred_mfg=deferred_mfg,
            )
            pipeline_context[type(self)] = completed_batch_req_map
            if self.next_node:
                # next phase isn't executed unless this phase is complete
                # chain of responsibility
                if isinstance(self.next_node, LLMExtractionNode):
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
                    llm_model=llm_model,
                    model_params=model_params,
                )
        else:
            # phase not complete yet, so no batch requests to return and no next phase executed
            logger.debug(
                f"[{mfg.etld1}] ⏸️  {self.__class__.__name__} is NOT complete for '{self.field_type.name}'. "
                f"Waiting for requests to complete. Cannot proceed to: {self.next_node.__class__.__name__ if self.next_node else 'None'}"
            )
