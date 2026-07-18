from __future__ import annotations
import asyncio
import logging
from abc import abstractmethod
from datetime import datetime
from typing import Generic, TypeVar

from core.models.db.gpt_batch_request import GPTBatchRequest
from core.models.db.deferred_manufacturer import DeferredManufacturer
from core.models.db.manufacturer import Manufacturer
from data_etl_app.models.pipeline_nodes.base.base_llm_extraction_node import (
    BaseLLMExtractionNode,
    ExtractionMetadata,
    ExtractionRequestMap,
)
from data_etl_app.models.pipeline_nodes.base.base_node import PipelineContext
from data_etl_app.models.types_and_enums import LLMExtractedFieldTypeVar
from open_ai_key_app.models.field_types import GPTBatchRequestCustomID
from scraper_app.models.scraped_text_file import ScrapedTextFile

from core.services.gpt_batch_request_queries import (
    find_incomplete_gpt_batch_requests_by_custom_ids,
)
from core.services.gpt_batch_request_writes import (
    bulk_record_gpt_batch_responses,
    bulk_upsert_gpt_batch_requests_with_only_req_bodies,
)

logger = logging.getLogger(__name__)

ResultT = TypeVar("ResultT")


class BaseLLMRecursiveExtractionNode(
    BaseLLMExtractionNode[LLMExtractedFieldTypeVar, ResultT],
    Generic[LLMExtractedFieldTypeVar, ResultT],
):
    """Base class for recursive extraction phases.

    Concrete recursive nodes keep their own recursion rules inside
    ``embed_request_ids``. This base only owns eager convergence: keep embedding,
    creating, dispatching, and recording newly discovered request ids until the
    recursive node reports no missing request ids, then proceed to the next node.
    """

    @abstractmethod  # Child classes must implement this method
    async def create_batch_requests(
        self,
        mfg_etld1: str,
        scraped_text_file: ScrapedTextFile,
        missing_request_ids: set[GPTBatchRequestCustomID],
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

    async def execute(
        self,
        mfg: Manufacturer,
        deferred_mfg: DeferredManufacturer,
        scraped_text_file: ScrapedTextFile,
        timestamp,
        pipeline_context: PipelineContext,
        eager: bool,
    ) -> None:
        if not eager:
            await super().execute(
                mfg=mfg,
                deferred_mfg=deferred_mfg,
                scraped_text_file=scraped_text_file,
                timestamp=timestamp,
                pipeline_context=pipeline_context,
                eager=eager,
            )
            return

        extraction_requests = getattr(deferred_mfg, self.field_type.name)
        if not extraction_requests:
            raise ValueError(
                f"execute was called for {self.field_type.name} in {self.__class__.__name__} but no deferred extraction exists."
            )

        metadata = extraction_requests.metadata
        chunked_request_map = extraction_requests.chunked_request_map

        while True:
            await self.embed_request_ids(
                mfg_etld1=mfg.etld1,
                pipeline_context=pipeline_context,
                metadata=metadata,
                chunked_request_map=chunked_request_map,
                timestamp=timestamp,
            )

            missing_req_ids: set[GPTBatchRequestCustomID] = (
                await self.get_missing_req_ids(
                    mfg_etld1=mfg.etld1,
                    chunked_request_map=chunked_request_map,
                )
            )
            logger.info(
                f"[{mfg.etld1}] After embedding request ids for {self.__class__.__name__} ('{self.field_type.name}'), missing_req_ids:{missing_req_ids}"
            )
            if not missing_req_ids:
                break

            batch_requests = await self.create_batch_requests(
                missing_request_ids=missing_req_ids,
                mfg_etld1=deferred_mfg.etld1,
                scraped_text_file=scraped_text_file,
                timestamp=timestamp,
                pipeline_context=pipeline_context,
                metadata=metadata,
                chunked_request_map=chunked_request_map,
                eager=True,
            )
            logger.info(
                f"[{mfg.etld1}] ✅ Created {len(batch_requests)} batch requests for {self.__class__.__name__} ('{self.field_type.name}')"
                f"batch_requests custom_ids:{[br.request.custom_id for br in batch_requests]}"
            )
            await bulk_upsert_gpt_batch_requests_with_only_req_bodies(
                batch_requests=batch_requests,
                mfg_etld1=mfg.etld1,
            )

            logger.info(
                f"[{mfg.etld1}] 🚀 Eager execution enabled. Dispatching {len(batch_requests)} batch requests for {self.__class__.__name__} ('{self.field_type.name}') immediately."
            )

            batch_response_blobs = await asyncio.gather(
                *[
                    self.dispatch_batch_request(
                        gpt_batch_request=req,
                        metadata=metadata,
                    )
                    for req in batch_requests
                ]
            )
            modified_count, failed_updates = await bulk_record_gpt_batch_responses(
                batch_requests=batch_requests,
                response_blobs=batch_response_blobs,
                timestamp=timestamp,
            )
            logger.info(
                f"[{mfg.etld1}] ✅ Eagerly dispatched {len(batch_requests)} batch requests for {self.__class__.__name__} ('{self.field_type.name}') with {modified_count} successful response recordings and {failed_updates} failed updates."
            )
            await deferred_mfg.save()

        # check if all requests are complete
        if await self.are_all_requests_complete(
            mfg_etld1=mfg.etld1,
            chunked_request_map=extraction_requests.chunked_request_map,
        ):
            logger.info(
                f"[{mfg.etld1}] ✅ {self.__class__.__name__} converged for '{self.field_type.name}'. "
                f"Proceeding to next phase: {self.next_node.__class__.__name__ if self.next_node else 'None'}"
            )

            completed_request_map = await self.get_completed_request_map(
                mfg_etld1=mfg.etld1,
                chunked_request_map=chunked_request_map,
                all_requests_must_be_complete=True,
            )
            pipeline_context[type(self)] = completed_request_map

            if self.next_node:
                await self.next_node.execute(
                    mfg=mfg,
                    deferred_mfg=deferred_mfg,
                    scraped_text_file=scraped_text_file,
                    pipeline_context=pipeline_context,
                    timestamp=timestamp,
                    eager=eager,
                )
