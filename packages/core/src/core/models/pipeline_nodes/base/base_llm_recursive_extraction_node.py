from __future__ import annotations
import asyncio
import logging

from core.models.extraction_subject import (
    AbstractExtractionSubject,
    AbstractDeferredExtractionSubject,
)
from core.models.pipeline_nodes.base.base_llm_extraction_node import (
    BaseLLMExtractionNode,
    ResultT,
)
from core.models.pipeline_nodes.base.base_node import PipelineContext
from core.models.field_types import LLMExtractedFieldTypeVar
from llm_providers.field_types import BatchRequestIDType
from scraper.models.s3.scraped_text_file import ScrapedTextFile

from llm_providers.services.gpt_batch_request.gpt_batch_request_writes import (
    bulk_record_gpt_batch_responses,
    bulk_upsert_gpt_batch_requests_with_only_req_bodies,
)

logger = logging.getLogger(__name__)


class BaseLLMRecursiveExtractionNode(
    BaseLLMExtractionNode[LLMExtractedFieldTypeVar, ResultT],
):
    """Base class for recursive extraction phases.

    Concrete recursive nodes keep their own recursion rules inside
    ``embed_request_ids``. This base only owns eager convergence: keep embedding,
    creating, dispatching, and recording newly discovered request ids until the
    recursive node reports no missing request ids, then proceed to the next node.
    """

    async def execute(
        self,
        subject: AbstractExtractionSubject,
        deferred_subject: AbstractDeferredExtractionSubject,
        scraped_text_file: ScrapedTextFile,
        timestamp,
        pipeline_context: PipelineContext,
        eager: bool,
    ) -> None:
        # Gated here too, not only in the base: the eager branch below never
        # reaches super().execute(), so a check that lived only there would let
        # a disabled recursive stage run its whole convergence loop.
        if await self.stop_if_stage_disabled(
            subject=subject,
            deferred_subject=deferred_subject,
            scraped_text_file=scraped_text_file,
            timestamp=timestamp,
            pipeline_context=pipeline_context,
        ):
            return

        if not eager:
            await super().execute(
                subject=subject,
                deferred_subject=deferred_subject,
                scraped_text_file=scraped_text_file,
                timestamp=timestamp,
                pipeline_context=pipeline_context,
                eager=eager,
            )
            return

        while True:
            extraction_requests = getattr(deferred_subject, self.field_type.name)
            if not extraction_requests:
                raise ValueError(
                    f"execute was called for {self.field_type.name} in {self.__class__.__name__} but no deferred extraction exists."
                )

            metadata = extraction_requests.metadata
            chunked_request_map = extraction_requests.chunked_request_map

            logger.info(
                f"Embedding request ids for {self.__class__.__name__} ('{self.field_type.name}') for {subject.subject_unique_id}"
            )
            await self.embed_request_ids(
                subject_unique_id=subject.subject_unique_id,
                pipeline_context=pipeline_context,
                metadata=metadata,
                chunked_request_map=chunked_request_map,
                timestamp=timestamp,
            )
            logger.info(
                f"Saving deferred subject after embedding request ids for {subject.subject_unique_id}"
            )

            await deferred_subject.save()

            missing_req_ids: set[BatchRequestIDType] = await self.get_missing_req_ids(
                subject_unique_id=subject.subject_unique_id,
                chunked_request_map=chunked_request_map,
            )
            logger.info(
                f"[{subject.subject_unique_id}] After embedding all request ids for {self.__class__.__name__} ('{self.field_type.name}'), missing_req_ids:{[missing_req_ids]}"
            )
            if not missing_req_ids:
                break

            batch_requests = await self.create_batch_requests(
                missing_request_ids=missing_req_ids,
                subject_unique_id=deferred_subject.subject_unique_id,
                scraped_text_file=scraped_text_file,
                timestamp=timestamp,
                pipeline_context=pipeline_context,
                metadata=metadata,
                chunked_request_map=chunked_request_map,
                eager=True,
            )
            logger.info(
                f"[{subject.subject_unique_id}] ✅ Created {len(batch_requests)} batch requests for {self.__class__.__name__} ('{self.field_type.name}')"
                f"batch_requests custom_ids:{[br.request.custom_id for br in batch_requests]}"
            )
            await bulk_upsert_gpt_batch_requests_with_only_req_bodies(
                batch_requests=batch_requests,
                subject_unique_id=subject.subject_unique_id,
            )

            logger.info(
                f"[{subject.subject_unique_id}] 🚀 Eager execution enabled. Dispatching {len(batch_requests)} batch requests for {self.__class__.__name__} ('{self.field_type.name}') immediately."
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
                f"[{subject.subject_unique_id}] ✅ Eagerly dispatched {len(batch_requests)} batch requests for {self.__class__.__name__} ('{self.field_type.name}') with {modified_count} successful response recordings and {failed_updates} failed updates."
            )

        extraction_requests = getattr(deferred_subject, self.field_type.name)
        if not extraction_requests:
            raise ValueError(
                f"execute was called for {self.field_type.name} in {self.__class__.__name__} but no deferred extraction exists."
            )

        # check if all requests are complete
        if await self.are_all_requests_complete(
            subject_unique_id=subject.subject_unique_id,
            chunked_request_map=extraction_requests.chunked_request_map,
        ):
            logger.info(
                f"[{subject.subject_unique_id}] ✅ {self.__class__.__name__} converged for '{self.field_type.name}'. "
                f"Proceeding to next phase: {self.next_node.__class__.__name__ if self.next_node else 'None'}"
            )

            completed_request_map = await self.get_completed_request_map(
                subject_unique_id=subject.subject_unique_id,
                chunked_request_map=extraction_requests.chunked_request_map,
            )
            pipeline_context[type(self)] = completed_request_map

            if self.next_node:
                await self.next_node.execute(
                    subject=subject,
                    deferred_subject=deferred_subject,
                    scraped_text_file=scraped_text_file,
                    pipeline_context=pipeline_context,
                    timestamp=timestamp,
                    eager=eager,
                )
