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
    LLMPhraseExtractionRequestBundle,
)
from core.models.deferred_extraction.deferred_concept_extraction import (
    ConceptExtractionRequestMap,
    DeferredConceptExtractionRequests,
    ConceptExtractionRequestBundle,
)
from core.models.extraction_results.llm_phrase_extraction_results import (
    ConceptExtractionMetadata,
    KeywordExtractionMetadata,
    LLMPhraseExtractionMetadata,
)
from core.models.deferred_extraction.deferred_single_stage_extraction_requests import (
    SingleStageExtractionRequestMap,
    LLMSingleStageExtractionMetadata,
    DeferredSingleStageExtractionRequests,
    SingleStageExtractionRequestBundle,
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
from core.models.field_types import (
    LLMExtractedFieldTypeVar,
    ExtractionFieldType,
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
    bulk_record_gpt_batch_responses,
    bulk_upsert_gpt_batch_requests_with_only_req_bodies,
    mark_gpt_batch_requests_eager,
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


class EagerDispatchFailure(Exception):
    """One or more eager dispatches raised, AFTER every answer that did arrive was recorded.

    Raised only by the non-recursive eager path, which has no convergence loop to
    re-ask on: swallowing it there would leave the field incomplete, skip
    ``next_node`` and return normally — the silently-vanished-field shape
    ``BaseLLMRecursiveExtractionNode`` documents. Recursive nodes let their
    failures fall back into their own loop instead, bounded by
    ``MAX_UNPRODUCTIVE_PASSES``.
    """


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
        field_type: ExtractionFieldType,
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

    async def get_incomplete_req_ids(
        self,
        subject_unique_id: str,
        chunked_request_map: ExtractionRequestMap,
    ) -> set[BatchRequestIDType]:
        """The embedded request ids that have no response yet.

        Distinct from ``get_missing_req_ids``, which asks only whether a request
        DOCUMENT exists. A row can exist and still be unanswered — that is the
        state ``record_response_parse_error`` writes on purpose so the next pass
        re-asks — so the two sets are not interchangeable.
        """
        req_ids_to_lookup: set[BatchRequestIDType] = self.get_embedded_request_ids(
            subject_unique_id=subject_unique_id,
            chunked_request_map=chunked_request_map,
        )
        if not req_ids_to_lookup:
            return set()
        return req_ids_to_lookup - (
            await find_completed_gpt_batch_request_ids_only(
                subject_unique_id, list(req_ids_to_lookup)
            )
        )

    async def are_all_requests_complete(
        self,
        subject_unique_id: str,
        chunked_request_map: ExtractionRequestMap,
    ) -> bool:
        # Check if all search requests are complete
        incomplete_gpt_req_ids = await self.get_incomplete_req_ids(
            subject_unique_id=subject_unique_id,
            chunked_request_map=chunked_request_map,
        )
        logger.info(
            f"Checked whether all requests are complete; incomplete: {incomplete_gpt_req_ids}"
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
        field_type: ExtractionFieldType,
        chunk_bounds: str,
        extraction_bundle: ExtractionRequestBundle,
        completed_request_map: dict[BatchRequestIDType, GPTBatchRequest],
        timestamp: datetime,  # for recording errors
    ) -> ResultT:
        pass

    async def validate_own_responses(
        self,
        subject_unique_id: str,
        chunked_request_map: ExtractionRequestMap,
        completed_request_map: dict[BatchRequestIDType, GPTBatchRequest],
        timestamp: datetime,
    ) -> None:
        """Parse this node's own responses before any downstream node reads them.

        A node's responses were, until now, first parsed by whichever LATER node
        consumed them. On 2026-08-11 that meant the screening node logged COMPLETE
        and published its map at :57.211, and the grounding node discovered a
        malformed screening response at :57.276 — the failure surfaced against the
        node that read the data rather than the node that paid for it, one phase
        after the request that could be re-run had already been declared done.

        Overriding this puts the check back where the request was made: a bad
        response fails here, ``record_response_parse_error`` marks THIS node's
        request, and the map is never published to the pipeline context.

        Opt-in rather than a default that calls ``get_result``, because that is
        only safe where ``get_result`` is a pure parse over the map it is handed.
        ``LLMPhraseIterativeGroundingNode`` takes two request maps and a concept
        map, so it cannot be called from here at all, and the recursive-search
        nodes fold convergence work into theirs. A node opts in when parsing it
        twice is free, which it is wherever the work is JSON over a response that
        is already in memory.
        """
        return None

    @abstractmethod
    async def dispatch_batch_request(
        self,
        gpt_batch_request: GPTBatchRequest,
        metadata: ExtractionMetadata,
    ) -> GPTBatchResponse:
        pass

    async def dispatch_and_record_eagerly(
        self,
        subject_unique_id: str,
        requests_to_dispatch: list[GPTBatchRequest],
        metadata: ExtractionMetadata,
        timestamp: datetime,
    ) -> list[tuple[GPTBatchRequest, BaseException]]:
        """Dispatch every request, record every answer that arrived, return the failures.

        ``return_exceptions=True`` is the load-bearing part. A bare ``gather``
        raises on the FIRST failure, and ``bulk_record_gpt_batch_responses`` ran
        only after it returned — so one rate-limited request (the provider 429
        that survives the LiteLLM proxy's ``num_retries`` and the OpenAI client's
        own ``max_retries``) threw away the answers of every sibling in the same
        gather. Those siblings had been answered and paid for, were never
        written to Mongo, and the next run re-sent and re-paid for all of them.

        Recording is now per request, so the unanswered set is exactly the set
        that actually failed — which is also the set both callers re-ask, since
        ``find_incomplete_gpt_batch_requests_by_custom_ids`` selects on
        ``response == None``.
        """
        if not requests_to_dispatch:
            return []

        # Claim every PENDING row (batch_id None) before sending it. That is the
        # state record_response_parse_error writes so the next pass re-asks —
        # and the state dispatch_gpt_batch_request refuses, because a pending
        # row belongs to the batch-file path. Until 2026-09-05 the refusal
        # made the parse-error retry unreachable in eager mode: the row was
        # re-found, re-refused, and the pass bound tripped (run
        # 20260904T184906, one subject lost to one truncated answer). Written
        # to Mongo first, because completion is judged by batch_id != None AND
        # response != None, and a dispatch that fails must leave a row the
        # next pass can send again (see mark_gpt_batch_requests_eager).
        pending = [req for req in requests_to_dispatch if req.batch_id is None]
        if pending:
            await mark_gpt_batch_requests_eager(
                timestamp=timestamp,
                custom_ids={req.request.custom_id for req in pending},
            )
            for req in pending:
                req.batch_id = "Eager"
            logger.info(
                f"[{subject_unique_id}] {self.__class__.__name__} "
                f"('{self.field_type.name}') claimed {len(pending)} parse-failed "
                f"request(s) for eager re-dispatch."
            )

        results = await asyncio.gather(
            *[
                self.dispatch_batch_request(
                    gpt_batch_request=req,
                    metadata=metadata,
                )
                for req in requests_to_dispatch
            ],
            return_exceptions=True,
        )

        answered: list[GPTBatchRequest] = []
        response_blobs: list[GPTBatchResponse] = []
        failures: list[tuple[GPTBatchRequest, BaseException]] = []
        for req, result in zip(requests_to_dispatch, results):
            if isinstance(result, BaseException):
                failures.append((req, result))
            else:
                answered.append(req)
                response_blobs.append(result)

        modified_count = failed_updates = 0
        if answered:
            modified_count, failed_updates = await bulk_record_gpt_batch_responses(
                batch_requests=answered,
                response_blobs=response_blobs,
                timestamp=timestamp,
            )
        logger.info(
            f"[{subject_unique_id}] ✅ Eagerly dispatched {len(requests_to_dispatch)} batch requests for "
            f"{self.__class__.__name__} ('{self.field_type.name}'): {modified_count} response(s) recorded, "
            f"{failed_updates} failed update(s), {len(failures)} dispatch failure(s)."
        )
        for failed_req, exc in failures:
            logger.error(
                f"[{subject_unique_id}] ❌ {self.__class__.__name__} ('{self.field_type.name}') dispatch failed "
                f"for {failed_req.request.custom_id}: {type(exc).__name__}: {exc}"
            )
        return failures

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
        if await self.stop_if_stage_disabled(
            subject=subject,
            deferred_subject=deferred_subject,
            scraped_text_file=scraped_text_file,
            timestamp=timestamp,
            pipeline_context=pipeline_context,
        ):
            return

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
            failures = await self.dispatch_and_record_eagerly(
                subject_unique_id=subject.subject_unique_id,
                requests_to_dispatch=list(incomplete_requests.values()),
                metadata=extraction_requests.metadata,
                timestamp=timestamp,
            )
            if failures:
                # Recorded first, then raised. This node has no loop to re-ask
                # on, and an incomplete field returns without calling next_node
                # and without an error — so the raise is what keeps the failure
                # visible (an ExtractionError row naming this field, written by
                # the orchestrator's per-field handler).
                first_failed_req, first_exc = failures[0]
                raise EagerDispatchFailure(
                    f"{self.__class__.__name__} ('{self.field_type.name}') for "
                    f"{subject.subject_unique_id}: {len(failures)} of "
                    f"{len(incomplete_requests)} eager dispatches failed. The answers "
                    f"that arrived were recorded, so only the failed request(s) are "
                    f"still unanswered. First failure on "
                    f"{first_failed_req.request.custom_id}: "
                    f"{type(first_exc).__name__}: {first_exc}"
                ) from first_exc

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
            await self.validate_own_responses(
                subject_unique_id=subject.subject_unique_id,
                chunked_request_map=extraction_requests.chunked_request_map,
                completed_request_map=completed_request_map,
                timestamp=timestamp,
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
