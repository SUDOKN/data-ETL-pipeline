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

from llm_providers.services.gpt_batch_request.gpt_batch_request_queries import (
    find_incomplete_gpt_batch_requests_by_custom_ids,
)
from llm_providers.services.gpt_batch_request.gpt_batch_request_writes import (
    RepeatedParseFailure,
    bulk_record_gpt_batch_responses,
    bulk_upsert_gpt_batch_requests_with_only_req_bodies,
)

logger = logging.getLogger(__name__)

# Consecutive convergence passes that create no request and answer none of the
# outstanding ones before the node gives up. Small on purpose: by this point
# every request has been dispatched and its response has failed to record.
MAX_UNPRODUCTIVE_PASSES = 3


class BaseLLMRecursiveExtractionNode(
    BaseLLMExtractionNode[LLMExtractedFieldTypeVar, ResultT],
):
    """Base class for recursive extraction phases.

    Concrete recursive nodes keep their own recursion rules inside
    ``embed_request_ids``. This base only owns eager convergence: keep embedding,
    creating, dispatching, and recording request ids until the recursive node
    reports no missing request ids AND nothing embedded is still unanswered,
    then proceed to the next node.

    Convergence is over the requests that are UNANSWERED, not merely the ones
    absent from the DB. Until 2026-08-24 the loop dispatched only the requests
    it had just created, so a stored request with no response — the state
    ``record_response_parse_error`` writes ON PURPOSE (it nulls ``response`` and
    ``batch_id`` so the next pass re-asks, capped by ``RESPONSE_PARSE_ERROR_CAP``)
    — was never re-dispatched. ``get_missing_req_ids`` only asks whether a
    request DOCUMENT exists, so such a row is never "missing"; the loop broke,
    ``are_all_requests_complete`` stayed False, and the method returned without
    calling ``next_node``: no exception, no dump, a silently vanished field.
    That cost 36% of run 20260824T012721's records and had disabled the
    parse-error retry for every recursive stage since it was written. The
    non-recursive base never had the bug — its eager path dispatches whatever
    ``find_incomplete_gpt_batch_requests_by_custom_ids`` returns — so this now
    does the same.

    That fix made the re-dispatch REACHABLE; it did not make it REACHED. The
    parse failure that arms it is raised from inside ``embed_request_ids``, at
    the top of the loop, so until 2026-08-25 the exception escaped before the
    dispatch at the bottom could spend a single one of the
    ``RESPONSE_PARSE_ERROR_CAP`` re-asks. A parse error is now HELD for the
    rest of the pass and re-raised only if the pass left nothing to re-ask;
    ``RepeatedParseFailure`` is never held, because it is raised instead of
    recording and so arms nothing. Two bounds stop a spin:
    ``RESPONSE_PARSE_ERROR_CAP`` on one request's parse failures, and
    ``MAX_UNPRODUCTIVE_PASSES`` on passes that shrink nothing.
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

        # Convergence bound. A parse failure that repeats is already bounded —
        # record_response_parse_error_capped raises RepeatedParseFailure after
        # RESPONSE_PARSE_ERROR_CAP attempts — but a dispatch whose response never
        # RECORDS (failed_updates) would leave the same rows incomplete forever,
        # and unlike the old loop this one does not stop just because no new ids
        # were discovered. Count passes that create nothing and shrink nothing.
        unproductive_passes = 0
        previously_incomplete: set[BatchRequestIDType] | None = None

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
            # A response that fails to parse in here has ALREADY been recorded
            # by record_response_parse_error_capped, which nulls its response
            # and batch_id precisely so the dispatch below re-asks it. Until
            # 2026-08-25 the exception escaped this loop instead, so the
            # CAP re-dispatches that helper's docstring promises were never
            # spent: run 20260824T190359 surfaced a grounding parse failure on
            # attempt 1 of 4 and lost both subjects. Held here so the pass can
            # finish and the retry can happen.
            #
            # RepeatedParseFailure passes through: it is raised INSTEAD of
            # recording, so nothing became re-dispatchable and it is the signal
            # that the budget is spent. Anything else is re-raised below unless
            # this pass actually left work to re-dispatch — an error that made
            # nothing incomplete cannot be retried, only spun on.
            embed_error: Exception | None = None
            try:
                await self.embed_request_ids(
                    subject_unique_id=subject.subject_unique_id,
                    pipeline_context=pipeline_context,
                    metadata=metadata,
                    chunked_request_map=chunked_request_map,
                    timestamp=timestamp,
                )
            except RepeatedParseFailure:
                raise
            except Exception as exc:
                embed_error = exc
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

            if missing_req_ids:
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

            # Dispatch everything embedded that has no answer yet — the ids just
            # created AND any stored row a previous pass or a previous RUN left
            # unanswered. Reading the incomplete set back from the DB is also
            # what keeps a pre-answered request unsent: a node may create a
            # request that is answered at creation (the mention stage's single
            # dummy for a window with nothing to locate, built for NO_MODEL with
            # its response pre-filled), and sending one tripped the model guard
            # in dispatch_gpt_batch_request and stopped a whole subject until
            # 2026-08-23. Such a row is complete, so it is never returned here.
            all_request_ids = self.get_embedded_request_ids(
                subject_unique_id=deferred_subject.subject_unique_id,
                chunked_request_map=chunked_request_map,
            )
            # The query rejects an empty id list rather than returning nothing.
            incomplete_requests = (
                await find_incomplete_gpt_batch_requests_by_custom_ids(
                    deferred_subject.subject_unique_id, list(all_request_ids)
                )
                if all_request_ids
                else {}
            )
            if embed_error is not None:
                # Nothing to re-ask means the error was not a recorded parse
                # failure — a genuine bug, a re-defer, a malformed request —
                # and retrying would spin against it. Surface it unchanged.
                if not missing_req_ids and not incomplete_requests:
                    raise embed_error
                logger.warning(
                    f"[{subject.subject_unique_id}] {self.__class__.__name__} "
                    f"('{self.field_type.name}') held an embedding error and will "
                    f"re-dispatch {len(incomplete_requests)} unanswered request(s): "
                    f"{embed_error}"
                )

            if not missing_req_ids and not incomplete_requests:
                break

            incomplete_ids = set(incomplete_requests)
            if (
                not missing_req_ids
                and previously_incomplete is not None
                and not (incomplete_ids < previously_incomplete)
            ):
                unproductive_passes += 1
                if unproductive_passes >= MAX_UNPRODUCTIVE_PASSES:
                    raise ValueError(
                        f"{self.__class__.__name__} ('{self.field_type.name}') for "
                        f"{subject.subject_unique_id} made no progress in "
                        f"{MAX_UNPRODUCTIVE_PASSES} passes: {len(incomplete_ids)} request(s) "
                        f"are still unanswered after being dispatched. Incomplete request "
                        f"ids: {sorted(incomplete_ids)}"
                    )
            else:
                unproductive_passes = 0
            previously_incomplete = incomplete_ids

            requests_to_dispatch = list(incomplete_requests.values())
            logger.info(
                f"[{subject.subject_unique_id}] 🚀 Eager execution enabled. Dispatching "
                f"{len(requests_to_dispatch)} incomplete batch requests for "
                f"{self.__class__.__name__} ('{self.field_type.name}') immediately."
            )

            batch_response_blobs = await asyncio.gather(
                *[
                    self.dispatch_batch_request(
                        gpt_batch_request=req,
                        metadata=metadata,
                    )
                    for req in requests_to_dispatch
                ]
            )
            modified_count, failed_updates = await bulk_record_gpt_batch_responses(
                batch_requests=requests_to_dispatch,
                response_blobs=batch_response_blobs,
                timestamp=timestamp,
            )
            logger.info(
                f"[{subject.subject_unique_id}] ✅ Eagerly dispatched {len(requests_to_dispatch)} "
                f"batch requests for {self.__class__.__name__} ('{self.field_type.name}') with "
                f"{modified_count} successful response recordings and {failed_updates} "
                f"failed updates."
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
        else:
            # Never silent. The loop only exits with work outstanding when the
            # convergence bound above did not catch it; returning here is what
            # made three fields of run 20260824T012721 disappear with no
            # exception, no log and no dump while the sweep reported success.
            incomplete = await self.get_incomplete_req_ids(
                subject_unique_id=subject.subject_unique_id,
                chunked_request_map=extraction_requests.chunked_request_map,
            )
            raise ValueError(
                f"{self.__class__.__name__} ('{self.field_type.name}') for "
                f"{subject.subject_unique_id} finished its convergence loop with "
                f"{len(incomplete)} request(s) still unanswered, so "
                f"{self.next_node.__class__.__name__ if self.next_node else 'the next node'} "
                f"cannot run. Incomplete request ids: {sorted(incomplete)}"
            )
