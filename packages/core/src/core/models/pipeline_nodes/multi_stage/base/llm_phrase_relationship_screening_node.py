from __future__ import annotations
import logging
from datetime import datetime
from typing import TYPE_CHECKING, Any, ClassVar

from llm_providers.db_models.gpt_batch_request import (
    GPTBatchRequest,
)
from llm_providers.models.open_ai.gpt_batch_response_blob import (
    GPTBatchResponse,
)
from core.models.deferred_extraction.deferred_phrase_extraction_requests import (
    LLMPhraseExtractionRequestBundle,
    LLMPhraseExtractionRequestMap,
)
from core.models.extraction_results.llm_phrase_extraction_results_v2 import (
    LLMPhraseExtractionMetadataV2,
)
from core.models.extraction_schemas.screening import (
    RecordScreeningResults,
)
from llm_providers.models.file_objects.prompt import Prompt
from core.models.field_types import ExtractionFieldType
from core.models.pipeline_nodes.base.base_node import (
    LLMExtractedFieldTypeVar,
    PipelineContext,
)
from core.models.pipeline_nodes.base.base_reconcile_node import (
    ReconcileNode,
)
from core.models.pipeline_nodes.base.base_llm_extraction_node import (
    BaseLLMExtractionNode,
)
from core.models.pipeline_nodes.base.base_llm_recursive_extraction_node import (
    BaseLLMRecursiveExtractionNode,
)
from llm_providers.field_types import BatchRequestIDType

if TYPE_CHECKING:
    from scraper.models.s3.scraped_text_file import (
        ScrapedTextFile,
    )

from llm_providers.services.gpt_batch_request.gpt_batch_request_service import (
    dispatch_gpt_batch_request,
)
from core.services.pipeline_nodes.multi_stage.llm_grounding_node_service import (
    grouped_record_payloads,
)
from core.services.pipeline_nodes.multi_stage.llm_phrase_synthesis_node_service import (
    get_chunk_group_records,
)
from core.services.pipeline_nodes.multi_stage.llm_relationship_screening_node_service import (
    build_screening_payloads,
    create_missing_record_screening_requests,
    get_chunk_record_screening_answer,
    get_record_screening_result,
    screening_catalog_for,
)
from core.utils.request_custom_id_util import upstream_digest_segment
from core.models.pipeline_nodes.base.pipeline_stage import (
    STAGE_REQUEST_ID_TOKEN,
    PipelineStage,
)

logger = logging.getLogger(__name__)


class LLMPhraseRelationshipScreeningNode(
    BaseLLMRecursiveExtractionNode[LLMExtractedFieldTypeVar, RecordScreeningResults]
):
    """The consolidated screening: it vets EVERY candidate the grounding
    pass(es) enumerated, per record, against the record's own deposition —
    records and candidates, never chunk text (fork F8). Since 3.3 the records
    are the synthesis stage's per-group records (D16). What the candidates
    are (in-vocab ∪ OOV for concepts, freehand mints for keywords) is the
    subclasses' business, via ``get_chunk_candidates_by_record``.

    A recursive node since 3.3, for the under-answer retry (the synthesis
    stage's two-pass policy, ported by user decision 2026-08-24): pass 1
    embeds every chunk's group requests; once those are complete, pass 2
    ASSESSES each chunk — record ids the answers left unanswered are stored,
    and a chunk with any gets ONE retry request set for just those records."""

    stage: ClassVar[PipelineStage] = PipelineStage.screening

    def __init__(
        self,
        field_type: LLMExtractedFieldTypeVar,
        next_node: BaseLLMExtractionNode | ReconcileNode,
        phrase_relationship_screening_prompt: Prompt,
    ):
        super().__init__(
            field_type=field_type,
            next_node=next_node,
        )
        self.phrase_relationship_screening_prompt = phrase_relationship_screening_prompt

    def get_upstream_mention_collection_map(
        self, pipeline_context: PipelineContext
    ) -> dict[BatchRequestIDType, GPTBatchRequest]:
        """The completed mention-collection request map — what the fold behind
        the synthesis result is recomputed from."""
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement get_upstream_mention_collection_map"
        )

    def get_upstream_synthesis_map(
        self, pipeline_context: PipelineContext
    ) -> dict[BatchRequestIDType, GPTBatchRequest]:
        """The completed synthesis request map — the per-group records this
        stage consumes are derived from its held answers."""
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement get_upstream_synthesis_map"
        )

    @staticmethod
    def _subject_text_of(
        pipeline_context: PipelineContext, subject_unique_id: str, field: str
    ) -> str:
        subject_text = pipeline_context.subject_text
        if subject_text is None:
            raise ValueError(
                f"Cannot embed req ids for record screening: "
                f"PipelineContext.subject_text is None for subject:{subject_unique_id}, "
                f"field:{field}. The orchestrator must set it (the node recomputes the "
                f"synthesis stage's fold from the text when it derives its records)."
            )
        return subject_text

    @staticmethod
    def _subject_name_of(
        pipeline_context: PipelineContext, subject_unique_id: str, field: str
    ) -> str:
        subject_name = pipeline_context.subject_name
        if not subject_name:
            raise ValueError(
                f"Cannot create record screening requests: "
                f"PipelineContext.subject_name is empty for subject:{subject_unique_id}, "
                f"field:{field}. The orchestrator sets it; the request names the "
                f"manufacturer so the judge knows whose record it is reading (3.3 rider)."
            )
        return subject_name

    async def get_chunk_candidates_by_record(
        self,
        subject_unique_id: str,
        chunk_bounds: str,
        extraction_bundle: LLMPhraseExtractionRequestBundle,
        pipeline_context: PipelineContext,
        timestamp: datetime,
    ) -> dict[str, list[str]]:
        """Each record's candidate list for this chunk, derived from the
        grounding stage(s) upstream of screening in this pipeline family."""
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement get_chunk_candidates_by_record"
        )

    async def _chunk_screening_payloads(
        self,
        subject_unique_id: str,
        chunk_bounds: str,
        extraction_bundle: LLMPhraseExtractionRequestBundle,
        pipeline_context: PipelineContext,
        subject_text: str,
        metadata: LLMPhraseExtractionMetadataV2,
        timestamp: datetime,
    ) -> dict[str, dict[str, Any]]:
        group_records = await get_chunk_group_records(
            subject_unique_id,
            self.field_type,
            chunk_bounds,
            extraction_bundle,
            timestamp,
            synthesis_completed_request_map=self.get_upstream_synthesis_map(
                pipeline_context
            ),
            mention_completed_request_map=self.get_upstream_mention_collection_map(
                pipeline_context
            ),
            subject_text=subject_text,
            metadata=metadata,
        )
        candidates_by_record = await self.get_chunk_candidates_by_record(
            subject_unique_id=subject_unique_id,
            chunk_bounds=chunk_bounds,
            extraction_bundle=extraction_bundle,
            pipeline_context=pipeline_context,
            timestamp=timestamp,
        )
        return build_screening_payloads(group_records, candidates_by_record)

    async def embed_request_ids(  # prefill folded into this function
        self,
        subject_unique_id: str,
        pipeline_context: PipelineContext,
        metadata: LLMPhraseExtractionMetadataV2,
        chunked_request_map: LLMPhraseExtractionRequestMap,
        timestamp: datetime,
    ):
        if not chunked_request_map:
            raise ValueError(
                f"Cannot embed req ids for llm phrase relationship screening, "
                f"as chunked_request_map found empty for subject:{subject_unique_id}, field:{self.field_type.name}."
            )

        # Every grounding pass upstream has fully executed by the time screening
        # embeds ids, so each record's candidate set is final and the group
        # count can be computed once, upfront.
        max_pairs_per_request = (
            metadata.llm_phrase_relationship_screening.max_pairs_per_request
        )
        subject_text = self._subject_text_of(
            pipeline_context, subject_unique_id, self.field_type.name
        )

        # PASS 1 — the group requests.
        embedded_groups = False
        for (
            chunk_bounds,
            extraction_request_bundle,
        ) in chunked_request_map.items():
            if extraction_request_bundle.llm_phrase_relationship_screening_req_ids:
                continue  # already embedded; group count is stable once computed

            payloads = await self._chunk_screening_payloads(
                subject_unique_id=subject_unique_id,
                chunk_bounds=chunk_bounds,
                extraction_bundle=extraction_request_bundle,
                pipeline_context=pipeline_context,
                subject_text=subject_text,
                metadata=metadata,
                timestamp=timestamp,
            )
            embedded_groups = True
            extraction_request_bundle.llm_phrase_relationship_screening_req_ids = [
                self.get_request_custom_id(
                    subject_unique_id=subject_unique_id,
                    field_type=self.field_type,
                    chunk_bounds=chunk_bounds,
                    group_index=group_index,
                    metadata=metadata,
                    group_payload=payload_group,
                )
                for group_index, payload_group in enumerate(
                    grouped_record_payloads(payloads, max_pairs_per_request)
                )
            ]
        if embedded_groups:
            return  # the groups must complete before any chunk can be assessed

        # PASS 2 — assessment + the retry (see the class docstring). A chunk is
        # assessed once, when every embedded request of the field is complete.
        unassessed = [
            (chunk_bounds, bundle)
            for chunk_bounds, bundle in chunked_request_map.items()
            if bundle.llm_phrase_relationship_screening_retry_record_ids is None
        ]
        if not unassessed:
            return
        if not await self.are_all_requests_complete(
            subject_unique_id=subject_unique_id, chunked_request_map=chunked_request_map
        ):
            logger.info(
                f"[{subject_unique_id}] Waiting for the record screening requests to "
                f"complete before assessing chunks for a retry ({self.field_type.name})."
            )
            return
        completed_request_map = await self.get_completed_request_map(
            subject_unique_id=subject_unique_id, chunked_request_map=chunked_request_map
        )
        for chunk_bounds, bundle in unassessed:
            answer = await get_chunk_record_screening_answer(
                subject_unique_id=subject_unique_id,
                field_name=self.field_type.name,
                chunk_bounds=chunk_bounds,
                catalog=screening_catalog_for(self.field_type.name),
                group_req_ids=bundle.llm_phrase_relationship_screening_req_ids,
                completed_request_map=completed_request_map,
                timestamp=timestamp,
            )
            missing = answer.missing_ids
            bundle.llm_phrase_relationship_screening_retry_record_ids = missing
            if not missing:
                continue
            payloads = await self._chunk_screening_payloads(
                subject_unique_id=subject_unique_id,
                chunk_bounds=chunk_bounds,
                extraction_bundle=bundle,
                pipeline_context=pipeline_context,
                subject_text=subject_text,
                metadata=metadata,
                timestamp=timestamp,
            )
            unknown = [rid for rid in missing if rid not in payloads]
            if unknown:
                raise ValueError(
                    f"record_screening: assessed missing record id(s) {unknown} of "
                    f"chunk {chunk_bounds} in {subject_unique_id}:{self.field_type.name} "
                    f"are not among the chunk's records; the upstream state changed "
                    f"under the stored request ids (re-defer)."
                )
            retry_payloads = {rid: payloads[rid] for rid in missing}
            bundle.llm_phrase_relationship_screening_retry_req_ids = [
                self.get_request_custom_id(
                    subject_unique_id=subject_unique_id,
                    field_type=self.field_type,
                    chunk_bounds=chunk_bounds,
                    group_index=group_index,
                    metadata=metadata,
                    group_payload=payload_group,
                    retry_index=1,
                )
                for group_index, payload_group in enumerate(
                    grouped_record_payloads(retry_payloads, max_pairs_per_request)
                )
            ]
            logger.info(
                f"[{subject_unique_id}] record screening: {len(missing)} of "
                f"{len(answer.sent_ids)} record(s) in chunk {chunk_bounds} "
                f"({self.field_type.name}) came back unanswered; embedding "
                f"{len(bundle.llm_phrase_relationship_screening_retry_req_ids)} retry request(s)."
            )

    def get_embedded_request_ids(
        self,
        subject_unique_id: str,
        chunked_request_map: LLMPhraseExtractionRequestMap,
    ) -> set[BatchRequestIDType]:
        llm_phrase_relationship_screening_req_ids: set[BatchRequestIDType] = set()
        for (
            chunk_bounds,
            extraction_bundle,
        ) in chunked_request_map.items():
            if not extraction_bundle.llm_phrase_relationship_screening_req_ids:
                raise ValueError(
                    f"get_embedded_request_ids was called for {subject_unique_id}:{self.field_type.name} but "
                    f"extraction_bundle.llm_phrase_relationship_screening_req_ids is empty for chunk bounds {chunk_bounds}."
                )

            llm_phrase_relationship_screening_req_ids.update(
                extraction_bundle.llm_phrase_relationship_screening_req_ids
            )
            llm_phrase_relationship_screening_req_ids.update(
                extraction_bundle.llm_phrase_relationship_screening_retry_req_ids
            )

        return llm_phrase_relationship_screening_req_ids

    @staticmethod
    def get_request_custom_id(
        subject_unique_id: str,
        field_type: ExtractionFieldType,
        chunk_bounds: str,
        group_index: int,
        metadata: LLMPhraseExtractionMetadataV2,
        group_payload: dict[str, dict[str, Any]],
        retry_index: int | None = None,
    ) -> BatchRequestIDType:
        # `|ud=` (fork F12): the payload carries the records AND their candidate
        # lists, so a grounding change re-asks the screening question instead of
        # replaying a verdict on candidates that no longer exist — since 3.3 the
        # records are the synthesis stage's group records (D16). A retry request
        # carries `>retry>{n}>` before its group index.
        retry = f"retry>{retry_index}>" if retry_index is not None else ""
        return (
            f"{subject_unique_id}>{field_type.name}"
            f">{STAGE_REQUEST_ID_TOKEN[PipelineStage.screening]}"
            f">{retry}group>{group_index}>chunk>{chunk_bounds}>"
            f"{metadata.llm_phrase_relationship_screening.to_custom_id_segment()}"
            f"{upstream_digest_segment(group_payload)}"
        )

    async def create_batch_requests(
        self,
        subject_unique_id: str,
        scraped_text_file: ScrapedTextFile,
        missing_request_ids: set[BatchRequestIDType],
        metadata: LLMPhraseExtractionMetadataV2,
        chunked_request_map: LLMPhraseExtractionRequestMap,
        pipeline_context: PipelineContext,
        timestamp: datetime,
        eager: bool,
    ) -> list[GPTBatchRequest]:
        chunk_payload_maps: dict[str, dict[str, dict[str, Any]]] = {}
        group_req_ids_by_chunk: dict[str, list[BatchRequestIDType]] = {}
        retry_req_ids_by_chunk: dict[str, list[BatchRequestIDType]] = {}
        retry_record_ids_by_chunk: dict[str, list[str]] = {}
        for chunk_bounds, extraction_bundle in chunked_request_map.items():
            group_req_ids_by_chunk[chunk_bounds] = (
                extraction_bundle.llm_phrase_relationship_screening_req_ids
            )
            retry_req_ids_by_chunk[chunk_bounds] = (
                extraction_bundle.llm_phrase_relationship_screening_retry_req_ids
            )
            retry_record_ids_by_chunk[chunk_bounds] = (
                extraction_bundle.llm_phrase_relationship_screening_retry_record_ids
                or []
            )
            if not (
                (
                    set(extraction_bundle.llm_phrase_relationship_screening_req_ids)
                    | set(
                        extraction_bundle.llm_phrase_relationship_screening_retry_req_ids
                    )
                )
                & missing_request_ids
            ):
                continue
            chunk_payload_maps[chunk_bounds] = await self._chunk_screening_payloads(
                subject_unique_id=subject_unique_id,
                chunk_bounds=chunk_bounds,
                extraction_bundle=extraction_bundle,
                pipeline_context=pipeline_context,
                subject_text=scraped_text_file.text,
                metadata=metadata,
                timestamp=timestamp,
            )

        return await create_missing_record_screening_requests(
            subject_unique_id=subject_unique_id,
            field_name=self.field_type.name,
            chunk_payload_maps=chunk_payload_maps,
            group_req_ids_by_chunk=group_req_ids_by_chunk,
            retry_req_ids_by_chunk=retry_req_ids_by_chunk,
            retry_record_ids_by_chunk=retry_record_ids_by_chunk,
            missing_req_ids=missing_request_ids,
            prompt=self.phrase_relationship_screening_prompt,
            catalog=screening_catalog_for(self.field_type.name),
            subject_name=self._subject_name_of(
                pipeline_context, subject_unique_id, self.field_type.name
            ),
            max_records_per_request=metadata.llm_phrase_relationship_screening.max_pairs_per_request,
            deferred_at=timestamp,
            llm_model=metadata.llm_phrase_relationship_screening.llm_model,
            model_params=metadata.llm_phrase_relationship_screening.model_params,
            eager=eager,
        )

    @staticmethod
    async def get_result(
        subject_unique_id: str,
        field_type: ExtractionFieldType,
        chunk_bounds: str,
        extraction_bundle: LLMPhraseExtractionRequestBundle,
        completed_request_map: dict[BatchRequestIDType, GPTBatchRequest],
        timestamp: datetime,  # for recording errors
    ) -> RecordScreeningResults:
        return await get_record_screening_result(
            subject_unique_id=subject_unique_id,
            field_name=field_type.name,
            chunk_bounds=chunk_bounds,
            catalog=screening_catalog_for(field_type.name),
            group_req_ids=extraction_bundle.llm_phrase_relationship_screening_req_ids,
            retry_req_ids=extraction_bundle.llm_phrase_relationship_screening_retry_req_ids,
            completed_request_map=completed_request_map,
            timestamp=timestamp,
        )

    async def validate_own_responses(
        self,
        subject_unique_id: str,
        chunked_request_map: LLMPhraseExtractionRequestMap,
        completed_request_map: dict[BatchRequestIDType, GPTBatchRequest],
        timestamp: datetime,
    ) -> None:
        """Parse every screening response here, where the requests were made.

        ``get_result`` is a pure parse over a map already in memory — cheap
        against being told which of this node's requests to re-run by the node
        that made them, one phase before a consumer trips over it. Both axes
        (record ids AND candidate sets) are held inside the group parse.
        """
        for chunk_bounds, extraction_bundle in chunked_request_map.items():
            await self.get_result(
                subject_unique_id=subject_unique_id,
                field_type=self.field_type,
                chunk_bounds=chunk_bounds,
                extraction_bundle=extraction_bundle,
                completed_request_map=completed_request_map,
                timestamp=timestamp,
            )

    async def dispatch_batch_request(
        self,
        gpt_batch_request: GPTBatchRequest,
        metadata: LLMPhraseExtractionMetadataV2,
    ) -> GPTBatchResponse:
        return await dispatch_gpt_batch_request(
            gpt_batch_request=gpt_batch_request,
            gpt_model=metadata.llm_phrase_relationship_screening.llm_model,
        )
