from __future__ import annotations
import logging
from datetime import datetime

from llm_providers.db_models.gpt_batch_request import (
    GPTBatchRequest,
)
from llm_providers.models.open_ai.gpt_batch_response_blob import (
    GPTBatchResponse,
)
from core.models.deferred_extraction.deferred_concept_extraction import (
    ConceptExtractionRequestBundle,
    ConceptExtractionRequestMap,
)
from core.models.extraction_results.llm_phrase_extraction_results import (
    ConceptExtractionMetadata,
)
from core.models.extraction_schemas.grounding import (
    RecordGroundingResults,
)
from core.models.rule_catalog import STAGE_INITIAL_GROUNDING, RuleCatalog
from llm_providers.models.file_objects.prompt import Prompt
from core.models.skos_concept import Concept
from core.models.field_types import (
    ConceptFieldType,
    ExtractionFieldType,
)
from core.models.pipeline_nodes.base.base_node import (
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
from scraper.models.s3.scraped_text_file import ScrapedTextFile

from llm_providers.services.gpt_batch_request.gpt_batch_request_service import (
    dispatch_gpt_batch_request,
)
from core.services.pipeline_nodes.multi_stage.llm_grounding_node_service import (
    build_group_record_payloads,
    create_missing_record_grounding_requests,
    get_chunk_record_grounding_answer,
    get_record_grounding_result,
    grouped_record_payloads,
    retry_record_payloads,
)
from core.services.pipeline_nodes.multi_stage.llm_phrase_synthesis_node_service import (
    get_chunk_group_records,
)
from core.services.rule_catalog_registry import get_rule_catalog

from core.utils.rdf_to_graph_util import (
    get_match_label_to_concept_map,
    render_concept_outline,
)
from core.utils.request_custom_id_util import upstream_digest_segment
from typing import Any, ClassVar, Optional
from core.models.pipeline_nodes.base.pipeline_stage import (
    STAGE_REQUEST_ID_TOKEN,
    PipelineStage,
)

logger = logging.getLogger(__name__)


class LLMPhraseInitialGroundingNode(
    BaseLLMRecursiveExtractionNode[ConceptFieldType, RecordGroundingResults]
):
    """The in-vocab enumeration pass: every synthesized group record, against
    the full vocabulary, under candidate-only rules (no attribution). Runs
    straight off the synthesis stage (3.3, D16: per-group records keyed
    group_id) — screening sits downstream and vets every candidate this pass
    emits.

    A recursive node since 3.3, for the under-answer retry (the synthesis
    stage's two-pass policy, ported by user decision 2026-08-24): pass 1
    embeds every chunk's group requests; once those are complete, pass 2
    ASSESSES each chunk — record ids the answers left unanswered are stored,
    and a chunk with any gets ONE retry request set for just those records."""

    stage: ClassVar[PipelineStage] = PipelineStage.initial_grounding

    def __init__(
        self,
        field_type: ConceptFieldType,
        next_node: BaseLLMExtractionNode | ReconcileNode,
        phrase_initial_grounding_prompt: Prompt,
        known_concepts: set[Concept],
    ):
        super().__init__(
            field_type=field_type,
            next_node=next_node,
        )
        self.phrase_initial_grounding_prompt = phrase_initial_grounding_prompt
        self.known_concepts = known_concepts
        self.match_label_to_concept_map = get_match_label_to_concept_map(known_concepts)


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
                f"Cannot embed req ids for in-vocab grounding: "
                f"PipelineContext.subject_text is None for subject:{subject_unique_id}, "
                f"field:{field}. The orchestrator must set it (the node recomputes the "
                f"synthesis stage's fold from the text when it derives its records)."
            )
        return subject_text

    def catalog(self) -> RuleCatalog:
        return get_rule_catalog(STAGE_INITIAL_GROUNDING, self.field_type.name)

    def options_section(self) -> str:
        """The pre-rendered outline block a request appends after its records."""
        return (
            f"options to match against:\n"
            f"{render_concept_outline(self.known_concepts)}"
        )

    async def _chunk_record_payloads(
        self,
        subject_unique_id: str,
        chunk_bounds: str,
        extraction_bundle: ConceptExtractionRequestBundle,
        synthesis_map: dict[BatchRequestIDType, GPTBatchRequest],
        subject_text: str,
        metadata: ConceptExtractionMetadata,
        timestamp: datetime,
    ) -> dict[str, dict[str, Any]]:
        """The chunk's group_id → payload map: every SYNTHESIZED group (a
        group left unsynthesized has nothing to judge and is absent — the v3
        analog of the v2 evidence filter). Both id-embedding (group count +
        digests) and request creation derive from this identically."""
        group_records = await get_chunk_group_records(
            subject_unique_id,
            self.field_type,
            chunk_bounds,
            extraction_bundle,
            timestamp,
            synthesis_completed_request_map=synthesis_map,
            subject_text=subject_text,
            metadata=metadata,
        )
        return build_group_record_payloads(group_records)

    async def embed_request_ids(  # prefill folded into this function
        self,
        subject_unique_id: str,
        pipeline_context: PipelineContext,
        metadata: ConceptExtractionMetadata,
        chunked_request_map: ConceptExtractionRequestMap,
        timestamp: datetime,
    ):
        if not chunked_request_map:
            raise ValueError(
                f"Cannot embed req ids for llm initial grounding node, "
                f"as chunked_request_map found empty for subject:{subject_unique_id}, field:{self.field_type.name}."
            )

        # The synthesis stage has fully executed by the time grounding embeds
        # ids, so the group-record set for a chunk is final and the group count
        # can be computed once, upfront.
        max_pairs_per_request = (
            metadata.llm_phrase_initial_grounding.max_pairs_per_request
        )
        synthesis_map = self.get_upstream_synthesis_map(pipeline_context)
        subject_text = self._subject_text_of(
            pipeline_context, subject_unique_id, self.field_type.name
        )

        # PASS 1 — the group requests.
        embedded_groups = False
        for (
            chunk_bounds,
            extraction_request_bundle,
        ) in chunked_request_map.items():
            if extraction_request_bundle.llm_phrase_initial_grounding_req_ids:
                continue  # already embedded; group count is stable once computed

            payloads = await self._chunk_record_payloads(
                subject_unique_id=subject_unique_id,
                chunk_bounds=chunk_bounds,
                extraction_bundle=extraction_request_bundle,
                synthesis_map=synthesis_map,
                subject_text=subject_text,
                metadata=metadata,
                timestamp=timestamp,
            )
            embedded_groups = True
            extraction_request_bundle.llm_phrase_initial_grounding_req_ids = [
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
            if bundle.llm_phrase_initial_grounding_retry_record_ids is None
        ]
        if not unassessed:
            return
        if not await self.are_all_requests_complete(
            subject_unique_id=subject_unique_id, chunked_request_map=chunked_request_map
        ):
            logger.info(
                f"[{subject_unique_id}] Waiting for the in-vocab grounding requests to "
                f"complete before assessing chunks for a retry ({self.field_type.name})."
            )
            return
        completed_request_map = await self.get_completed_request_map(
            subject_unique_id=subject_unique_id, chunked_request_map=chunked_request_map
        )
        for chunk_bounds, bundle in unassessed:
            answer = await get_chunk_record_grounding_answer(
                stage_label="in-vocab grounding",
                subject_unique_id=subject_unique_id,
                field_name=self.field_type.name,
                chunk_bounds=chunk_bounds,
                catalog=self.catalog(),
                group_req_ids=bundle.llm_phrase_initial_grounding_req_ids,
                completed_request_map=completed_request_map,
                timestamp=timestamp,
                allowed_labels=list(self.match_label_to_concept_map.keys()),
            )
            missing = answer.missing_ids
            bundle.llm_phrase_initial_grounding_retry_record_ids = missing
            if not missing:
                continue
            payloads = await self._chunk_record_payloads(
                subject_unique_id=subject_unique_id,
                chunk_bounds=chunk_bounds,
                extraction_bundle=bundle,
                synthesis_map=synthesis_map,
                subject_text=subject_text,
                metadata=metadata,
                timestamp=timestamp,
            )
            retry_payloads = retry_record_payloads(
                "in-vocab grounding",
                subject_unique_id,
                self.field_type.name,
                chunk_bounds,
                payloads,
                missing,
            )
            bundle.llm_phrase_initial_grounding_retry_req_ids = [
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
                f"[{subject_unique_id}] in-vocab grounding: {len(missing)} of "
                f"{len(answer.sent_ids)} record(s) in chunk {chunk_bounds} "
                f"({self.field_type.name}) came back unanswered; embedding "
                f"{len(bundle.llm_phrase_initial_grounding_retry_req_ids)} retry request(s)."
            )

    def get_embedded_request_ids(
        self,
        subject_unique_id: str,
        chunked_request_map: ConceptExtractionRequestMap,
    ) -> set[BatchRequestIDType]:
        llm_phrase_initial_grounding_req_ids: set[BatchRequestIDType] = set()
        for (
            chunk_bounds,
            extraction_bundle,
        ) in chunked_request_map.items():
            if not extraction_bundle.llm_phrase_initial_grounding_req_ids:
                raise ValueError(
                    f"Cannot get embedded request ids for subject_unique_id:{subject_unique_id}>{chunk_bounds} as "
                    f"llm_phrase_initial_grounding_req_ids is empty in the extraction_bundle."
                )

            llm_phrase_initial_grounding_req_ids.update(
                extraction_bundle.llm_phrase_initial_grounding_req_ids
            )
            llm_phrase_initial_grounding_req_ids.update(
                extraction_bundle.llm_phrase_initial_grounding_retry_req_ids
            )

        return llm_phrase_initial_grounding_req_ids

    @staticmethod
    def get_request_custom_id(
        subject_unique_id: str,
        field_type: ExtractionFieldType,
        chunk_bounds: str,
        group_index: int,
        metadata: ConceptExtractionMetadata,
        group_payload: dict[str, dict[str, Any]],
        retry_index: int | None = None,
    ) -> BatchRequestIDType:
        # `|ud=` (fork F12): the group's own record payloads are part of request
        # identity, so a re-run after an upstream change re-asks rather than
        # replaying answers built from records that no longer exist — since 3.3
        # those are the synthesis stage's group records (D16). A retry request
        # carries `>retry>{n}>` before its group index.
        retry = f"retry>{retry_index}>" if retry_index is not None else ""
        return (
            f"{subject_unique_id}>{field_type.name}"
            f">{STAGE_REQUEST_ID_TOKEN[PipelineStage.initial_grounding]}"
            f">{retry}group>{group_index}>chunk>{chunk_bounds}>"
            f"{metadata.llm_phrase_initial_grounding.to_custom_id_segment()}"
            f"{upstream_digest_segment(group_payload)}"
        )

    async def create_batch_requests(
        self,
        subject_unique_id: str,
        scraped_text_file: ScrapedTextFile,
        missing_request_ids: set[BatchRequestIDType],
        metadata: ConceptExtractionMetadata,
        chunked_request_map: ConceptExtractionRequestMap,
        pipeline_context: PipelineContext,
        timestamp: datetime,
        eager: bool,
    ) -> list[GPTBatchRequest]:
        synthesis_map = self.get_upstream_synthesis_map(pipeline_context)
        chunk_payload_maps: dict[str, dict[str, dict[str, Any]]] = {}
        group_req_ids_by_chunk: dict[str, list[BatchRequestIDType]] = {}
        retry_req_ids_by_chunk: dict[str, list[BatchRequestIDType]] = {}
        retry_record_ids_by_chunk: dict[str, list[str]] = {}
        options_section_by_chunk: dict[str, Optional[str]] = {}
        options_section = self.options_section()
        for chunk_bounds, extraction_bundle in chunked_request_map.items():
            group_req_ids_by_chunk[chunk_bounds] = (
                extraction_bundle.llm_phrase_initial_grounding_req_ids
            )
            retry_req_ids_by_chunk[chunk_bounds] = (
                extraction_bundle.llm_phrase_initial_grounding_retry_req_ids
            )
            retry_record_ids_by_chunk[chunk_bounds] = (
                extraction_bundle.llm_phrase_initial_grounding_retry_record_ids or []
            )
            if not (
                (
                    set(extraction_bundle.llm_phrase_initial_grounding_req_ids)
                    | set(extraction_bundle.llm_phrase_initial_grounding_retry_req_ids)
                )
                & missing_request_ids
            ):
                continue
            chunk_payload_maps[chunk_bounds] = await self._chunk_record_payloads(
                subject_unique_id=subject_unique_id,
                chunk_bounds=chunk_bounds,
                extraction_bundle=extraction_bundle,
                synthesis_map=synthesis_map,
                subject_text=scraped_text_file.text,
                metadata=metadata,
                timestamp=timestamp,
            )
            options_section_by_chunk[chunk_bounds] = options_section

        return await create_missing_record_grounding_requests(
            stage_label="in-vocab grounding",
            subject_unique_id=subject_unique_id,
            field_name=self.field_type.name,
            chunk_payload_maps=chunk_payload_maps,
            group_req_ids_by_chunk=group_req_ids_by_chunk,
            retry_req_ids_by_chunk=retry_req_ids_by_chunk,
            retry_record_ids_by_chunk=retry_record_ids_by_chunk,
            missing_req_ids=missing_request_ids,
            prompt=self.phrase_initial_grounding_prompt,
            catalog=self.catalog(),
            options_section_by_chunk=options_section_by_chunk,
            max_records_per_request=metadata.llm_phrase_initial_grounding.max_pairs_per_request,
            deferred_at=timestamp,
            llm_model=metadata.llm_phrase_initial_grounding.llm_model,
            model_params=metadata.llm_phrase_initial_grounding.model_params,
            eager=eager,
            dummy_note=(
                "No in-vocab grounding needed - no record carries any evidence "
                "for this chunk."
            ),
        )

    @staticmethod
    async def get_result(
        subject_unique_id: str,
        field_type: ConceptFieldType,
        chunk_bounds: str,
        extraction_bundle: ConceptExtractionRequestBundle,
        completed_request_map: dict[BatchRequestIDType, GPTBatchRequest],
        timestamp: datetime,  # for recording errors
    ) -> RecordGroundingResults:
        """The chunk's grounding entries, WITHOUT the vocabulary hold — this
        static signature serves generic readers (the partial dump), which have
        no ontology in hand. Every pipeline consumer re-parses through the
        service with ``allowed_labels`` supplied; ``validate_own_responses``
        below is where the vocabulary hold pays at this node's own expense."""
        return await get_record_grounding_result(
            stage_label="in-vocab grounding",
            subject_unique_id=subject_unique_id,
            field_name=field_type.name,
            chunk_bounds=chunk_bounds,
            catalog=get_rule_catalog(STAGE_INITIAL_GROUNDING, field_type.name),
            group_req_ids=extraction_bundle.llm_phrase_initial_grounding_req_ids,
            retry_req_ids=extraction_bundle.llm_phrase_initial_grounding_retry_req_ids,
            completed_request_map=completed_request_map,
            timestamp=timestamp,
        )

    async def validate_own_responses(
        self,
        subject_unique_id: str,
        chunked_request_map: ConceptExtractionRequestMap,
        completed_request_map: dict[BatchRequestIDType, GPTBatchRequest],
        timestamp: datetime,
    ) -> None:
        """Parse every response WITH the vocabulary hold (fork F4), here where
        the requests were made: a non-vocab option fails this node's own
        request rather than surfacing one stage later against a consumer."""
        for chunk_bounds, extraction_bundle in chunked_request_map.items():
            await get_record_grounding_result(
                stage_label="in-vocab grounding",
                subject_unique_id=subject_unique_id,
                field_name=self.field_type.name,
                chunk_bounds=chunk_bounds,
                catalog=self.catalog(),
                group_req_ids=extraction_bundle.llm_phrase_initial_grounding_req_ids,
                retry_req_ids=extraction_bundle.llm_phrase_initial_grounding_retry_req_ids,
                completed_request_map=completed_request_map,
                timestamp=timestamp,
                allowed_labels=list(self.match_label_to_concept_map.keys()),
            )

    async def dispatch_batch_request(
        self,
        gpt_batch_request: GPTBatchRequest,
        metadata: ConceptExtractionMetadata,
    ) -> GPTBatchResponse:
        return await dispatch_gpt_batch_request(
            gpt_batch_request=gpt_batch_request,
            gpt_model=metadata.llm_phrase_initial_grounding.llm_model,
        )
