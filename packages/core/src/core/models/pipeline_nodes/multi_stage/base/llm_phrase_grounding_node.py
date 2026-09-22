"""The Step 2 grounding stage: ONE call per record group replaces the in-vocab
pass and the out-of-vocabulary pass (design draft §3, D5; record-major wire by
user decision 2026-09-21).

What differs from the retired ``LLMPhraseInitialGroundingNode`` (cutover
2026-09-22), which it replaced and was otherwise modelled on:

- the ``phrase_grounding`` catalog (structural reporting): every option a
  record matches carries the record's own quote and its matching branch,
  proposals sit beside the options, and a record that yields nothing carries
  its reason — parsed by ``parse_record_grounding_structural_result`` with the
  vocabulary hold at this node's own expense;
- the record block names each record's phrase ``subject`` (the prompts' word);
- the vocabulary is the dash-line outline with full definitions, and it sits
  at the END OF THE SYSTEM TEXT — instructions, vocabulary, then the nonce and
  the records in the user message (the outline tryout of 2026-09-21: the
  position that won on stability and judged quality, and the one the
  provider's prefix cache can serve);
- the under-answer retry, the group cap, the ``|ud=`` digests and the result
  contract are the initial node's, unchanged.

The stored map holds vocabulary matches and proposals together; the reconcile
step splits them with ``split_vocabulary_and_proposals``.

The class is also the base of the PROPOSAL PASS (``LLMPhraseProposalNode``,
substep 3): the same request layout, retry and parse over a different record
set, catalog, metadata slot and bundle slots — the hooks below are what the
subclass overrides.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, ClassVar, Optional

from llm_providers.db_models.gpt_batch_request import GPTBatchRequest
from llm_providers.field_types import BatchRequestIDType
from llm_providers.models.file_objects.prompt import Prompt
from llm_providers.models.open_ai.gpt_batch_response_blob import GPTBatchResponse
from llm_providers.services.gpt_batch_request.gpt_batch_request_service import (
    dispatch_gpt_batch_request,
)
from scraper.models.s3.scraped_text_file import ScrapedTextFile

from core.models.deferred_extraction.deferred_concept_extraction import (
    ConceptExtractionRequestBundle,
    ConceptExtractionRequestMap,
)
from core.models.extraction_results.concept_extraction_results import (
    BatchedInitialGroundingNodeMetadata,
)
from core.models.extraction_results.llm_phrase_extraction_results import (
    ConceptExtractionMetadata,
)
from core.models.extraction_schemas.grounding import RecordGroundingResults
from core.models.field_types import ConceptFieldType, ExtractionFieldType
from core.models.pipeline_nodes.base.base_llm_extraction_node import (
    BaseLLMExtractionNode,
)
from core.models.pipeline_nodes.base.base_llm_recursive_extraction_node import (
    BaseLLMRecursiveExtractionNode,
)
from core.models.pipeline_nodes.base.base_node import PipelineContext
from core.models.pipeline_nodes.base.base_reconcile_node import ReconcileNode
from core.models.pipeline_nodes.base.pipeline_stage import (
    STAGE_REQUEST_ID_TOKEN,
    PipelineStage,
)
from core.models.rule_catalog import STAGE_GROUNDING, RuleCatalog
from core.models.skos_concept import Concept
from core.services.pipeline_nodes.multi_stage.llm_grounding_node_service import (
    VOCABULARY_HEADING,
    build_group_record_payloads,
    create_missing_record_grounding_requests,
    get_chunk_record_grounding_answer,
    get_record_grounding_result,
    grouped_record_payloads,
    retry_record_payloads,
    subject_keyed_payloads,
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

logger = logging.getLogger(__name__)


def grounding_metadata_of(
    metadata: ConceptExtractionMetadata,
) -> BatchedInitialGroundingNodeMetadata:
    """The stage's run identity, which the factory sets for a Step 2 run; a
    chain that reaches this node without it is a configuration error."""
    if metadata.llm_phrase_grounding is None:
        raise ValueError(
            "the grounding node needs metadata.llm_phrase_grounding (the Step 2 "
            "one-call stage's identity); the chain was built without it"
        )
    return metadata.llm_phrase_grounding


class LLMPhraseGroundingNode(
    BaseLLMRecursiveExtractionNode[ConceptFieldType, RecordGroundingResults]
):
    stage: ClassVar[PipelineStage] = PipelineStage.grounding
    # --- the hooks a sibling stage overrides (the proposal pass) ------------
    STAGE_LABEL: ClassVar[str] = "grounding"
    CATALOG_STAGE: ClassVar[str] = STAGE_GROUNDING
    DUMMY_NOTE: ClassVar[str] = (
        "No grounding needed - no record carries any evidence for this chunk."
    )
    # An OFF pass (run flag) embeds no ids; only such a stage may read empty
    # id lists as "complete with an empty result".
    EMPTY_IDS_ARE_A_COMPLETE_STAGE: ClassVar[bool] = False

    def __init__(
        self,
        field_type: ConceptFieldType,
        next_node: BaseLLMExtractionNode | ReconcileNode,
        phrase_grounding_prompt: Prompt,
        known_concepts: set[Concept],
    ):
        super().__init__(field_type=field_type, next_node=next_node)
        self.prompt = phrase_grounding_prompt
        self.known_concepts = known_concepts
        self.match_label_to_concept_map = get_match_label_to_concept_map(known_concepts)

    # --- the stage's run identity and its bundle slots ---------------------

    @classmethod
    def stage_metadata(
        cls, metadata: ConceptExtractionMetadata
    ) -> Optional[BatchedInitialGroundingNodeMetadata]:
        """The stage's metadata node; None means the pass is OFF for this run
        (only a stage with ``EMPTY_IDS_ARE_A_COMPLETE_STAGE`` may return None)."""
        return grounding_metadata_of(metadata)

    @classmethod
    def required_stage_metadata(
        cls, metadata: ConceptExtractionMetadata, doing: str
    ) -> BatchedInitialGroundingNodeMetadata:
        node = cls.stage_metadata(metadata)
        if node is None:
            raise ValueError(
                f"{doing} was called for the {cls.STAGE_LABEL} pass while its "
                f"metadata node is None; an OFF pass embeds no ids at all."
            )
        return node

    @staticmethod
    def _req_ids(bundle: ConceptExtractionRequestBundle) -> list[BatchRequestIDType]:
        return bundle.llm_phrase_grounding_req_ids

    @staticmethod
    def _set_req_ids(bundle: ConceptExtractionRequestBundle, ids: list[BatchRequestIDType]) -> None:
        bundle.llm_phrase_grounding_req_ids = ids

    @staticmethod
    def _retry_record_ids(bundle: ConceptExtractionRequestBundle) -> Optional[list[str]]:
        return bundle.llm_phrase_grounding_retry_record_ids

    @staticmethod
    def _set_retry_record_ids(bundle: ConceptExtractionRequestBundle, ids: list[str]) -> None:
        bundle.llm_phrase_grounding_retry_record_ids = ids

    @staticmethod
    def _retry_req_ids(bundle: ConceptExtractionRequestBundle) -> list[BatchRequestIDType]:
        return bundle.llm_phrase_grounding_retry_req_ids

    @staticmethod
    def _set_retry_req_ids(bundle: ConceptExtractionRequestBundle, ids: list[BatchRequestIDType]) -> None:
        bundle.llm_phrase_grounding_retry_req_ids = ids

    def get_upstream_synthesis_map(
        self, pipeline_context: PipelineContext
    ) -> dict[BatchRequestIDType, GPTBatchRequest]:
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
                f"Cannot embed req ids for grounding: PipelineContext.subject_text "
                f"is None for subject:{subject_unique_id}, field:{field}. The "
                f"orchestrator must set it (the node recomputes the synthesis "
                f"stage's fold from the text when it derives its records)."
            )
        return subject_text

    def catalog(self) -> RuleCatalog:
        return get_rule_catalog(self.CATALOG_STAGE, self.field_type.name)

    def allowed_labels(self) -> list[str]:
        return list(self.match_label_to_concept_map.keys())

    def options_section(self) -> str:
        """The vocabulary block a request carries at the end of its SYSTEM
        text: the heading, then the dash-line outline with every label's
        other names and full definition."""
        return (
            f"{VOCABULARY_HEADING}\n"
            f"{render_concept_outline(self.known_concepts, with_definitions=True)}"
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
        """The chunk's group_id → payload map, every synthesized group, with the
        phrase under the ``subject`` key the prompt names. Both id-embedding
        (group count + digests) and request creation derive from this
        identically, so the digest is over the payload as sent."""
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
        return subject_keyed_payloads(build_group_record_payloads(group_records))

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
                f"Cannot embed req ids for the {self.STAGE_LABEL} node, as chunked_request_map "
                f"found empty for subject:{subject_unique_id}, field:{self.field_type.name}."
            )
        stage_metadata = self.stage_metadata(metadata)
        if stage_metadata is None:
            return  # the pass is OFF for this run: zero groups everywhere
        max_records_per_request = stage_metadata.max_pairs_per_request
        synthesis_map = self.get_upstream_synthesis_map(pipeline_context)
        subject_text = self._subject_text_of(
            pipeline_context, subject_unique_id, self.field_type.name
        )

        # PASS 1 — the group requests.
        embedded_groups = False
        for chunk_bounds, bundle in chunked_request_map.items():
            if self._req_ids(bundle):
                continue  # already embedded; group count is stable once computed
            payloads = await self._chunk_record_payloads(
                subject_unique_id=subject_unique_id,
                chunk_bounds=chunk_bounds,
                extraction_bundle=bundle,
                synthesis_map=synthesis_map,
                subject_text=subject_text,
                metadata=metadata,
                timestamp=timestamp,
            )
            embedded_groups = True
            self._set_req_ids(bundle, [
                self.get_request_custom_id(
                    subject_unique_id=subject_unique_id,
                    field_type=self.field_type,
                    chunk_bounds=chunk_bounds,
                    group_index=group_index,
                    metadata=metadata,
                    group_payload=payload_group,
                )
                for group_index, payload_group in enumerate(
                    grouped_record_payloads(payloads, max_records_per_request)
                )
            ])
        if embedded_groups:
            return  # the groups must complete before any chunk can be assessed

        # PASS 2 — assessment + the retry: a chunk is assessed once, when every
        # embedded request of the field is complete.
        unassessed = [
            (chunk_bounds, bundle)
            for chunk_bounds, bundle in chunked_request_map.items()
            if self._retry_record_ids(bundle) is None
        ]
        if not unassessed:
            return
        if not await self.are_all_requests_complete(
            subject_unique_id=subject_unique_id, chunked_request_map=chunked_request_map
        ):
            logger.info(
                f"[{subject_unique_id}] Waiting for the {self.STAGE_LABEL} requests to complete "
                f"before assessing chunks for a retry ({self.field_type.name})."
            )
            return
        completed_request_map = await self.get_completed_request_map(
            subject_unique_id=subject_unique_id, chunked_request_map=chunked_request_map
        )
        for chunk_bounds, bundle in unassessed:
            answer = await get_chunk_record_grounding_answer(
                stage_label=self.STAGE_LABEL,
                subject_unique_id=subject_unique_id,
                field_name=self.field_type.name,
                chunk_bounds=chunk_bounds,
                catalog=self.catalog(),
                group_req_ids=self._req_ids(bundle),
                completed_request_map=completed_request_map,
                timestamp=timestamp,
                allowed_labels=self.allowed_labels(),
                structural=True,
            )
            missing = answer.missing_ids
            self._set_retry_record_ids(bundle, missing)
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
                self.STAGE_LABEL,
                subject_unique_id,
                self.field_type.name,
                chunk_bounds,
                payloads,
                missing,
            )
            self._set_retry_req_ids(bundle, [
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
                    grouped_record_payloads(retry_payloads, max_records_per_request)
                )
            ])
            logger.info(
                f"[{subject_unique_id}] {self.STAGE_LABEL}: {len(missing)} of {len(answer.sent_ids)} "
                f"record(s) in chunk {chunk_bounds} ({self.field_type.name}) came back "
                f"unanswered; embedding {len(self._retry_req_ids(bundle))} "
                f"retry request(s)."
            )

    def get_embedded_request_ids(
        self,
        subject_unique_id: str,
        chunked_request_map: ConceptExtractionRequestMap,
    ) -> set[BatchRequestIDType]:
        req_ids: set[BatchRequestIDType] = set()
        for chunk_bounds, bundle in chunked_request_map.items():
            if not self._req_ids(bundle) and not self.EMPTY_IDS_ARE_A_COMPLETE_STAGE:
                raise ValueError(
                    f"Cannot get embedded request ids for subject_unique_id:"
                    f"{subject_unique_id}>{chunk_bounds} as the {self.STAGE_LABEL} "
                    f"request id list is empty in the extraction_bundle."
                )
            req_ids.update(self._req_ids(bundle))
            req_ids.update(self._retry_req_ids(bundle))
        return req_ids

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
        # `|ud=`: the group's own record payloads (as sent, subject-keyed) are
        # part of request identity; a retry carries `>retry>{n}>` before its
        # group index — the initial node's contract, unchanged.
        retry = f"retry>{retry_index}>" if retry_index is not None else ""
        return (
            f"{subject_unique_id}>{field_type.name}"
            f">{STAGE_REQUEST_ID_TOKEN[PipelineStage.grounding]}"
            f">{retry}group>{group_index}>chunk>{chunk_bounds}>"
            f"{grounding_metadata_of(metadata).to_custom_id_segment()}"
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
        stage_metadata = self.required_stage_metadata(metadata, "create_batch_requests")
        synthesis_map = self.get_upstream_synthesis_map(pipeline_context)
        chunk_payload_maps: dict[str, dict[str, dict[str, Any]]] = {}
        group_req_ids_by_chunk: dict[str, list[BatchRequestIDType]] = {}
        retry_req_ids_by_chunk: dict[str, list[BatchRequestIDType]] = {}
        retry_record_ids_by_chunk: dict[str, list[str]] = {}
        options_section_by_chunk: dict[str, Optional[str]] = {}
        options_section = self.options_section()
        for chunk_bounds, bundle in chunked_request_map.items():
            group_req_ids_by_chunk[chunk_bounds] = self._req_ids(bundle)
            retry_req_ids_by_chunk[chunk_bounds] = self._retry_req_ids(bundle)
            retry_record_ids_by_chunk[chunk_bounds] = self._retry_record_ids(bundle) or []
            if not (set(self._req_ids(bundle)) | set(self._retry_req_ids(bundle))) & missing_request_ids:
                continue
            chunk_payload_maps[chunk_bounds] = await self._chunk_record_payloads(
                subject_unique_id=subject_unique_id,
                chunk_bounds=chunk_bounds,
                extraction_bundle=bundle,
                synthesis_map=synthesis_map,
                subject_text=scraped_text_file.text,
                metadata=metadata,
                timestamp=timestamp,
            )
            options_section_by_chunk[chunk_bounds] = options_section

        return await create_missing_record_grounding_requests(
            stage_label=self.STAGE_LABEL,
            subject_unique_id=subject_unique_id,
            field_name=self.field_type.name,
            chunk_payload_maps=chunk_payload_maps,
            group_req_ids_by_chunk=group_req_ids_by_chunk,
            retry_req_ids_by_chunk=retry_req_ids_by_chunk,
            retry_record_ids_by_chunk=retry_record_ids_by_chunk,
            missing_req_ids=missing_request_ids,
            prompt=self.prompt,
            catalog=self.catalog(),
            options_section_by_chunk=options_section_by_chunk,
            max_records_per_request=stage_metadata.max_pairs_per_request,
            deferred_at=timestamp,
            llm_model=stage_metadata.llm_model,
            model_params=stage_metadata.model_params,
            eager=eager,
            dummy_note=self.DUMMY_NOTE,
            options_in_system=True,
        )

    @classmethod
    async def get_result(
        cls,
        subject_unique_id: str,
        field_type: ConceptFieldType,
        chunk_bounds: str,
        extraction_bundle: ConceptExtractionRequestBundle,
        completed_request_map: dict[BatchRequestIDType, GPTBatchRequest],
        timestamp: datetime,
        allowed_labels: Optional[list[str]] = None,
    ) -> RecordGroundingResults:
        """The chunk's grounding entries — vocabulary matches and proposals in
        one per-record map. Without ``allowed_labels`` (a generic reader such
        as the partial dump, which has no ontology in hand) no membership hold
        is applied; every pipeline consumer passes the vocabulary. An OFF pass
        (no ids embedded) reads as an empty map."""
        if not cls._req_ids(extraction_bundle) and cls.EMPTY_IDS_ARE_A_COMPLETE_STAGE:
            return {}
        return await get_record_grounding_result(
            stage_label=cls.STAGE_LABEL,
            subject_unique_id=subject_unique_id,
            field_name=field_type.name,
            chunk_bounds=chunk_bounds,
            catalog=get_rule_catalog(cls.CATALOG_STAGE, field_type.name),
            group_req_ids=cls._req_ids(extraction_bundle),
            retry_req_ids=cls._retry_req_ids(extraction_bundle),
            completed_request_map=completed_request_map,
            timestamp=timestamp,
            allowed_labels=allowed_labels,
            structural=True,
        )

    async def validate_own_responses(
        self,
        subject_unique_id: str,
        chunked_request_map: ConceptExtractionRequestMap,
        completed_request_map: dict[BatchRequestIDType, GPTBatchRequest],
        timestamp: datetime,
    ) -> None:
        """Parse every response WITH the vocabulary hold, here where the
        requests were made, so a bad answer fails this node's own request."""
        for chunk_bounds, bundle in chunked_request_map.items():
            await self.get_result(
                subject_unique_id=subject_unique_id,
                field_type=self.field_type,
                chunk_bounds=chunk_bounds,
                extraction_bundle=bundle,
                completed_request_map=completed_request_map,
                timestamp=timestamp,
                allowed_labels=self.allowed_labels(),
            )

    async def dispatch_batch_request(
        self,
        gpt_batch_request: GPTBatchRequest,
        metadata: ConceptExtractionMetadata,
    ) -> GPTBatchResponse:
        return await dispatch_gpt_batch_request(
            gpt_batch_request=gpt_batch_request,
            gpt_model=self.required_stage_metadata(metadata, "dispatch_batch_request").llm_model,
        )
