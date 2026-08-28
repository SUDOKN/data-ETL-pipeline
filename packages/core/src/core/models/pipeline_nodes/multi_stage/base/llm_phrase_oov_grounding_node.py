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
from core.models.rule_catalog import (
    STAGE_INITIAL_GROUNDING,
    STAGE_OOV_GROUNDING,
    RuleCatalog,
)
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


class LLMPhraseOovGroundingNode(
    BaseLLMRecursiveExtractionNode[ConceptFieldType, RecordGroundingResults]
):
    """The out-of-vocabulary discovery pass, serial after in-vocab grounding.
    Since 3.3 its records are the synthesis stage's per-group records (D16),
    and it is a recursive node for the under-answer retry (the synthesis
    stage's two-pass policy, ported by user decision 2026-08-24).

    Each record rides with what the in-vocab pass already identified from it
    (fork F5's arm 1: full vocabulary + pass-1 results in context), and the
    model names what BOTH miss. Candidates are minted; one that restates a
    vocabulary label is folded onto the canonical spelling before screening
    (``candidates_for_screening``) or re-routed at reconcile — never raised.

    Whether the pass runs at all is RUN CONFIG carried as metadata identity:
    ``metadata.llm_phrase_oov_grounding is None`` means off, and this node
    embeds zero requests, publishes an empty completed map, and hands the chain
    straight to screening. Never a StageToggle — those are hard-stop.
    """

    stage: ClassVar[PipelineStage] = PipelineStage.oov_grounding

    def __init__(
        self,
        field_type: ConceptFieldType,
        next_node: BaseLLMExtractionNode | ReconcileNode,
        phrase_oov_grounding_prompt: Optional[Prompt],
        known_concepts: set[Concept],
    ):
        super().__init__(
            field_type=field_type,
            next_node=next_node,
        )
        self.phrase_oov_grounding_prompt = phrase_oov_grounding_prompt
        self.known_concepts = known_concepts
        self.match_label_to_concept_map = get_match_label_to_concept_map(known_concepts)

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
                f"Cannot embed req ids for oov grounding: "
                f"PipelineContext.subject_text is None for subject:{subject_unique_id}, "
                f"field:{field}. The orchestrator must set it (the node recomputes the "
                f"synthesis stage's fold from the text when it derives its records)."
            )
        return subject_text

    def get_upstream_in_vocab_grounding_map(
        self, pipeline_context: PipelineContext
    ) -> dict[BatchRequestIDType, GPTBatchRequest]:
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement get_upstream_in_vocab_grounding_map"
        )

    def catalog(self) -> RuleCatalog:
        return get_rule_catalog(STAGE_OOV_GROUNDING, self.field_type.name)

    def options_section(self) -> str:
        return (
            f"the vocabulary that earlier identification matched against:\n"
            f"{render_concept_outline(self.known_concepts)}"
        )

    async def _chunk_record_payloads(
        self,
        subject_unique_id: str,
        chunk_bounds: str,
        extraction_bundle: ConceptExtractionRequestBundle,
        mention_map: dict[BatchRequestIDType, GPTBatchRequest],
        synthesis_map: dict[BatchRequestIDType, GPTBatchRequest],
        upstream_in_vocab_map: dict[BatchRequestIDType, GPTBatchRequest],
        subject_text: str,
        metadata: ConceptExtractionMetadata,
        timestamp: datetime,
    ) -> dict[str, dict[str, Any]]:
        """Every synthesized group record (3.3, D16), each carrying the labels
        the in-vocab pass already identified from it (empty list when it
        declined)."""
        group_records = await get_chunk_group_records(
            subject_unique_id,
            self.field_type,
            chunk_bounds,
            extraction_bundle,
            timestamp,
            synthesis_completed_request_map=synthesis_map,
            mention_completed_request_map=mention_map,
            subject_text=subject_text,
            metadata=metadata,
        )
        in_vocab_results = await get_record_grounding_result(
            stage_label="in-vocab grounding",
            subject_unique_id=subject_unique_id,
            field_name=self.field_type.name,
            chunk_bounds=chunk_bounds,
            catalog=get_rule_catalog(STAGE_INITIAL_GROUNDING, self.field_type.name),
            group_req_ids=extraction_bundle.llm_phrase_initial_grounding_req_ids,
            retry_req_ids=extraction_bundle.llm_phrase_initial_grounding_retry_req_ids,
            completed_request_map=upstream_in_vocab_map,
            timestamp=timestamp,
            allowed_labels=list(self.match_label_to_concept_map.keys()),
        )
        return build_group_record_payloads(
            group_records,
            already_identified={
                group_id: sorted(entry.tags)
                for group_id, entry in in_vocab_results.items()
            },
        )

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
                f"Cannot embed req ids for llm oov grounding node, "
                f"as chunked_request_map found empty for subject:{subject_unique_id}, field:{self.field_type.name}."
            )

        oov_metadata = metadata.llm_phrase_oov_grounding
        if oov_metadata is None:
            # The pass is off for this run: zero groups everywhere, and the
            # node completes with an empty request map.
            return

        max_pairs_per_request = oov_metadata.max_pairs_per_request
        mention_map = self.get_upstream_mention_collection_map(pipeline_context)
        synthesis_map = self.get_upstream_synthesis_map(pipeline_context)
        upstream_in_vocab_map = self.get_upstream_in_vocab_grounding_map(
            pipeline_context
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
            if extraction_request_bundle.llm_phrase_oov_grounding_req_ids:
                continue  # already embedded; group count is stable once computed

            payloads = await self._chunk_record_payloads(
                subject_unique_id=subject_unique_id,
                chunk_bounds=chunk_bounds,
                extraction_bundle=extraction_request_bundle,
                mention_map=mention_map,
                synthesis_map=synthesis_map,
                upstream_in_vocab_map=upstream_in_vocab_map,
                subject_text=subject_text,
                metadata=metadata,
                timestamp=timestamp,
            )
            embedded_groups = True
            extraction_request_bundle.llm_phrase_oov_grounding_req_ids = [
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
            if bundle.llm_phrase_oov_grounding_retry_record_ids is None
        ]
        if not unassessed:
            return
        if not await self.are_all_requests_complete(
            subject_unique_id=subject_unique_id, chunked_request_map=chunked_request_map
        ):
            logger.info(
                f"[{subject_unique_id}] Waiting for the oov grounding requests to "
                f"complete before assessing chunks for a retry ({self.field_type.name})."
            )
            return
        completed_request_map = await self.get_completed_request_map(
            subject_unique_id=subject_unique_id, chunked_request_map=chunked_request_map
        )
        for chunk_bounds, bundle in unassessed:
            answer = await get_chunk_record_grounding_answer(
                stage_label="oov grounding",
                subject_unique_id=subject_unique_id,
                field_name=self.field_type.name,
                chunk_bounds=chunk_bounds,
                catalog=self.catalog(),
                group_req_ids=bundle.llm_phrase_oov_grounding_req_ids,
                completed_request_map=completed_request_map,
                timestamp=timestamp,
            )
            missing = answer.missing_ids
            bundle.llm_phrase_oov_grounding_retry_record_ids = missing
            if not missing:
                continue
            payloads = await self._chunk_record_payloads(
                subject_unique_id=subject_unique_id,
                chunk_bounds=chunk_bounds,
                extraction_bundle=bundle,
                mention_map=mention_map,
                synthesis_map=synthesis_map,
                upstream_in_vocab_map=upstream_in_vocab_map,
                subject_text=subject_text,
                metadata=metadata,
                timestamp=timestamp,
            )
            retry_payloads = retry_record_payloads(
                "oov grounding",
                subject_unique_id,
                self.field_type.name,
                chunk_bounds,
                payloads,
                missing,
            )
            bundle.llm_phrase_oov_grounding_retry_req_ids = [
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
                f"[{subject_unique_id}] oov grounding: {len(missing)} of "
                f"{len(answer.sent_ids)} record(s) in chunk {chunk_bounds} "
                f"({self.field_type.name}) came back unanswered; embedding "
                f"{len(bundle.llm_phrase_oov_grounding_retry_req_ids)} retry request(s)."
            )

    def get_embedded_request_ids(
        self,
        subject_unique_id: str,
        chunked_request_map: ConceptExtractionRequestMap,
    ) -> set[BatchRequestIDType]:
        # Empty lists are legitimate here, unlike every other stage: they are
        # what an OFF pass embeds, and the machinery treats zero ids as a
        # complete stage with an empty result.
        req_ids: set[BatchRequestIDType] = set()
        for extraction_bundle in chunked_request_map.values():
            req_ids.update(extraction_bundle.llm_phrase_oov_grounding_req_ids)
            req_ids.update(extraction_bundle.llm_phrase_oov_grounding_retry_req_ids)
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
        oov_metadata = metadata.llm_phrase_oov_grounding
        if oov_metadata is None:
            raise ValueError(
                "get_request_custom_id was called for the OOV pass while its "
                "metadata node is None; an OFF pass embeds no ids at all."
            )
        # `|ud=` (fork F12): the payload carries the records AND each one's
        # already-identified pass-1 results, so an in-vocab change re-asks the
        # discovery question instead of replaying it — since 3.3 the records
        # are the synthesis stage's group records (D16). A retry request
        # carries `>retry>{n}>` before its group index.
        retry = f"retry>{retry_index}>" if retry_index is not None else ""
        return (
            f"{subject_unique_id}>{field_type.name}"
            f">{STAGE_REQUEST_ID_TOKEN[PipelineStage.oov_grounding]}"
            f">{retry}group>{group_index}>chunk>{chunk_bounds}>"
            f"{oov_metadata.to_custom_id_segment()}"
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
        oov_metadata = metadata.llm_phrase_oov_grounding
        if oov_metadata is None:
            raise ValueError(
                "create_batch_requests was called for the OOV pass while its "
                "metadata node is None; an OFF pass has no missing requests."
            )
        if self.phrase_oov_grounding_prompt is None:
            raise ValueError(
                f"{self.field_type.name}: the OOV pass is on (metadata carries "
                f"its node) but the node was built without a prompt."
            )

        mention_map = self.get_upstream_mention_collection_map(pipeline_context)
        synthesis_map = self.get_upstream_synthesis_map(pipeline_context)
        upstream_in_vocab_map = self.get_upstream_in_vocab_grounding_map(
            pipeline_context
        )
        chunk_payload_maps: dict[str, dict[str, dict[str, Any]]] = {}
        group_req_ids_by_chunk: dict[str, list[BatchRequestIDType]] = {}
        retry_req_ids_by_chunk: dict[str, list[BatchRequestIDType]] = {}
        retry_record_ids_by_chunk: dict[str, list[str]] = {}
        options_section_by_chunk: dict[str, Optional[str]] = {}
        options_section = self.options_section()
        for chunk_bounds, extraction_bundle in chunked_request_map.items():
            group_req_ids_by_chunk[chunk_bounds] = (
                extraction_bundle.llm_phrase_oov_grounding_req_ids
            )
            retry_req_ids_by_chunk[chunk_bounds] = (
                extraction_bundle.llm_phrase_oov_grounding_retry_req_ids
            )
            retry_record_ids_by_chunk[chunk_bounds] = (
                extraction_bundle.llm_phrase_oov_grounding_retry_record_ids or []
            )
            if not (
                (
                    set(extraction_bundle.llm_phrase_oov_grounding_req_ids)
                    | set(extraction_bundle.llm_phrase_oov_grounding_retry_req_ids)
                )
                & missing_request_ids
            ):
                continue
            chunk_payload_maps[chunk_bounds] = await self._chunk_record_payloads(
                subject_unique_id=subject_unique_id,
                chunk_bounds=chunk_bounds,
                extraction_bundle=extraction_bundle,
                mention_map=mention_map,
                synthesis_map=synthesis_map,
                upstream_in_vocab_map=upstream_in_vocab_map,
                subject_text=scraped_text_file.text,
                metadata=metadata,
                timestamp=timestamp,
            )
            options_section_by_chunk[chunk_bounds] = options_section

        return await create_missing_record_grounding_requests(
            stage_label="oov grounding",
            subject_unique_id=subject_unique_id,
            field_name=self.field_type.name,
            chunk_payload_maps=chunk_payload_maps,
            group_req_ids_by_chunk=group_req_ids_by_chunk,
            retry_req_ids_by_chunk=retry_req_ids_by_chunk,
            retry_record_ids_by_chunk=retry_record_ids_by_chunk,
            missing_req_ids=missing_request_ids,
            prompt=self.phrase_oov_grounding_prompt,
            catalog=self.catalog(),
            options_section_by_chunk=options_section_by_chunk,
            max_records_per_request=oov_metadata.max_pairs_per_request,
            deferred_at=timestamp,
            llm_model=oov_metadata.llm_model,
            model_params=oov_metadata.model_params,
            eager=eager,
            dummy_note=(
                "No oov grounding needed - no record carries any evidence for "
                "this chunk."
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
        """Minted labels pass through as written; an empty map when the pass is
        off for this run — the caller cannot tell "off" from "found nothing
        anywhere" here, which is why the stats' oov block distinguishes them
        via the metadata node."""
        if not extraction_bundle.llm_phrase_oov_grounding_req_ids:
            return {}
        return await get_record_grounding_result(
            stage_label="oov grounding",
            subject_unique_id=subject_unique_id,
            field_name=field_type.name,
            chunk_bounds=chunk_bounds,
            catalog=get_rule_catalog(STAGE_OOV_GROUNDING, field_type.name),
            group_req_ids=extraction_bundle.llm_phrase_oov_grounding_req_ids,
            retry_req_ids=extraction_bundle.llm_phrase_oov_grounding_retry_req_ids,
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
        metadata: ConceptExtractionMetadata,
    ) -> GPTBatchResponse:
        oov_metadata = metadata.llm_phrase_oov_grounding
        if oov_metadata is None:
            raise ValueError(
                "dispatch_batch_request was called for the OOV pass while its "
                "metadata node is None; an OFF pass dispatches nothing."
            )
        return await dispatch_gpt_batch_request(
            gpt_batch_request=gpt_batch_request,
            gpt_model=oov_metadata.llm_model,
        )
