"""The Step 2 descent stage: screening and descent in DEPTH WAVES, then one
proposal wave (design draft §6.3 / §6.5; user decisions 2026-09-20 → 22).

Per chunk, one wave per vocabulary depth k = 1 … max depth:

1. the wave's UNITS — the vocabulary labels grounding (the one call, and the
   proposal pass when on) matched directly at depth k, plus the labels the
   wave before reached by descent — deduplicated by (label, record), minus
   every pair whose ancestor FAILED on that record (V13), are screened in one
   batched round through the unit-screening layer (wave k of
   ``llm_phrase_unit_screening_req_ids``);
2. every accepted label with children gets one DESCENT request over its
   accepted records (children as the dash-line outline in the system text);
   an accepted leaf gets the LEAF STEP (no children listed) when the run
   flag is on; a false child the model names is recorded and the parent
   stands; a proposal is collected;
3. the next wave waits for those.

After the last depth wave, EVERY proposal — the grounding call's, the
proposal pass's, descent's sibling proposals, the leaf steps' — is screened
together in one batch (wave 0), so no proposal ships unvetted and none is
screened per level (user decision 2026-09-22). The trail of all of it is the
stage's result (``DescentTrail``); the reconcile step applies the deepest-
accepted rule per record (``deepest_accepted_labels``) and ships accepted
proposals as out-of-vocabulary results.

Two request kinds live under this one node — the wave screens (unit
screening's catalog, prompt and metadata) and the descents (this stage's) —
told apart by the stage token in their ids.
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
from core.models.extraction_results.extraction_node_metadata import (
    BatchedScreeningNodeMetadata,
    DescentNodeMetadata,
)
from core.models.extraction_results.llm_phrase_extraction_results import (
    ConceptExtractionMetadata,
)
from core.models.extraction_schemas.descent import (
    PROPOSAL_WAVE,
    DescentRequest,
    DescentTrail,
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
from core.models.pipeline_nodes.multi_stage.base.llm_phrase_grounding_node import (
    LLMPhraseGroundingNode,
)
from core.models.pipeline_nodes.multi_stage.base.llm_phrase_proposal_node import (
    LLMPhraseProposalNode,
)
from core.models.pipeline_nodes.multi_stage.base.llm_phrase_unit_screening_node import (
    LLMPhraseUnitScreeningNode,
)
from core.models.skos_concept import Concept
from core.services.pipeline_nodes.multi_stage.llm_descent_node_service import (
    DESCENT_LABEL,
    ChunkInputs,
    Vocabulary,
    accepted_by_label,
    chunk_inputs_from,
    create_deferred_descent_gpt_request,
    descent_request_payload,
    failed_by_record,
    reached_and_answers_from,
    read_descent_trail,
    wave_payloads,
)
from core.services.pipeline_nodes.multi_stage.llm_phrase_synthesis_node_service import (
    get_chunk_group_records,
)
from core.services.pipeline_nodes.multi_stage.llm_relationship_screening_node_service import (
    UnitRequestPayload,
    create_missing_unit_screening_requests,
    get_unit_screening_result,
    unit_screening_catalog_for,
)
from core.services.pipeline_nodes.multi_stage.stage_derivations import wave_group
from core.utils.request_custom_id_util import upstream_digest_segment

logger = logging.getLogger(__name__)

_DESCENT_TOKEN = f">{STAGE_REQUEST_ID_TOKEN[PipelineStage.descent]}>"


def descent_metadata_of(metadata: ConceptExtractionMetadata) -> DescentNodeMetadata:
    if metadata.llm_phrase_descent is None:
        raise ValueError("the descent node needs metadata.llm_phrase_descent; the chain was built without it")
    return metadata.llm_phrase_descent


def screening_metadata_of(metadata: ConceptExtractionMetadata) -> BatchedScreeningNodeMetadata:
    if metadata.llm_phrase_unit_screening is None:
        raise ValueError("the descent node needs metadata.llm_phrase_unit_screening (its wave screens); the chain was built without it")
    return metadata.llm_phrase_unit_screening


# The service's ``ChunkInputs`` under the name the walkthrough test imports.
_ChunkInputs = ChunkInputs


class LLMPhraseDescentNode(
    BaseLLMRecursiveExtractionNode[ConceptFieldType, DescentTrail]
):
    stage: ClassVar[PipelineStage] = PipelineStage.descent

    def __init__(
        self,
        field_type: ConceptFieldType,
        next_node: BaseLLMExtractionNode | ReconcileNode,
        phrase_descent_prompt: Prompt,
        phrase_unit_screening_prompt: Prompt,
        known_concepts: set[Concept],
    ):
        super().__init__(field_type=field_type, next_node=next_node)
        self.descent_prompt = phrase_descent_prompt
        self.screening_prompt = phrase_unit_screening_prompt
        self.known_concepts = known_concepts
        self.vocab = Vocabulary(known_concepts)

    # The vocabulary view, exposed under the names the walk and its test use.
    @property
    def children_of(self) -> dict[str, list[Concept]]:
        return self.vocab.children_of

    @property
    def concept_by_name(self) -> dict[str, Concept]:
        return self.vocab.concept_by_name

    @property
    def vocabulary_names(self) -> dict[str, str]:
        return self.vocab.vocabulary_names

    @property
    def max_depth(self) -> int:
        return self.vocab.max_depth

    # --- what the subclasses supply -----------------------------------------

    def get_upstream_synthesis_map(self, pipeline_context: PipelineContext) -> dict[BatchRequestIDType, GPTBatchRequest]:
        raise NotImplementedError(f"{self.__class__.__name__} must implement get_upstream_synthesis_map")

    def get_upstream_grounding_map(self, pipeline_context: PipelineContext) -> dict[BatchRequestIDType, GPTBatchRequest]:
        raise NotImplementedError(f"{self.__class__.__name__} must implement get_upstream_grounding_map")

    def get_upstream_proposal_map(self, pipeline_context: PipelineContext) -> dict[BatchRequestIDType, GPTBatchRequest]:
        raise NotImplementedError(f"{self.__class__.__name__} must implement get_upstream_proposal_map")

    # --- vocabulary helpers --------------------------------------------------

    def fold(self, label: str) -> str:
        return self.vocab.fold(label)

    def ancestors_of(self, label: str) -> list[str]:
        return self.vocab.ancestors_of(label)

    def meaning_of(self, label: str) -> Optional[str]:
        return self.vocab.meaning_of(label)

    def allowed_labels(self) -> list[str]:
        return self.vocab.allowed_labels()

    # --- the chunk's inputs ---------------------------------------------------

    async def _chunk_inputs(
        self,
        subject_unique_id: str,
        chunk_bounds: str,
        bundle: ConceptExtractionRequestBundle,
        pipeline_context: PipelineContext,
        subject_text: str,
        metadata: ConceptExtractionMetadata,
        timestamp: datetime,
    ) -> ChunkInputs:
        group_records = await get_chunk_group_records(
            subject_unique_id, self.field_type, chunk_bounds, bundle, timestamp,
            synthesis_completed_request_map=self.get_upstream_synthesis_map(pipeline_context),
            subject_text=subject_text, metadata=metadata,
        )
        sources: list[tuple[str, RecordGroundingResults]] = [
            ("grounding", await LLMPhraseGroundingNode.get_result(
                subject_unique_id=subject_unique_id, field_type=self.field_type, chunk_bounds=chunk_bounds,
                extraction_bundle=bundle, completed_request_map=self.get_upstream_grounding_map(pipeline_context),
                timestamp=timestamp, allowed_labels=self.allowed_labels(),
            ))
        ]
        if metadata.llm_phrase_proposal is not None:
            sources.append(("proposal_pass", await LLMPhraseProposalNode.get_result(
                subject_unique_id=subject_unique_id, field_type=self.field_type, chunk_bounds=chunk_bounds,
                extraction_bundle=bundle, completed_request_map=self.get_upstream_proposal_map(pipeline_context),
                timestamp=timestamp, allowed_labels=self.allowed_labels(),
            )))
        return chunk_inputs_from(self.vocab, group_records, sources)

    # --- ids -------------------------------------------------------------------

    @staticmethod
    def get_request_custom_id(
        subject_unique_id: str,
        field_type: ExtractionFieldType,
        chunk_bounds: str,
        wave: int,
        parent: str,
        leaf: bool,
        metadata: ConceptExtractionMetadata,
        group_payload: dict[str, Any],
    ) -> BatchRequestIDType:
        # `|ud=`: the parent's accepted records AND the children offered are
        # request identity — an upstream change re-asks the descent.
        return (
            f"{subject_unique_id}>{field_type.name}"
            f">{STAGE_REQUEST_ID_TOKEN[PipelineStage.descent]}"
            f">wave>{wave}>parent>{parent}>leaf>{int(leaf)}>chunk>{chunk_bounds}>"
            f"{descent_metadata_of(metadata).to_custom_id_segment()}"
            f"{upstream_digest_segment(group_payload)}"
        )

    def _screening_id(self, subject_unique_id: str, chunk_bounds: str, wave: int, group_index: int,
                      metadata: ConceptExtractionMetadata, payload: UnitRequestPayload) -> BatchRequestIDType:
        return LLMPhraseUnitScreeningNode.get_request_custom_id(
            subject_unique_id=subject_unique_id, field_type=self.field_type, chunk_bounds=chunk_bounds,
            group_index=group_index, metadata=metadata, group_payload=payload, wave=wave,
        )

    # --- per-wave derivations (identical at embed, create and result time) ----

    async def _reached_and_proposals_from(
        self,
        subject_unique_id: str,
        reqs: list[DescentRequest],
        completed: dict[BatchRequestIDType, GPTBatchRequest],
        timestamp: datetime,
    ):
        return await reached_and_answers_from(
            vocab=self.vocab, subject_unique_id=subject_unique_id, field_name=self.field_type.name,
            reqs=reqs, completed=completed, timestamp=timestamp,
        )

    _failed_by_record = staticmethod(failed_by_record)
    _accepted_by_label = staticmethod(accepted_by_label)

    def _wave_payloads(self, inputs: ChunkInputs, units: dict[str, list[str]], cap: int) -> list[UnitRequestPayload]:
        return wave_payloads(self.vocab, inputs, units, cap)

    # --- the walk ----------------------------------------------------------------

    async def embed_request_ids(  # prefill folded into this function
        self,
        subject_unique_id: str,
        pipeline_context: PipelineContext,
        metadata: ConceptExtractionMetadata,
        chunked_request_map: ConceptExtractionRequestMap,
        timestamp: datetime,
    ):
        if not chunked_request_map:
            raise ValueError(f"Cannot embed req ids for the descent node, as chunked_request_map found empty for subject:{subject_unique_id}, field:{self.field_type.name}.")
        descent_md = descent_metadata_of(metadata)
        cap = screening_metadata_of(metadata).max_pairs_per_request
        subject_text = pipeline_context.subject_text
        if subject_text is None:
            raise ValueError(f"Cannot embed req ids for descent: PipelineContext.subject_text is None for subject:{subject_unique_id}, field:{self.field_type.name}.")
        completed = await self.get_completed_request_map(
            subject_unique_id=subject_unique_id, chunked_request_map=chunked_request_map, all_requests_must_be_complete=False,
        )
        for chunk_bounds, bundle in chunked_request_map.items():
            await self._advance_chunk(
                subject_unique_id=subject_unique_id, chunk_bounds=chunk_bounds, bundle=bundle,
                pipeline_context=pipeline_context, subject_text=subject_text, metadata=metadata,
                timestamp=timestamp, completed=completed, cap=cap, leaf_step=descent_md.leaf_step,
            )

    async def _advance_chunk(
        self, *, subject_unique_id: str, chunk_bounds: str, bundle: ConceptExtractionRequestBundle,
        pipeline_context: PipelineContext, subject_text: str, metadata: ConceptExtractionMetadata,
        timestamp: datetime, completed: dict[BatchRequestIDType, GPTBatchRequest], cap: int, leaf_step: bool,
    ) -> None:
        """Move one chunk as far as its completed answers allow: embed the
        next wave's screens, or its descents, or the proposal wave."""
        inputs = await self._chunk_inputs(subject_unique_id, chunk_bounds, bundle, pipeline_context, subject_text, metadata, timestamp)
        failed: dict[str, set[str]] = {}
        reached: dict[str, set[str]] = {}
        screens = bundle.llm_phrase_unit_screening_req_ids
        descents = bundle.llm_phrase_descent_reqs
        for wave in range(1, self.max_depth + 1):
            if wave not in screens:
                units, _removed = wave_group(inputs.direct_at(wave), {k: sorted(v) for k, v in reached.items()}, failed, self.ancestors_of)
                payloads = self._wave_payloads(inputs, units, cap)
                screens[wave] = [self._screening_id(subject_unique_id, chunk_bounds, wave, g, metadata, p) for g, p in enumerate(payloads)]
                if screens[wave]:
                    return
            if any(i not in completed for i in screens[wave]):
                return
            verdicts = await get_unit_screening_result(
                subject_unique_id=subject_unique_id, field_name=self.field_type.name,
                catalog=unit_screening_catalog_for(self.field_type.name), req_ids=screens[wave],
                completed_request_map=completed, timestamp=timestamp,
            ) if screens[wave] else {}
            self._failed_by_record(verdicts, failed)
            if wave not in descents:
                reqs: list[DescentRequest] = []
                for parent, ids in self._accepted_by_label(verdicts).items():
                    children = self.children_of.get(parent, [])
                    leaf = not children
                    if leaf and not leaf_step:
                        continue
                    payload = descent_request_payload(inputs.group_records, ids, children)
                    reqs.append(DescentRequest(parent=parent, wave=wave, leaf=leaf, req_id=self.get_request_custom_id(
                        subject_unique_id=subject_unique_id, field_type=self.field_type, chunk_bounds=chunk_bounds,
                        wave=wave, parent=parent, leaf=leaf, metadata=metadata, group_payload=payload,
                    )))
                descents[wave] = reqs
                if reqs:
                    return
            if any(r.req_id not in completed for r in descents[wave]):
                return
            reached, answers = await self._reached_and_proposals_from(subject_unique_id, descents[wave], completed, timestamp)
            for parent, answer in answers.items():
                for label, by_record in answer.proposals.items():
                    for rid in by_record:
                        inputs.add_proposal(label, rid, f"{'leaf' if not self.children_of.get(parent) else 'descent'}:{parent}")
        # every depth wave is done: the proposal wave, once
        if PROPOSAL_WAVE not in screens:
            units = inputs.proposal_units()
            payloads = self._wave_payloads(inputs, units, cap)
            screens[PROPOSAL_WAVE] = [self._screening_id(subject_unique_id, chunk_bounds, PROPOSAL_WAVE, g, metadata, p) for g, p in enumerate(payloads)]

    def get_embedded_request_ids(
        self, subject_unique_id: str, chunked_request_map: ConceptExtractionRequestMap
    ) -> set[BatchRequestIDType]:
        ids: set[BatchRequestIDType] = set()
        for bundle in chunked_request_map.values():
            for wave_ids in bundle.llm_phrase_unit_screening_req_ids.values():
                ids.update(wave_ids)
            for reqs in bundle.llm_phrase_descent_reqs.values():
                ids.update(r.req_id for r in reqs)
        return ids

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
        """Re-derive the missing requests' payloads from the same walk the ids
        came from, then create them: wave screens through the unit-screening
        layer, descents through this stage's builder."""
        descent_md, screening_md = descent_metadata_of(metadata), screening_metadata_of(metadata)
        cap = screening_md.max_pairs_per_request
        subject_name = pipeline_context.subject_name
        if not subject_name:
            raise ValueError(f"Cannot create descent requests: PipelineContext.subject_name is empty for subject:{subject_unique_id}, field:{self.field_type.name}.")
        completed = await self.get_completed_request_map(
            subject_unique_id=subject_unique_id, chunked_request_map=chunked_request_map, all_requests_must_be_complete=False,
        )
        screen_payloads: dict[BatchRequestIDType, UnitRequestPayload] = {}
        descent_requests: list[GPTBatchRequest] = []
        for chunk_bounds, bundle in chunked_request_map.items():
            wanted = self.get_embedded_request_ids(subject_unique_id, {chunk_bounds: bundle}) & missing_request_ids
            if not wanted:
                continue
            inputs = await self._chunk_inputs(subject_unique_id, chunk_bounds, bundle, pipeline_context, scraped_text_file.text, metadata, timestamp)
            failed: dict[str, set[str]] = {}
            reached: dict[str, set[str]] = {}
            screens, descents = bundle.llm_phrase_unit_screening_req_ids, bundle.llm_phrase_descent_reqs
            for wave in range(1, self.max_depth + 1):
                if wave not in screens:
                    break
                units, _ = wave_group(inputs.direct_at(wave), {k: sorted(v) for k, v in reached.items()}, failed, self.ancestors_of)
                payloads = self._wave_payloads(inputs, units, cap)
                self._check_count(screens[wave], payloads, f"wave {wave} screens", chunk_bounds, subject_unique_id)
                screen_payloads.update(zip(screens[wave], payloads))
                if any(i not in completed for i in screens[wave]):
                    break
                verdicts = await get_unit_screening_result(
                    subject_unique_id=subject_unique_id, field_name=self.field_type.name,
                    catalog=unit_screening_catalog_for(self.field_type.name), req_ids=screens[wave],
                    completed_request_map=completed, timestamp=timestamp,
                ) if screens[wave] else {}
                self._failed_by_record(verdicts, failed)
                if wave not in descents:
                    break
                accepted = self._accepted_by_label(verdicts)
                for r in descents[wave]:
                    if r.req_id not in missing_request_ids:
                        continue
                    parent = self.concept_by_name[r.parent]
                    children = [] if r.leaf else self.children_of.get(r.parent, [])
                    payload = descent_request_payload(inputs.group_records, accepted.get(r.parent, []), children)
                    descent_requests.append(create_deferred_descent_gpt_request(
                        deferred_at=timestamp, subject_unique_id=subject_unique_id, field_type=self.field_type,
                        request_id=r.req_id, prompt=self.descent_prompt, parent=parent, children=children, payload=payload,
                        gpt_model=descent_md.llm_model, eager=eager, model_params=descent_md.model_params,
                    ))
                if any(r.req_id not in completed for r in descents[wave]):
                    break
                reached, answers = await self._reached_and_proposals_from(subject_unique_id, descents[wave], completed, timestamp)
                for parent, answer in answers.items():
                    for label, by_record in answer.proposals.items():
                        for rid in by_record:
                            inputs.add_proposal(label, rid, f"{'leaf' if not self.children_of.get(parent) else 'descent'}:{parent}")
            if PROPOSAL_WAVE in screens and set(screens[PROPOSAL_WAVE]) & missing_request_ids:
                units = inputs.proposal_units()
                payloads = self._wave_payloads(inputs, units, cap)
                self._check_count(screens[PROPOSAL_WAVE], payloads, "the proposal wave", chunk_bounds, subject_unique_id)
                screen_payloads.update(zip(screens[PROPOSAL_WAVE], payloads))
        missing_screens = {i for i in missing_request_ids if _DESCENT_TOKEN not in i}
        screens_created = await create_missing_unit_screening_requests(
            subject_unique_id=subject_unique_id, field_name=self.field_type.name,
            payloads_by_req_id=screen_payloads, missing_req_ids=missing_screens,
            prompt=self.screening_prompt, catalog=unit_screening_catalog_for(self.field_type.name),
            subject_name=subject_name, deferred_at=timestamp, llm_model=screening_md.llm_model,
            model_params=screening_md.model_params, eager=eager,
        ) if missing_screens else []
        return [*screens_created, *descent_requests]

    @staticmethod
    def _check_count(ids: list[BatchRequestIDType], payloads: list[Any], what: str, chunk_bounds: str, subject_unique_id: str) -> None:
        if len(ids) != len(payloads):
            raise ValueError(
                f"{DESCENT_LABEL}: embedded request count ({len(ids)}) for {what} does not match the computed "
                f"packing ({len(payloads)}) for chunk {chunk_bounds} in {subject_unique_id}; the upstream state "
                f"changed under the stored request ids (re-defer)."
            )

    # --- the result -------------------------------------------------------------

    async def get_result(  # type: ignore[override]
        self,
        subject_unique_id: str,
        field_type: ConceptFieldType,
        chunk_bounds: str,
        extraction_bundle: ConceptExtractionRequestBundle,
        completed_request_map: dict[BatchRequestIDType, GPTBatchRequest],
        timestamp: datetime,
        pipeline_context: PipelineContext,
        subject_text: str,
        metadata: ConceptExtractionMetadata,
    ) -> DescentTrail:
        """The chunk's whole descent trail, re-read from the completed answers
        (``read_descent_trail``: the reconcile step reads the same function)."""
        inputs = await self._chunk_inputs(subject_unique_id, chunk_bounds, extraction_bundle, pipeline_context, subject_text, metadata, timestamp)
        return await read_descent_trail(
            vocab=self.vocab, subject_unique_id=subject_unique_id, field_name=field_type.name, inputs=inputs,
            screens=extraction_bundle.llm_phrase_unit_screening_req_ids, descents=extraction_bundle.llm_phrase_descent_reqs,
            completed=completed_request_map, timestamp=timestamp,
        )

    async def validate_own_responses(
        self,
        subject_unique_id: str,
        chunked_request_map: ConceptExtractionRequestMap,
        completed_request_map: dict[BatchRequestIDType, GPTBatchRequest],
        timestamp: datetime,
    ) -> None:
        """Every answer is parsed during the walk itself (a bad one fails its
        request there); nothing further to hold here."""
        return None

    async def dispatch_batch_request(
        self, gpt_batch_request: GPTBatchRequest, metadata: ConceptExtractionMetadata
    ) -> GPTBatchResponse:
        is_descent = _DESCENT_TOKEN in gpt_batch_request.request.custom_id
        model = descent_metadata_of(metadata).llm_model if is_descent else screening_metadata_of(metadata).llm_model
        return await dispatch_gpt_batch_request(gpt_batch_request=gpt_batch_request, gpt_model=model)

