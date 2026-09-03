"""The v3 synthesis node (PIPELINE_V3_PLAN.md D15 as amended 2026-08-22, D16;
Phase 3.2): per chunk, one LLM description per group, written from the
aggregation fold's records — focal form + entries — packed into requests under
a soft entry cap, groups never split. See the node service for the contracts.

TWO PASSES (user decision 2026-08-22, the under-answer policy — the Location
stage's, applied here). The node is a recursive node so ``embed_request_ids``
runs until it adds nothing: pass 1 embeds every chunk's group requests (the
fold recomputed from the text + the mention stage's completed answers); once
those are complete, pass 2 ASSESSES each chunk — the record ids its answers
left unsynthesized are stored, PLUS (2026-09-02, Phase B of the search-recall
roadmap) the ids whose answer dropped designation-shaped tokens their entries
carry (``under_enumerated_record_ids`` — the conservation check behind the
hardened preserve-specifics prompt sentence), and a chunk with any gets ONE
retry request set for just those records; a third entry finds nothing to add.
For an under-enumerated record both passes hold an answer and the read path
keeps whichever names more designations, the retry winning ties
(``resolve_under_enumeration``). Eager runs loop in-process
(``BaseLLMRecursiveExtractionNode.execute``); batch runs take one pass per
invocation.
"""

from __future__ import annotations

import logging
from abc import abstractmethod
from datetime import datetime
from typing import ClassVar

from llm_providers.db_models.gpt_batch_request import GPTBatchRequest
from llm_providers.field_types import BatchRequestIDType
from llm_providers.models.file_objects.prompt import Prompt
from llm_providers.models.open_ai.gpt_batch_response_blob import GPTBatchResponse
from llm_providers.services.gpt_batch_request.gpt_batch_request_service import (
    dispatch_gpt_batch_request,
)
from scraper.models.s3.scraped_text_file import ScrapedTextFile

from core.models.deferred_extraction.deferred_phrase_extraction_requests import (
    LLMPhraseExtractionRequestBundle,
    LLMPhraseExtractionRequestMap,
)
from core.models.extraction_results.llm_phrase_extraction_results import (
    LLMPhraseExtractionMetadata,
)
from core.models.extraction_schemas.synthesis import SynthesisRecordInput
from core.models.field_types import ExtractionFieldType
from core.models.pipeline_nodes.base.base_llm_extraction_node import (
    BaseLLMExtractionNode,
)
from core.models.pipeline_nodes.base.base_llm_recursive_extraction_node import (
    BaseLLMRecursiveExtractionNode,
)
from core.models.pipeline_nodes.base.base_node import (
    LLMExtractedFieldTypeVar,
    PipelineContext,
)
from core.models.pipeline_nodes.base.base_reconcile_node import ReconcileNode
from core.models.pipeline_nodes.base.pipeline_stage import (
    STAGE_REQUEST_ID_TOKEN,
    PipelineStage,
)
from core.services.pipeline_nodes.multi_stage.llm_phrase_mention_collection_node_service import (
    fold_collapse_compounds_of,
    fold_snippet_radius_of,
    fold_verb_fold_of,
)
from core.services.pipeline_nodes.multi_stage.llm_phrase_synthesis_node_service import (
    ChunkSynthesisResult,
    chunk_fold,
    create_missing_synthesis_requests,
    get_chunk_synthesis_result,
    get_chunk_syntheses,
    group_digest_payload,
    pack_records,
    require_synthesis_metadata,
    retry_records_of_chunk,
    under_enumerated_record_ids,
)
from core.utils.request_custom_id_util import upstream_digest_segment

logger = logging.getLogger(__name__)


class LLMPhraseSynthesisNode(
    BaseLLMRecursiveExtractionNode[LLMExtractedFieldTypeVar, ChunkSynthesisResult]
):
    stage: ClassVar[PipelineStage] = PipelineStage.synthesis

    def __init__(
        self,
        field_type: LLMExtractedFieldTypeVar,
        next_node: BaseLLMExtractionNode | ReconcileNode,
        phrase_synthesis_prompt: Prompt,
    ):
        super().__init__(field_type=field_type, next_node=next_node)
        self.phrase_synthesis_prompt = phrase_synthesis_prompt

    @abstractmethod
    def get_upstream_mention_collection_map(
        self, pipeline_context: PipelineContext
    ) -> dict[BatchRequestIDType, GPTBatchRequest]:
        """The completed mention-collection request map from the pipeline
        context — what the chunk's fold is computed from."""
        raise NotImplementedError

    @staticmethod
    def _subject_text_of(
        pipeline_context: PipelineContext, subject_unique_id: str, field: str
    ) -> str:
        subject_text = pipeline_context.subject_text
        if subject_text is None:
            raise ValueError(
                f"Cannot embed req ids for synthesis: PipelineContext.subject_text is None "
                f"for subject:{subject_unique_id}, field:{field}. The orchestrator must set "
                f"it (the node recomputes the fold from the text when it mints its ids)."
            )
        return subject_text

    @staticmethod
    def _subject_name_of(
        pipeline_context: PipelineContext, subject_unique_id: str, field: str
    ) -> str:
        subject_name = pipeline_context.subject_name
        if not subject_name:
            raise ValueError(
                f"Cannot create synthesis requests: PipelineContext.subject_name is empty "
                f"for subject:{subject_unique_id}, field:{field}. The orchestrator sets it; "
                f"the static asks the model to mask it."
            )
        return subject_name

    async def embed_request_ids(  # prefill folded into this function
        self,
        subject_unique_id: str,
        pipeline_context: PipelineContext,
        metadata: LLMPhraseExtractionMetadata,
        chunked_request_map: LLMPhraseExtractionRequestMap,
        timestamp: datetime,
    ):
        if not chunked_request_map:
            raise ValueError(
                f"Cannot embed req ids for synthesis: chunked_request_map is empty for "
                f"subject:{subject_unique_id}, field:{self.field_type.name}."
            )
        synthesis_metadata = require_synthesis_metadata(metadata)
        mention_map = self.get_upstream_mention_collection_map(pipeline_context)
        subject_text = self._subject_text_of(
            pipeline_context, subject_unique_id, self.field_type.name
        )
        verb_fold = fold_verb_fold_of(metadata)
        snippet_radius = fold_snippet_radius_of(metadata)
        collapse_compounds = fold_collapse_compounds_of(metadata)

        async def records_of(chunk_bounds: str, bundle: LLMPhraseExtractionRequestBundle):
            fold = await chunk_fold(
                subject_unique_id,
                self.field_type,
                chunk_bounds,
                bundle,
                mention_map,
                timestamp,
                subject_text=subject_text,
                verb_fold=verb_fold,
                snippet_radius=snippet_radius,
                collapse_compounds=collapse_compounds,
            )
            return fold.synthesis_records(include_location=synthesis_metadata.include_location)

        # PASS 1 — the group requests. The mention stage is complete by now (a
        # node only reaches its successor once its own requests are), so the
        # chunk's fold is final and its records can be computed once, upfront.
        embedded_groups = False
        for chunk_bounds, bundle in chunked_request_map.items():
            if bundle.llm_phrase_synthesis_req_ids:
                continue  # already embedded; the fold is a pure function of stored state
            groups = pack_records(
                await records_of(chunk_bounds, bundle), synthesis_metadata.max_entries_per_request
            )
            bundle.llm_phrase_synthesis_req_ids = [
                self.get_request_custom_id(
                    subject_unique_id=subject_unique_id,
                    field_type=self.field_type,
                    chunk_bounds=chunk_bounds,
                    group_index=group_index,
                    metadata=metadata,
                    group_records=group,
                )
                for group_index, group in enumerate(groups)
            ]
            embedded_groups = True
        if embedded_groups:
            return  # the groups must complete before any chunk can be assessed

        # PASS 2 — assessment + the retry (see the module docstring). A chunk is
        # assessed once, when every embedded request of the field is complete.
        unassessed = [
            (chunk_bounds, bundle)
            for chunk_bounds, bundle in chunked_request_map.items()
            if bundle.llm_phrase_synthesis_retry_record_ids is None
        ]
        if not unassessed:
            return
        if not await self.are_all_requests_complete(
            subject_unique_id=subject_unique_id, chunked_request_map=chunked_request_map
        ):
            logger.info(
                f"[{subject_unique_id}] Waiting for the synthesis requests to complete "
                f"before assessing chunks for a retry ({self.field_type.name})."
            )
            return
        completed_request_map = await self.get_completed_request_map(
            subject_unique_id=subject_unique_id, chunked_request_map=chunked_request_map
        )
        for chunk_bounds, bundle in unassessed:
            answer = await get_chunk_syntheses(
                subject_unique_id=subject_unique_id,
                field_type=self.field_type,
                chunk_bounds=chunk_bounds,
                extraction_bundle=bundle,
                completed_request_map=completed_request_map,
                timestamp=timestamp,
                include_retry=False,
            )
            records = await records_of(chunk_bounds, bundle)
            missing = answer.missing_ids
            # 2026-09-02 (Phase B): the retry also re-asks ANSWERED records
            # whose synthesis dropped designation-shaped tokens their entries
            # carry — the under-enumeration conservation check. The read path
            # keeps the better of the two answers per record
            # (resolve_under_enumeration), so a worse retry can never regress
            # a record.
            under_enumerated = under_enumerated_record_ids(records, answer.syntheses)
            retry_ids = missing + [rid for rid in under_enumerated if rid not in missing]
            bundle.llm_phrase_synthesis_retry_record_ids = retry_ids
            if not retry_ids:
                continue
            retry_groups = pack_records(
                retry_records_of_chunk(
                    subject_unique_id,
                    self.field_type,
                    chunk_bounds,
                    records,
                    retry_ids,
                ),
                synthesis_metadata.max_entries_per_request,
            )
            bundle.llm_phrase_synthesis_retry_req_ids = [
                self.get_request_custom_id(
                    subject_unique_id=subject_unique_id,
                    field_type=self.field_type,
                    chunk_bounds=chunk_bounds,
                    group_index=group_index,
                    metadata=metadata,
                    group_records=group,
                    retry_index=1,
                )
                for group_index, group in enumerate(retry_groups)
            ]
            unknown_note = (
                f" ({len(answer.unknown_answer_ids)} answered id(s) never sent)"
                if answer.unknown_answer_ids
                else ""
            )
            logger.info(
                f"[{subject_unique_id}] synthesis: chunk {chunk_bounds} "
                f"({self.field_type.name}): {len(missing)} of {len(answer.sent_ids)} "
                f"record(s) unsynthesized, {len(under_enumerated)} under-enumerated"
                f"{unknown_note}; embedding {len(retry_groups)} retry request(s)."
            )

    def get_embedded_request_ids(
        self,
        subject_unique_id: str,
        chunked_request_map: LLMPhraseExtractionRequestMap,
    ) -> set[BatchRequestIDType]:
        request_ids: set[BatchRequestIDType] = set()
        for chunk_bounds, bundle in chunked_request_map.items():
            if not bundle.llm_phrase_synthesis_req_ids:
                raise ValueError(
                    f"get_embedded_request_ids was called for {subject_unique_id}:"
                    f"{self.field_type.name} but llm_phrase_synthesis_req_ids is empty for "
                    f"chunk bounds {chunk_bounds}."
                )
            request_ids.update(bundle.llm_phrase_synthesis_req_ids)
            request_ids.update(bundle.llm_phrase_synthesis_retry_req_ids)
        return request_ids

    @staticmethod
    def get_request_custom_id(
        subject_unique_id: str,
        field_type: ExtractionFieldType,
        chunk_bounds: str,
        group_index: int,
        metadata: LLMPhraseExtractionMetadata,
        group_records: list[SynthesisRecordInput],
        retry_index: int | None = None,
    ) -> BatchRequestIDType:
        # `|ud=` digests the group's own records (ids, focal forms, entries as
        # the wire shows them) into its identity: change what the fold yields
        # and the id changes, so a stale response is never found (D16 — the
        # same F12 discipline every downstream stage follows). A retry request
        # carries `>retry>{n}>` before its group index; a first-pass id is
        # unchanged.
        retry = f"retry>{retry_index}>" if retry_index is not None else ""
        return (
            f"{subject_unique_id}>{field_type.name}"
            f">{STAGE_REQUEST_ID_TOKEN[PipelineStage.synthesis]}"
            f">chunk>{chunk_bounds}>{retry}group>{group_index}>"
            f"{require_synthesis_metadata(metadata).to_custom_id_segment()}"
            f"{upstream_digest_segment(group_digest_payload(group_records))}"
        )

    async def create_batch_requests(
        self,
        subject_unique_id: str,
        scraped_text_file: ScrapedTextFile,
        missing_request_ids: set[BatchRequestIDType],
        metadata: LLMPhraseExtractionMetadata,
        chunked_request_map: LLMPhraseExtractionRequestMap,
        pipeline_context: PipelineContext,
        timestamp: datetime,
        eager: bool,
    ) -> list[GPTBatchRequest]:
        synthesis_metadata = require_synthesis_metadata(metadata)
        return await create_missing_synthesis_requests(
            subject_unique_id=subject_unique_id,
            field_type=self.field_type,
            chunked_request_map=chunked_request_map,
            missing_request_ids=missing_request_ids,
            subject_text=scraped_text_file.text,
            subject_name=self._subject_name_of(
                pipeline_context, subject_unique_id, self.field_type.name
            ),
            mention_completed_request_map=self.get_upstream_mention_collection_map(
                pipeline_context
            ),
            phrase_synthesis_prompt=self.phrase_synthesis_prompt,
            timestamp=timestamp,
            llm_model=synthesis_metadata.llm_model,
            model_params=synthesis_metadata.model_params,
            max_entries_per_request=synthesis_metadata.max_entries_per_request,
            include_location=synthesis_metadata.include_location,
            verb_fold=fold_verb_fold_of(metadata),
            snippet_radius=fold_snippet_radius_of(metadata),
            collapse_compounds=fold_collapse_compounds_of(metadata),
            eager=eager,
        )

    @staticmethod
    async def get_result(
        subject_unique_id: str,
        field_type: ExtractionFieldType,
        chunk_bounds: str,
        extraction_bundle: LLMPhraseExtractionRequestBundle,
        completed_request_map: dict[BatchRequestIDType, GPTBatchRequest],
        timestamp: datetime,
        *,
        mention_completed_request_map: dict[BatchRequestIDType, GPTBatchRequest],
        subject_text: str,
        verb_fold: bool,
        snippet_radius: int,
        include_location: bool,
        collapse_compounds: bool = False,
    ) -> ChunkSynthesisResult:
        """The chunk's fold, its records and the held syntheses (needs the
        mention stage's completed map and the text: the fold is recomputed;
        ``collapse_compounds`` = D21's dial, which decides the fold's groups
        and so the records the syntheses were keyed by)."""
        return await get_chunk_synthesis_result(
            subject_unique_id=subject_unique_id,
            field_type=field_type,
            chunk_bounds=chunk_bounds,
            extraction_bundle=extraction_bundle,
            completed_request_map=completed_request_map,
            timestamp=timestamp,
            mention_completed_request_map=mention_completed_request_map,
            subject_text=subject_text,
            verb_fold=verb_fold,
            snippet_radius=snippet_radius,
            include_location=include_location,
            collapse_compounds=collapse_compounds,
        )

    async def validate_own_responses(
        self,
        subject_unique_id: str,
        chunked_request_map: LLMPhraseExtractionRequestMap,
        completed_request_map: dict[BatchRequestIDType, GPTBatchRequest],
        timestamp: datetime,
    ) -> None:
        """Parse + hold every chunk's groups now, so a malformed answer fails
        against this node's request (pure JSON over an in-memory map: free)."""
        for chunk_bounds, bundle in chunked_request_map.items():
            await get_chunk_syntheses(
                subject_unique_id=subject_unique_id,
                field_type=self.field_type,
                chunk_bounds=chunk_bounds,
                extraction_bundle=bundle,
                completed_request_map=completed_request_map,
                timestamp=timestamp,
            )

    async def dispatch_batch_request(
        self,
        gpt_batch_request: GPTBatchRequest,
        metadata: LLMPhraseExtractionMetadata,
    ) -> GPTBatchResponse:
        return await dispatch_gpt_batch_request(
            gpt_batch_request=gpt_batch_request,
            gpt_model=require_synthesis_metadata(metadata).llm_model,
        )
