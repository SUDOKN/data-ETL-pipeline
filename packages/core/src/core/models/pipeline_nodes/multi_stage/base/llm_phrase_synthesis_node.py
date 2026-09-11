"""The v3 synthesis node (PIPELINE_V3_PLAN.md D15 as amended 2026-08-22, D16;
Phase 3.2; the location-stage merge, 2026-09-03): per chunk, one LLM
description per group, written from the aggregation fold's records — focal
form + snippets — with the chunk's text in the request, packed under a soft
snippet cap, groups never split. See the node service for the contracts.

Since the merge this is the first LLM stage after search: the node also does
what the retired mention-collection node's embed pass did — pool the subject's
sent forms from the completed search maps and store each sub-window's
occurrence-filtered list on the bundle (``llm_phrase_mention_sent_forms``, the
field keeping its historical name) — before minting its own ids, so the fold
stays a pure function of (text, stored forms, knobs).

PASSES (user decision 2026-08-22, the under-answer policy). The node is a
recursive node so ``embed_request_ids`` runs until it adds nothing: pass 1
stores the sent forms and embeds every chunk's group requests (the fold
recomputed from the text + stored forms); once those are complete, pass 2
ASSESSES each chunk — the record ids its answers left unsynthesized are
stored, PLUS (2026-09-02, Phase B of the search-recall roadmap; scoped
2026-09-10) the ids whose answer dropped a designation the record OWNS — one
inside its focal form or written directly beside it, the spans cut at the
chunk's sibling forms (``under_enumerated_record_ids`` over
``sibling_forms_by_record(fold)`` — the conservation check behind the statics'
focal-form designation paragraph), and a chunk with any gets ONE retry
request set for just those records; a third
entry finds nothing to add. For a record both passes answered the read path
keeps whichever names more of its own designations, the FIRST answer keeping
ties (``resolve_under_enumeration``). Eager runs loop in-process
(``BaseLLMRecursiveExtractionNode.execute`` — which, since 2026-09-10, keeps
looping while a pass embeds NEW ids, so the assessment pass runs even when
every group answer was already in Mongo); batch runs take one pass per
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
from core.services.pipeline_nodes.multi_stage.aggregation_fold_service import (
    fold_collapse_compounds_of,
    fold_snippet_radius_of,
    fold_verb_fold_of,
    forms_occurring_in_window,
    get_subject_forms,
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
    sibling_forms_by_record,
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
    def get_upstream_phrase_search_map(
        self, pipeline_context: PipelineContext
    ) -> dict[BatchRequestIDType, GPTBatchRequest]:
        """The completed first-search request map from the pipeline context —
        what the sent-forms pooling reads (the fold itself is pure code over
        the stored forms)."""
        raise NotImplementedError

    @abstractmethod
    def get_upstream_recursive_search_map(
        self, pipeline_context: PipelineContext
    ) -> dict[BatchRequestIDType, GPTBatchRequest]:
        """The completed recursive-search request map (empty when that pass is off)."""
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
        subject_text = self._subject_text_of(
            pipeline_context, subject_unique_id, self.field_type.name
        )
        verb_fold = fold_verb_fold_of(metadata)
        snippet_radius = fold_snippet_radius_of(metadata)
        collapse_compounds = fold_collapse_compounds_of(metadata)

        # PASS 0 — the sent forms (ported from the retired mention-collection
        # node, 2026-09-03). Both search passes are complete by now (a node
        # only reaches its successor once its own requests are), so the pooled
        # form list is final; each sub-window stores the pooled forms that
        # OCCUR in it, and every later step — the fold, request creation, the
        # result — reads the stored list, never the search maps. The pool is
        # SUBJECT-wide (2026-09-02: the measured cross-chunk gap).
        subject_forms: list[str] | None = None
        for chunk_bounds, bundle in chunked_request_map.items():
            if not bundle.search_sub_bounds:
                raise ValueError(
                    f"Cannot embed req ids for synthesis: search_sub_bounds is empty for "
                    f"{subject_unique_id}>{chunk_bounds}, field:{self.field_type.name}."
                )
            pending = [
                sub_bounds
                for sub_bounds in bundle.search_sub_bounds
                if sub_bounds not in bundle.llm_phrase_mention_sent_forms
            ]
            if not pending:
                continue  # already stored; the form pool is final once search completes
            if subject_forms is None:
                subject_forms = await get_subject_forms(
                    subject_unique_id=subject_unique_id,
                    field_type=self.field_type,
                    chunked_request_map=chunked_request_map,
                    llm_phrase_search_gpt_request_map=self.get_upstream_phrase_search_map(
                        pipeline_context
                    ),
                    llm_phrase_recursive_search_gpt_request_map=self.get_upstream_recursive_search_map(
                        pipeline_context
                    ),
                    timestamp=timestamp,
                )
            for sub_bounds in pending:
                bundle.llm_phrase_mention_sent_forms[sub_bounds] = forms_occurring_in_window(
                    subject_text, sub_bounds, subject_forms
                )

        def fold_of(chunk_bounds: str, bundle: LLMPhraseExtractionRequestBundle):
            return chunk_fold(
                subject_unique_id,
                self.field_type,
                chunk_bounds,
                bundle,
                subject_text=subject_text,
                verb_fold=verb_fold,
                snippet_radius=snippet_radius,
                collapse_compounds=collapse_compounds,
            )

        def records_of(chunk_bounds: str, bundle: LLMPhraseExtractionRequestBundle):
            return fold_of(chunk_bounds, bundle).synthesis_records()

        # PASS 1 — the group requests. The stored forms are final (pass 0), so
        # the chunk's fold is final and its records can be computed once,
        # upfront.
        embedded_groups = False
        for chunk_bounds, bundle in chunked_request_map.items():
            if bundle.llm_phrase_synthesis_req_ids:
                continue  # already embedded; the fold is a pure function of stored state
            groups = pack_records(
                records_of(chunk_bounds, bundle), synthesis_metadata.max_entries_per_request
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
            fold = fold_of(chunk_bounds, bundle)
            records = fold.synthesis_records()
            missing = answer.missing_ids
            # 2026-09-02 (Phase B): the retry also re-asks ANSWERED records
            # whose synthesis dropped a designation the record OWNS — inside
            # its focal form or written directly beside it, the spans cut at
            # the chunk's sibling forms (2026-09-10, D2/D16: the old
            # any-token demand was 82.8% other records' tokens). The read
            # path keeps the better of the two answers per record
            # (resolve_under_enumeration, first answer on ties), so a worse
            # retry can never regress a record. (2026-09-03 to 2026-09-05 this
            # also re-asked records whose per-snippet context quotes
            # miscounted; that emission is gone — location is the fold's.)
            under_enumerated = under_enumerated_record_ids(
                records, answer.syntheses, sibling_forms=sibling_forms_by_record(fold)
            )
            retry_ids = missing + [rid for rid in under_enumerated if rid not in missing]
            retry_ids = list(dict.fromkeys(retry_ids))
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
        # `|ud=` digests the group's own records (ids, focal forms, snippets as
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
            phrase_synthesis_prompt=self.phrase_synthesis_prompt,
            timestamp=timestamp,
            llm_model=synthesis_metadata.llm_model,
            model_params=synthesis_metadata.model_params,
            max_entries_per_request=synthesis_metadata.max_entries_per_request,
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
        subject_text: str,
        verb_fold: bool,
        snippet_radius: int,
        collapse_compounds: bool = False,
    ) -> ChunkSynthesisResult:
        """The chunk's fold, its records and the held syntheses (needs the
        text: the fold is recomputed from it and the bundle's stored forms;
        ``collapse_compounds`` = D21's dial, which decides the fold's groups
        and so the records the syntheses were keyed by)."""
        return await get_chunk_synthesis_result(
            subject_unique_id=subject_unique_id,
            field_type=field_type,
            chunk_bounds=chunk_bounds,
            extraction_bundle=extraction_bundle,
            completed_request_map=completed_request_map,
            timestamp=timestamp,
            subject_text=subject_text,
            verb_fold=verb_fold,
            snippet_radius=snippet_radius,
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
