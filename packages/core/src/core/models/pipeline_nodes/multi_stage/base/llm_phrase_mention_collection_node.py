"""The v3 mention-collection node (PIPELINE_V3_PLAN.md Phase 3.1, amended
2026-08-22): code collects each sub-window's mentions, one LLM request per
(chunk, sub-window, mention group) asks for their LOCATION, and the aggregation
fold is computed at ``get_result`` time, per chunk, from the completed map and
the subject text. See the node service for the contracts.

TWO PASSES (user decision 2026-08-22, the under-answer policy). The node is a
recursive node so ``embed_request_ids`` runs until it adds nothing: pass 1
embeds every sub-window's group requests (chunk-wide, occurrence-filtered
forms); once those are complete, pass 2 ASSESSES each window — the ids its
answers left undescribed are stored, and a window with any gets ONE retry
request set for just those items; a third entry finds nothing to add. Eager runs
loop in-process (``BaseLLMRecursiveExtractionNode.execute``); batch runs take
one pass per invocation, like the recursive search.
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
from core.models.extraction_results.llm_phrase_extraction_results_v2 import (
    LLMPhraseExtractionMetadataV2,
)
from core.models.extraction_schemas.mention_collection import MentionWireItem
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
    collect_sub_window,
    create_missing_mention_collection_requests,
    forms_occurring_in_window,
    get_chunk_fold,
    get_chunk_forms,
    get_window_locations,
    group_digest_payload,
    require_mention_metadata,
    retry_items_of_window,
    split_into_item_groups,
    stored_window_forms,
)
from core.utils.aggregation_fold import FoldResult
from core.utils.request_custom_id_util import upstream_digest_segment

logger = logging.getLogger(__name__)


class LLMPhraseMentionCollectionNode(
    BaseLLMRecursiveExtractionNode[LLMExtractedFieldTypeVar, FoldResult]
):
    stage: ClassVar[PipelineStage] = PipelineStage.mention_collection

    def __init__(
        self,
        field_type: LLMExtractedFieldTypeVar,
        next_node: BaseLLMExtractionNode | ReconcileNode,
        phrase_mention_collection_prompt: Prompt,
    ):
        super().__init__(field_type=field_type, next_node=next_node)
        self.phrase_mention_collection_prompt = phrase_mention_collection_prompt

    @abstractmethod
    def get_upstream_phrase_search_map(
        self, pipeline_context: PipelineContext
    ) -> dict[BatchRequestIDType, GPTBatchRequest]:
        """The completed first-search request map from the pipeline context."""
        raise NotImplementedError

    @abstractmethod
    def get_upstream_recursive_search_map(
        self, pipeline_context: PipelineContext
    ) -> dict[BatchRequestIDType, GPTBatchRequest]:
        """The completed recursive-search request map (empty when that pass is off)."""
        raise NotImplementedError

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
                f"Cannot embed req ids for mention collection: chunked_request_map is "
                f"empty for subject:{subject_unique_id}, field:{self.field_type.name}."
            )
        mention_metadata = require_mention_metadata(metadata)
        search_map = self.get_upstream_phrase_search_map(pipeline_context)
        recursive_map = self.get_upstream_recursive_search_map(pipeline_context)
        # The mentions are collected HERE, from the text: a window's group count
        # is the count of its distinct snippets over the group cap, and that is
        # known only by scanning. The orchestrator puts the text on the context
        # (``PipelineContext.subject_text``) for exactly this.
        subject_text = pipeline_context.subject_text
        if subject_text is None:
            raise ValueError(
                f"Cannot embed req ids for mention collection: PipelineContext.subject_text "
                f"is None for subject:{subject_unique_id}, field:{self.field_type.name}. "
                f"The orchestrator must set it (the node collects mentions from the text "
                f"when it mints its request ids)."
            )

        # PASS 1 — the group requests. Both search passes are complete by now (a
        # node only reaches its successor once its own requests are), so the
        # chunk's pooled form list is final and each window's collection + group
        # count can be computed once, upfront.
        embedded_groups = False
        for chunk_bounds, bundle in chunked_request_map.items():
            if not bundle.search_sub_bounds:
                raise ValueError(
                    f"Cannot embed req ids for mention collection: search_sub_bounds is "
                    f"empty for {subject_unique_id}>{chunk_bounds}, field:{self.field_type.name}."
                )
            pending = [
                sub_bounds
                for sub_bounds in bundle.search_sub_bounds
                if sub_bounds not in bundle.llm_phrase_mention_req_ids
            ]
            if not pending:
                continue  # already embedded; forms stored and group count stable
            chunk_forms = await get_chunk_forms(
                subject_unique_id=subject_unique_id,
                field_type=self.field_type,
                chunk_bounds=chunk_bounds,
                extraction_bundle=bundle,
                llm_phrase_search_gpt_request_map=search_map,
                llm_phrase_recursive_search_gpt_request_map=recursive_map,
                timestamp=timestamp,
            )
            for sub_bounds in pending:
                forms = forms_occurring_in_window(subject_text, sub_bounds, chunk_forms)
                bundle.llm_phrase_mention_sent_forms[sub_bounds] = forms
                collection = collect_sub_window(
                    subject_text, sub_bounds, forms, snippet_radius=mention_metadata.snippet_radius
                )
                groups = split_into_item_groups(
                    collection.items, mention_metadata.max_mentions_per_request
                )
                bundle.llm_phrase_mention_req_ids[sub_bounds] = [
                    self.get_request_custom_id(
                        subject_unique_id=subject_unique_id,
                        field_type=self.field_type,
                        chunk_bounds=chunk_bounds,
                        sub_bounds=sub_bounds,
                        group_index=group_index,
                        metadata=metadata,
                        group_items=group,
                    )
                    for group_index, group in enumerate(groups)
                ]
                embedded_groups = True
        if embedded_groups:
            return  # the groups must complete before any window can be assessed

        # PASS 2 — assessment + the retry (see the module docstring). A window is
        # assessed once, when every embedded request of the field is complete.
        unassessed = [
            (chunk_bounds, bundle, sub_bounds)
            for chunk_bounds, bundle in chunked_request_map.items()
            for sub_bounds in bundle.search_sub_bounds
            if sub_bounds not in bundle.llm_phrase_mention_retry_mention_ids
        ]
        if not unassessed:
            return
        if not await self.are_all_requests_complete(
            subject_unique_id=subject_unique_id, chunked_request_map=chunked_request_map
        ):
            logger.info(
                f"[{subject_unique_id}] Waiting for the mention-location requests to "
                f"complete before assessing windows for a retry ({self.field_type.name})."
            )
            return
        completed_request_map = await self.get_completed_request_map(
            subject_unique_id=subject_unique_id, chunked_request_map=chunked_request_map
        )
        for chunk_bounds, bundle, sub_bounds in unassessed:
            answer = await get_window_locations(
                subject_unique_id=subject_unique_id,
                field_type=self.field_type,
                chunk_bounds=chunk_bounds,
                sub_bounds=sub_bounds,
                extraction_bundle=bundle,
                completed_request_map=completed_request_map,
                timestamp=timestamp,
                include_retry=False,
            )
            missing = answer.missing_ids
            bundle.llm_phrase_mention_retry_mention_ids[sub_bounds] = missing
            if not missing:
                continue
            collection = collect_sub_window(
                subject_text,
                sub_bounds,
                stored_window_forms(
                    subject_unique_id, self.field_type, chunk_bounds, sub_bounds, bundle
                ),
                snippet_radius=mention_metadata.snippet_radius,
            )
            retry_groups = split_into_item_groups(
                retry_items_of_window(
                    subject_unique_id, self.field_type, sub_bounds, collection, missing
                ),
                mention_metadata.max_mentions_per_request,
            )
            bundle.llm_phrase_mention_retry_req_ids[sub_bounds] = [
                self.get_request_custom_id(
                    subject_unique_id=subject_unique_id,
                    field_type=self.field_type,
                    chunk_bounds=chunk_bounds,
                    sub_bounds=sub_bounds,
                    group_index=group_index,
                    metadata=metadata,
                    group_items=group,
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
                f"[{subject_unique_id}] mention_collection: {len(missing)} of "
                f"{len(answer.sent_ids)} mention(s) in sub-window {sub_bounds} of chunk "
                f"{chunk_bounds} ({self.field_type.name}) came back undescribed{unknown_note}; "
                f"embedding {len(retry_groups)} retry request(s)."
            )

    def get_embedded_request_ids(
        self,
        subject_unique_id: str,
        chunked_request_map: LLMPhraseExtractionRequestMap,
    ) -> set[BatchRequestIDType]:
        request_ids: set[BatchRequestIDType] = set()
        for chunk_bounds, bundle in chunked_request_map.items():
            if not bundle.llm_phrase_mention_req_ids:
                raise ValueError(
                    f"get_embedded_request_ids was called for {subject_unique_id}:"
                    f"{self.field_type.name} but llm_phrase_mention_req_ids is empty for "
                    f"chunk bounds {chunk_bounds}."
                )
            for group_req_ids in bundle.llm_phrase_mention_req_ids.values():
                request_ids.update(group_req_ids)
            for retry_req_ids in bundle.llm_phrase_mention_retry_req_ids.values():
                request_ids.update(retry_req_ids)
        return request_ids

    @staticmethod
    def get_request_custom_id(
        subject_unique_id: str,
        field_type: ExtractionFieldType,
        chunk_bounds: str,
        sub_bounds: str,
        group_index: int,
        metadata: LLMPhraseExtractionMetadataV2,
        group_items: list[MentionWireItem],
        retry_index: int | None = None,
    ) -> BatchRequestIDType:
        # `|ud=` digests the group's own items (ids + snippets) into its
        # identity: change what the window's text or forms yield and the id
        # changes, so a stale response is never found (the same F12 discipline
        # every downstream stage follows). A retry request carries
        # `>retry>{n}>` before its group index; a first-pass id is unchanged.
        retry = f"retry>{retry_index}>" if retry_index is not None else ""
        return (
            f"{subject_unique_id}>{field_type.name}"
            f">{STAGE_REQUEST_ID_TOKEN[PipelineStage.mention_collection]}"
            f">chunk>{chunk_bounds}>sub>{sub_bounds}>{retry}group>{group_index}>"
            f"{require_mention_metadata(metadata).to_custom_id_segment()}"
            f"{upstream_digest_segment(group_digest_payload(group_items))}"
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
        mention_metadata = require_mention_metadata(metadata)
        return await create_missing_mention_collection_requests(
            subject_unique_id=subject_unique_id,
            field_type=self.field_type,
            chunked_request_map=chunked_request_map,
            missing_request_ids=missing_request_ids,
            subject_text=scraped_text_file.text,
            phrase_mention_collection_prompt=self.phrase_mention_collection_prompt,
            timestamp=timestamp,
            llm_model=mention_metadata.llm_model,
            model_params=mention_metadata.model_params,
            max_mentions_per_request=mention_metadata.max_mentions_per_request,
            eager=eager,
            snippet_radius=mention_metadata.snippet_radius,
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
        snippet_radius: int = 0,
        collapse_compounds: bool = False,
    ) -> FoldResult:
        """The chunk's aggregation fold (needs the text: mentions are collected
        from it, and windows are located in it; ``snippet_radius`` = the
        stage's own, off its metadata; ``collapse_compounds`` = D21's dial)."""
        return await get_chunk_fold(
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
        """Parse + hold every window's groups now, so a malformed answer fails
        against this node's request (pure JSON over an in-memory map: free)."""
        for chunk_bounds, bundle in chunked_request_map.items():
            for sub_bounds in bundle.llm_phrase_mention_req_ids:
                await get_window_locations(
                    subject_unique_id=subject_unique_id,
                    field_type=self.field_type,
                    chunk_bounds=chunk_bounds,
                    sub_bounds=sub_bounds,
                    extraction_bundle=bundle,
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
            gpt_model=require_mention_metadata(metadata).llm_model,
        )
