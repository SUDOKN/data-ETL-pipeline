"""The v3 mention-collection node (PIPELINE_V3_PLAN.md Phase 3.1): the stage
that replaced relationship. One request per (chunk, sub-window, form group);
the aggregation fold is computed at ``get_result`` time, per chunk, from the
completed map and the subject text. See the node service for the contracts.
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
from core.models.field_types import ExtractionFieldType
from core.models.pipeline_nodes.base.base_llm_extraction_node import (
    BaseLLMExtractionNode,
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
    create_missing_mention_collection_requests,
    get_chunk_fold,
    get_window_forms,
    get_window_mentions,
    require_mention_metadata,
    split_into_form_groups,
)
from core.utils.aggregation_fold import FoldResult
from core.utils.request_custom_id_util import upstream_digest_segment

logger = logging.getLogger(__name__)


class LLMPhraseMentionCollectionNode(
    BaseLLMExtractionNode[LLMExtractedFieldTypeVar, FoldResult]
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

        # Both search passes are complete by now (a node only reaches its
        # successor once its own requests are), so a window's form list is final
        # and its group count can be computed once, upfront.
        for chunk_bounds, bundle in chunked_request_map.items():
            if not bundle.search_sub_bounds:
                raise ValueError(
                    f"Cannot embed req ids for mention collection: search_sub_bounds is "
                    f"empty for {subject_unique_id}>{chunk_bounds}, field:{self.field_type.name}."
                )
            for sub_bounds in bundle.search_sub_bounds:
                if sub_bounds in bundle.llm_phrase_mention_req_ids:
                    continue  # already embedded; group count is stable once computed
                forms = await get_window_forms(
                    subject_unique_id=subject_unique_id,
                    field_type=self.field_type,
                    chunk_bounds=chunk_bounds,
                    sub_bounds=sub_bounds,
                    extraction_bundle=bundle,
                    llm_phrase_search_gpt_request_map=search_map,
                    llm_phrase_recursive_search_gpt_request_map=recursive_map,
                    timestamp=timestamp,
                )
                groups = split_into_form_groups(forms, mention_metadata.max_forms_per_request)
                bundle.llm_phrase_mention_req_ids[sub_bounds] = [
                    self.get_request_custom_id(
                        subject_unique_id=subject_unique_id,
                        field_type=self.field_type,
                        chunk_bounds=chunk_bounds,
                        sub_bounds=sub_bounds,
                        group_index=group_index,
                        metadata=metadata,
                        group_forms=group,
                    )
                    for group_index, group in enumerate(groups)
                ]

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
        return request_ids

    @staticmethod
    def get_request_custom_id(
        subject_unique_id: str,
        field_type: ExtractionFieldType,
        chunk_bounds: str,
        sub_bounds: str,
        group_index: int,
        metadata: LLMPhraseExtractionMetadataV2,
        group_forms: list[str],
    ) -> BatchRequestIDType:
        # `|ud=` digests the group's own forms into its identity: change what
        # search feeds this window and the id changes, so a stale response is
        # never found (the same F12 discipline every downstream stage follows).
        return (
            f"{subject_unique_id}>{field_type.name}"
            f">{STAGE_REQUEST_ID_TOKEN[PipelineStage.mention_collection]}"
            f">chunk>{chunk_bounds}>sub>{sub_bounds}>group>{group_index}>"
            f"{require_mention_metadata(metadata).to_custom_id_segment()}"
            f"{upstream_digest_segment(group_forms)}"
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
            llm_phrase_search_gpt_request_map=self.get_upstream_phrase_search_map(
                pipeline_context
            ),
            llm_phrase_recursive_search_gpt_request_map=self.get_upstream_recursive_search_map(
                pipeline_context
            ),
            timestamp=timestamp,
            llm_model=mention_metadata.llm_model,
            model_params=mention_metadata.model_params,
            max_forms_per_request=mention_metadata.max_forms_per_request,
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
    ) -> FoldResult:
        """The chunk's aggregation fold (needs the text: windows are located in
        it, and the fold re-attributes from it)."""
        return await get_chunk_fold(
            subject_unique_id=subject_unique_id,
            field_type=field_type,
            chunk_bounds=chunk_bounds,
            extraction_bundle=extraction_bundle,
            completed_request_map=completed_request_map,
            timestamp=timestamp,
            subject_text=subject_text,
            verb_fold=verb_fold,
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
                await get_window_mentions(
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
