from __future__ import annotations

import logging
from datetime import datetime
from typing import override

from llm_providers.db_models.gpt_batch_request import (
    GPTBatchRequest,
)
from llm_providers.models.open_ai.gpt_batch_response_blob import (
    GPTBatchResponse,
)
from llm_providers.models.file_objects.prompt import Prompt
from core.models.extraction_schemas.search import LLMSearchResults
from core.models.deferred_extraction.deferred_phrase_extraction_requests import (
    DeferredLLMPhraseExtractionRequests,
    LLMPhraseExtractionMetadata,
    LLMPhraseExtractionRequestBundle,
    LLMPhraseExtractionRequestMap,
)
from core.models.pipeline_nodes.base.base_node import (
    LLMExtractedFieldTypeVar,
    PipelineContext,
)
from core.models.pipeline_nodes.base.base_reconcile_node import (
    ReconcileNode,
)
from core.models.field_types import (
    ExtractionFieldType,
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
from core.services.pipeline_nodes.multi_stage.llm_phrase_search_node_service import (
    parse_batch_request_result as parse_phrase_search_batch_req_result,
)
from core.services.pipeline_nodes.multi_stage.llm_phrase_recursive_search_node_service import (
    create_missing_phrase_recursive_search_requests,
    parse_recursive_search_round_result,
    get_new_phrases_for_latest_round,
    get_all_recursive_round_results,
)
from typing import ClassVar
from core.models.pipeline_nodes.base.pipeline_stage import PipelineStage

logger = logging.getLogger(__name__)


class LLMPhraseRecursiveSearchNode(
    BaseLLMRecursiveExtractionNode[LLMExtractedFieldTypeVar, LLMSearchResults]
):
    """Recursive phrase search phase.

    Runs after the first :class:`LLMPhraseSearchNode`. Each round re-searches every
    chunk while excluding the compounding union of phrases already found (first
    search + all prior rounds), so the LLM is nudged to surface *new* phrases.

    Recursion is driven by the re-entrant :meth:`embed_request_ids`: a round's
    request id is embedded only after the previous round completes, and only while
    the previous round yielded new phrases AND the ``max_rounds`` cap has not been
    reached. When neither holds, no further ids are embedded and the base
    ``execute`` loop advances to the next node with the union available via
    ``pipeline_context[type(self)]``.
    """

    stage: ClassVar[PipelineStage] = PipelineStage.recursive_search

    def __init__(
        self,
        field_type: LLMExtractedFieldTypeVar,
        next_node: BaseLLMExtractionNode | ReconcileNode,
        recursive_search_prompt: Prompt,
    ):
        super().__init__(
            field_type=field_type,
            next_node=next_node,
        )
        self.recursive_search_prompt = recursive_search_prompt

    def get_upstream_first_search_map(
        self, pipeline_context: PipelineContext
    ) -> dict[BatchRequestIDType, GPTBatchRequest]:
        """Return the completed first-search request map from the pipeline context.

        Subclasses bind this to the concrete upstream search node class (e.g.
        ``pipeline_context[ConceptPhraseSearchNode]``).
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement get_upstream_first_search_map"
        )

    @staticmethod
    @override
    def get_request_custom_id(
        subject_unique_id: str,
        field_type: ExtractionFieldType,
        chunk_bounds: str,
        round_index: int,
        metadata: LLMPhraseExtractionMetadata,
    ) -> BatchRequestIDType:
        recursive_meta = metadata.llm_phrase_recursive_search
        return (
            f"{subject_unique_id}>{field_type.name}>llm_recursive_search>round>{round_index}>chunk>{chunk_bounds}>"
            f"{recursive_meta.to_custom_id_segment()}"
        )

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
                f"Cannot embed req ids for llm recursive search node, "
                f"as chunked_request_map found empty for subject:{subject_unique_id}, field:{self.field_type.name}."
            )

        recursive_meta = metadata.llm_phrase_recursive_search
        if recursive_meta.max_rounds == 0:
            logger.info(f"Recursive phrase extraction will be skipped as max_rounds=0.")
            return

        # Seed round 1 (index 0) for every chunk on the first entry.
        any_rounds_embedded = any(
            bundle.llm_phrase_recursive_search_req_ids
            for bundle in chunked_request_map.values()
        )
        if not any_rounds_embedded:
            for chunk_bounds, bundle in chunked_request_map.items():
                bundle.llm_phrase_recursive_search_req_ids.append(
                    self.get_request_custom_id(
                        subject_unique_id=subject_unique_id,
                        field_type=self.field_type,
                        chunk_bounds=chunk_bounds,
                        round_index=0,
                        metadata=metadata,
                    )
                )
            return

        # Only advance to the next round once every embedded round has completed.
        if not await self.are_all_requests_complete(
            subject_unique_id=subject_unique_id, chunked_request_map=chunked_request_map
        ):
            logger.info(
                f"[{subject_unique_id}] Waiting for recursive search rounds to complete before embedding the next round."
            )
            return

        completed_recursive_map = await self.get_completed_request_map(
            subject_unique_id=subject_unique_id,
            chunked_request_map=chunked_request_map,
        )
        first_search_map = self.get_upstream_first_search_map(pipeline_context)

        for chunk_bounds, bundle in chunked_request_map.items():
            rounds = bundle.llm_phrase_recursive_search_req_ids
            if len(rounds) >= recursive_meta.max_rounds:
                continue  # hard cap reached for this chunk

            first_search_results = await parse_phrase_search_batch_req_result(
                subject_unique_id=subject_unique_id,
                field_type=self.field_type,
                chunk_bounds=chunk_bounds,
                extraction_bundle=bundle,
                all_phrase_search_req_responses_map=first_search_map,
                deferred_at=timestamp,
            )

            accumulated_before_latest: set[str] = set(first_search_results)
            for prior_round_req_id in rounds[:-1]:
                accumulated_before_latest |= await parse_recursive_search_round_result(
                    subject_unique_id=subject_unique_id,
                    field_type=self.field_type,
                    chunk_bounds=chunk_bounds,
                    round_req_id=prior_round_req_id,
                    completed_request_map=completed_recursive_map,
                    timestamp=timestamp,
                )

            latest_round_results = await parse_recursive_search_round_result(
                subject_unique_id=subject_unique_id,
                field_type=self.field_type,
                chunk_bounds=chunk_bounds,
                round_req_id=rounds[-1],
                completed_request_map=completed_recursive_map,
                timestamp=timestamp,
            )
            new_phrases = get_new_phrases_for_latest_round(
                accumulated_before_latest=accumulated_before_latest,
                latest_round_results=latest_round_results,
            )

            if new_phrases:
                # Latest round surfaced new phrases -> embed one more round.
                bundle.llm_phrase_recursive_search_req_ids.append(
                    self.get_request_custom_id(
                        subject_unique_id=subject_unique_id,
                        field_type=self.field_type,
                        chunk_bounds=chunk_bounds,
                        round_index=len(rounds),
                        metadata=metadata,
                    )
                )
            else:
                logger.info(
                    f"[{subject_unique_id}] Recursive search converged for chunk {chunk_bounds} "
                    f"({self.field_type.name}) after {len(rounds)} round(s)."
                )

    def get_embedded_request_ids(
        self,
        subject_unique_id: str,
        chunked_request_map: LLMPhraseExtractionRequestMap,
    ) -> set[BatchRequestIDType]:
        recursive_search_req_ids: set[BatchRequestIDType] = set()
        for _chunk_bounds, bundle in chunked_request_map.items():
            recursive_search_req_ids.update(bundle.llm_phrase_recursive_search_req_ids)
        return recursive_search_req_ids

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
        """Create batch requests for the missing recursive search rounds."""
        recursive_meta = metadata.llm_phrase_recursive_search
        first_search_map = self.get_upstream_first_search_map(pipeline_context)

        completed_recursive_map = await self.get_completed_request_map(
            subject_unique_id=subject_unique_id,
            chunked_request_map=chunked_request_map,
            all_requests_must_be_complete=False,
        )

        batch_requests = await create_missing_phrase_recursive_search_requests(
            timestamp=timestamp,
            field_type=self.field_type,
            missing_recursive_search_req_ids=missing_request_ids,
            chunked_request_map=chunked_request_map,
            subject_unique_id=subject_unique_id,
            subject_text=scraped_text_file.text,
            recursive_search_prompt=self.recursive_search_prompt,
            first_search_gpt_request_map=first_search_map,
            completed_recursive_search_req_map=completed_recursive_map,
            llm_model=recursive_meta.llm_model,
            model_params=recursive_meta.model_params,
            eager=eager,
        )

        return batch_requests

    @staticmethod
    async def get_result(
        subject_unique_id: str,
        field_type: ExtractionFieldType,
        chunk_bounds: str,
        extraction_bundle: LLMPhraseExtractionRequestBundle,
        completed_request_map: dict[BatchRequestIDType, GPTBatchRequest],
        timestamp: datetime,  # for recording errors
    ) -> LLMSearchResults:
        return await get_all_recursive_round_results(
            subject_unique_id=subject_unique_id,
            field_type=field_type,
            chunk_bounds=chunk_bounds,
            extraction_bundle=extraction_bundle,
            completed_request_map=completed_request_map,
            timestamp=timestamp,
        )

    async def dispatch_batch_request(
        self,
        gpt_batch_request: GPTBatchRequest,
        metadata: LLMPhraseExtractionMetadata,
    ) -> GPTBatchResponse:
        recursive_meta = metadata.llm_phrase_recursive_search
        return await dispatch_gpt_batch_request(
            gpt_batch_request=gpt_batch_request,
            gpt_model=recursive_meta.llm_model,
            model_params=recursive_meta.model_params,
        )
