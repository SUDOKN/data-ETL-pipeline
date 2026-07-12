from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional, override

from core.models.db.deferred_manufacturer import DeferredManufacturer
from core.models.db.gpt_batch_request import GPTBatchRequest
from core.models.gpt_batch_response_blob import GPTBatchResponse
from core.models.prompt import Prompt
from core.models.field_types import LLMSearchResults
from core.models.deferred_phrase_extraction_requests import (
    DeferredLLMPhraseExtractionRequests,
    LLMPhraseExtractionMetadata,
    LLMPhraseExtractionRequestMap,
)
from data_etl_app.models.pipeline_nodes.base.base_node import (
    LLMExtractedFieldTypeVar,
    PipelineContext,
)
from data_etl_app.models.pipeline_nodes.base.base_reconcile_node import ReconcileNode
from data_etl_app.models.types_and_enums import (
    LLMExtractedFieldTypeEnum,
)
from data_etl_app.models.pipeline_nodes.base.base_llm_extraction_node import (
    BaseLLMExtractionNode,
)
from data_etl_app.models.pipeline_nodes.base.base_llm_recursive_extraction_node import (
    BaseLLMRecursiveExtractionNode,
)
from open_ai_key_app.models.field_types import GPTBatchRequestCustomID
from scraper_app.models.scraped_text_file import ScrapedTextFile

from core.services.gpt_batch_request_service import dispatch_gpt_batch_request
from data_etl_app.services.extraction.deferred_llm_phrase_search_node_service import (
    parse_batch_request_result as parse_phrase_search_batch_req_result,
)
from data_etl_app.services.extraction.deferred_llm_phrase_recursive_search_node_service import (
    create_missing_phrase_recursive_search_requests,
    parse_recursive_search_round_result,
    get_new_phrases_for_latest_round,
)

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
    ) -> dict[GPTBatchRequestCustomID, GPTBatchRequest]:
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
        mfg_etld1: str,
        field_type: LLMExtractedFieldTypeEnum,
        chunk_bounds: str,
        round_index: int,
        metadata: LLMPhraseExtractionMetadata,
    ) -> GPTBatchRequestCustomID:
        recursive_meta = metadata.llm_phrase_recursive_search
        return (
            f"{mfg_etld1}>{field_type.name}>llm_recursive_search>round>{round_index}>chunk>{chunk_bounds}>"
            f"{recursive_meta.model_params.to_custom_id_segment(recursive_meta.llm_model.name)}"
        )

    async def embed_request_ids(  # prefill folded into this function
        self,
        mfg_etld1: str,
        pipeline_context: PipelineContext,
        metadata: LLMPhraseExtractionMetadata,
        request_map: LLMPhraseExtractionRequestMap,
        timestamp: datetime,
    ):
        if not request_map:
            raise ValueError(
                f"Cannot embed req ids for llm recursive search node, "
                f"as request_map found empty for mfg:{mfg_etld1}, field:{self.field_type.name}."
            )

        recursive_meta = metadata.llm_phrase_recursive_search
        if recursive_meta.max_rounds == 0:
            logger.info(f"Recursive phrase extraction will be skipped as max_rounds=0.")
            return

        # Seed round 1 (index 0) for every chunk on the first entry.
        any_rounds_embedded = any(
            bundle.llm_phrase_recursive_search_req_ids
            for bundle in request_map.values()
        )
        if not any_rounds_embedded:
            for chunk_bounds, bundle in request_map.items():
                bundle.llm_phrase_recursive_search_req_ids.append(
                    self.get_request_custom_id(
                        mfg_etld1=mfg_etld1,
                        field_type=self.field_type,
                        chunk_bounds=chunk_bounds,
                        round_index=0,
                        metadata=metadata,
                    )
                )
            return

        # Only advance to the next round once every embedded round has completed.
        if not await self.are_all_requests_complete(
            mfg_etld1=mfg_etld1, request_map=request_map
        ):
            logger.info(
                f"[{mfg_etld1}] Waiting for recursive search rounds to complete before embedding the next round."
            )
            return

        completed_recursive_map = await self.get_completed_request_map(
            mfg_etld1=mfg_etld1,
            request_map=request_map,
            all_requests_must_be_complete=False,
        )
        first_search_map = self.get_upstream_first_search_map(pipeline_context)

        for chunk_bounds, bundle in request_map.items():
            rounds = bundle.llm_phrase_recursive_search_req_ids
            if len(rounds) >= recursive_meta.max_rounds:
                continue  # hard cap reached for this chunk

            first_search_results = await parse_phrase_search_batch_req_result(
                mfg_etld1=mfg_etld1,
                field_type=self.field_type,
                chunk_bounds=chunk_bounds,
                extraction_bundle=bundle,
                all_phrase_search_req_responses_map=first_search_map,
                deferred_at=timestamp,
            )

            accumulated_before_latest: set[str] = set(first_search_results)
            for prior_round_req_id in rounds[:-1]:
                accumulated_before_latest |= parse_recursive_search_round_result(
                    prior_round_req_id, completed_recursive_map
                )

            latest_round_results = parse_recursive_search_round_result(
                rounds[-1], completed_recursive_map
            )
            new_phrases = get_new_phrases_for_latest_round(
                accumulated_before_latest=accumulated_before_latest,
                latest_round_results=latest_round_results,
            )

            if new_phrases:
                # Latest round surfaced new phrases -> embed one more round.
                bundle.llm_phrase_recursive_search_req_ids.append(
                    self.get_request_custom_id(
                        mfg_etld1=mfg_etld1,
                        field_type=self.field_type,
                        chunk_bounds=chunk_bounds,
                        round_index=len(rounds),
                        metadata=metadata,
                    )
                )
            else:
                logger.info(
                    f"[{mfg_etld1}] Recursive search converged for chunk {chunk_bounds} "
                    f"({self.field_type.name}) after {len(rounds)} round(s)."
                )

    def get_embedded_request_ids(
        self,
        mfg_etld1: str,
        request_map: LLMPhraseExtractionRequestMap,
    ) -> set[GPTBatchRequestCustomID]:
        recursive_search_req_ids: set[GPTBatchRequestCustomID] = set()
        for _chunk_bounds, bundle in request_map.items():
            recursive_search_req_ids.update(bundle.llm_phrase_recursive_search_req_ids)
        return recursive_search_req_ids

    async def create_batch_requests(
        self,
        missing_request_ids: set[GPTBatchRequestCustomID],
        deferred_mfg: DeferredManufacturer,
        scraped_text_file: ScrapedTextFile,
        timestamp: datetime,
        pipeline_context: PipelineContext,
        eager: bool,
    ) -> list[GPTBatchRequest]:
        """Create batch requests for the missing recursive search rounds."""
        extraction_requests: Optional[DeferredLLMPhraseExtractionRequests] = getattr(
            deferred_mfg, self.field_type.name
        )
        if not extraction_requests:
            raise ValueError(
                f"create_batch_requests was called for {self.field_type.name} in {self.__class__.__name__} but no deferred extraction exists."
            )
        recursive_meta = extraction_requests.metadata.llm_phrase_recursive_search
        first_search_map = self.get_upstream_first_search_map(pipeline_context)

        completed_recursive_map = await self.get_completed_request_map(
            mfg_etld1=deferred_mfg.etld1,
            request_map=extraction_requests.chunked_request_map,
            all_requests_must_be_complete=False,
        )

        batch_requests = await create_missing_phrase_recursive_search_requests(
            deferred_at=timestamp,
            field_type=self.field_type,
            missing_recursive_search_req_ids=missing_request_ids,
            chunked_request_map=extraction_requests.chunked_request_map,
            mfg_etld1=deferred_mfg.etld1,
            mfg_text=scraped_text_file.text,
            recursive_search_prompt=self.recursive_search_prompt,
            first_search_gpt_request_map=first_search_map,
            completed_recursive_search_req_map=completed_recursive_map,
            llm_model=recursive_meta.llm_model,
            model_params=recursive_meta.model_params,
            eager=eager,
        )

        return batch_requests

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
