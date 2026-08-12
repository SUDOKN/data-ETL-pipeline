from __future__ import annotations
import logging
from datetime import datetime
from math import ceil
from typing import TYPE_CHECKING, Optional

from llm_providers.db_models.gpt_batch_request import (
    GPTBatchRequest,
)
from llm_providers.models.open_ai.gpt_batch_response_blob import (
    GPTBatchResponse,
)
from core.models.deferred_extraction.deferred_phrase_extraction_requests import (
    DeferredLLMPhraseExtractionRequests,
    LLMPhraseExtractionRequestBundle,
    LLMPhraseExtractionRequestMap,
    LLMPhraseExtractionMetadata,
)
from core.models.extraction_schemas.screening import (
    LiveScreeningResults,
)
from llm_providers.models.file_objects.prompt import Prompt
from core.models.field_types import ExtractionFieldType
from core.models.pipeline_nodes.base.base_node import (
    LLMExtractedFieldTypeVar,
    PipelineContext,
)
from core.models.pipeline_nodes.base.base_reconcile_node import (
    ReconcileNode,
)
from core.models.pipeline_nodes.base.base_llm_extraction_node import (
    BaseLLMExtractionNode,
)
from core.models.pipeline_nodes.multi_stage.base.llm_phrase_relationship_node import (
    LLMPhraseRelationshipNode,
)
from llm_providers.field_types import BatchRequestIDType

if TYPE_CHECKING:
    from scraper.models.s3.scraped_text_file import (
        ScrapedTextFile,
    )

from llm_providers.services.gpt_batch_request.gpt_batch_request_service import (
    dispatch_gpt_batch_request,
)
from core.services.pipeline_nodes.multi_stage.llm_relationship_screening_node_service import (
    create_missing_phrase_relationship_screening_requests,
    get_phrase_relationship_screening_result,
)

logger = logging.getLogger(__name__)


class LLMPhraseRelationshipScreeningNode(
    BaseLLMExtractionNode[LLMExtractedFieldTypeVar, LiveScreeningResults]
):

    def __init__(
        self,
        field_type: LLMExtractedFieldTypeVar,
        next_node: BaseLLMExtractionNode | ReconcileNode,
        phrase_relationship_screening_prompt: Prompt,
    ):
        super().__init__(
            field_type=field_type,
            next_node=next_node,
        )
        self.phrase_relationship_screening_prompt = phrase_relationship_screening_prompt

    def get_upstream_phrase_relationship_map(
        self, pipeline_context: PipelineContext
    ) -> dict[BatchRequestIDType, GPTBatchRequest]:
        """Return the completed phrase-relationship request map from pipeline context."""
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement get_upstream_phrase_relationship_map"
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
                f"Cannot embed req ids for llm phrase relationship screening, "
                f"as chunked_request_map found empty for subject:{subject_unique_id}, field:{self.field_type.name}."
            )

        # Unlike recursive search, the full set of upstream relationship pairs for
        # a chunk is already complete by the time screening embeds ids (the
        # relationship phase has already fully executed), so the group count can
        # be computed once, upfront — no iterative/eager convergence loop needed.
        max_pairs_per_request = (
            metadata.llm_phrase_relationship_screening.max_pairs_per_request
        )
        upstream_phrase_relationship_map = self.get_upstream_phrase_relationship_map(
            pipeline_context
        )

        for (
            chunk_bounds,
            extraction_request_bundle,
        ) in chunked_request_map.items():
            if extraction_request_bundle.llm_phrase_relationship_screening_req_ids:
                continue  # already embedded; group count is stable once computed

            llm_phrase_relationships = await LLMPhraseRelationshipNode.get_result(
                subject_unique_id=subject_unique_id,
                field_type=self.field_type,
                chunk_bounds=chunk_bounds,
                extraction_bundle=extraction_request_bundle,
                completed_request_map=upstream_phrase_relationship_map,
                timestamp=timestamp,
            )
            num_groups = max(
                1, ceil(len(llm_phrase_relationships) / max_pairs_per_request)
            )
            extraction_request_bundle.llm_phrase_relationship_screening_req_ids = [
                self.get_request_custom_id(
                    subject_unique_id=subject_unique_id,
                    field_type=self.field_type,
                    chunk_bounds=chunk_bounds,
                    group_index=group_index,
                    metadata=metadata,
                )
                for group_index in range(num_groups)
            ]

    def get_embedded_request_ids(
        self,
        subject_unique_id: str,
        chunked_request_map: LLMPhraseExtractionRequestMap,
    ) -> set[BatchRequestIDType]:
        llm_phrase_relationship_screening_req_ids: set[BatchRequestIDType] = set()
        for (
            chunk_bounds,
            extraction_bundle,
        ) in chunked_request_map.items():
            if not extraction_bundle.llm_phrase_relationship_screening_req_ids:
                raise ValueError(
                    f"get_embedded_request_ids was called for {subject_unique_id}:{self.field_type.name} but "
                    f"extraction_bundle.llm_phrase_relationship_screening_req_ids is empty for chunk bounds {chunk_bounds}."
                )

            llm_phrase_relationship_screening_req_ids.update(
                extraction_bundle.llm_phrase_relationship_screening_req_ids
            )

        return llm_phrase_relationship_screening_req_ids

    @staticmethod
    def get_request_custom_id(
        subject_unique_id: str,
        field_type: ExtractionFieldType,
        chunk_bounds: str,
        group_index: int,
        metadata: LLMPhraseExtractionMetadata,
    ) -> BatchRequestIDType:
        return (
            f"{subject_unique_id}>{field_type.name}>llm_phrase_relationship_screening>group>{group_index}>chunk>{chunk_bounds}>"
            f"{metadata.llm_phrase_relationship_screening.to_custom_id_segment()}"
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
        """Create batch requests for the phrase_relationship phase."""
        subject_name = pipeline_context.subject_name
        if not subject_name:
            raise ValueError(
                f"llm_phrase_relationship_screening_node.create_batch_requests was called for {self.field_type.name} in {self.__class__.__name__} but pipeline_context.subject_name is not set. Ensure business_desc is extracted before phrase_relationship."
            )

        # create_missing_phrase_relationship_requests only creates batch requests fresh or only missing ones,
        # for e.g., new subject or some batch requests failed earlier and were deleted to allow re-processing
        batch_requests = await create_missing_phrase_relationship_screening_requests(
            deferred_at=timestamp,
            subject_unique_id=subject_unique_id,
            subject_name=subject_name,
            field_type=self.field_type,
            missing_phrase_relationship_screening_req_ids=missing_request_ids,
            chunked_request_map=chunked_request_map,
            subject_text=scraped_text_file.text,
            phrase_relationship_screening_prompt=self.phrase_relationship_screening_prompt,
            llm_phrase_relationship_gpt_request_map=self.get_upstream_phrase_relationship_map(
                pipeline_context
            ),
            llm_model=metadata.llm_phrase_relationship_screening.llm_model,
            model_params=metadata.llm_phrase_relationship_screening.model_params,
            max_pairs_per_request=metadata.llm_phrase_relationship_screening.max_pairs_per_request,
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
        repairs: Optional[dict[str, str]] = None,
    ) -> LiveScreeningResults:
        return await get_phrase_relationship_screening_result(
            subject_unique_id=subject_unique_id,
            field_type=field_type,
            chunk_bounds=chunk_bounds,
            extraction_bundle=extraction_bundle,
            completed_request_map=completed_request_map,
            timestamp=timestamp,
            repairs=repairs,
        )

    async def validate_own_responses(
        self,
        subject_unique_id: str,
        chunked_request_map: LLMPhraseExtractionRequestMap,
        completed_request_map: dict[BatchRequestIDType, GPTBatchRequest],
        timestamp: datetime,
    ) -> None:
        """Parse every screening response here, where the requests were made.

        ``get_result`` is a pure parse over a map already in memory, so the
        downstream node parsing it again costs nothing but CPU on a JSON blob —
        cheap against being told which of this node's requests to re-run by the
        node that made them, one phase before a consumer trips over it.
        """
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
        metadata: LLMPhraseExtractionMetadata,
    ) -> GPTBatchResponse:
        return await dispatch_gpt_batch_request(
            gpt_batch_request=gpt_batch_request,
            gpt_model=metadata.llm_phrase_relationship_screening.llm_model,
            model_params=metadata.llm_phrase_relationship_screening.model_params,
        )
