from __future__ import annotations

import logging
from datetime import datetime

from llm_providers.db_models.gpt_batch_request import (
    GPTBatchRequest,
)
from core.models.deferred_extraction.deferred_keyword_extraction import (
    KeywordExtractionRequestMap,
    KeywordExtractionRequestBundle,
)
from core.models.extraction_schemas.grounding import (
    RecordGroundingResults,
)
from llm_providers.models.open_ai.gpt_batch_response_blob import (
    GPTBatchResponse,
)
from core.models.extraction_results.llm_phrase_extraction_results_v2 import (
    KeywordExtractionMetadataV2,
)
from core.models.rule_catalog import STAGE_FREEHAND_GROUNDING, RuleCatalog
from llm_providers.models.file_objects.prompt import Prompt
from llm_providers.services.gpt_batch_request.gpt_batch_request_service import (
    dispatch_gpt_batch_request,
)
from core.models.pipeline_nodes.base.base_llm_extraction_node import (
    BaseLLMExtractionNode,
)
from core.models.pipeline_nodes.base.base_node import (
    LLMExtractedFieldTypeVar,
    PipelineContext,
)
from core.models.pipeline_nodes.base.base_reconcile_node import (
    ReconcileNode,
)
from core.models.pipeline_nodes.multi_stage.base.llm_phrase_relationship_node import (
    LLMPhraseRelationshipNode,
)
from core.models.field_types import ExtractionFieldType

from core.services.pipeline_nodes.multi_stage.llm_grounding_node_service import (
    build_record_payloads,
    create_missing_record_grounding_requests,
    get_record_grounding_result,
    grouped_record_payloads,
)
from core.services.pipeline_nodes.multi_stage.llm_phrase_relationship_node_service import (
    records_with_mentions,
)
from core.services.rule_catalog_registry import get_rule_catalog
from core.utils.request_custom_id_util import upstream_digest_segment
from llm_providers.field_types import BatchRequestIDType
from scraper.models.s3.scraped_text_file import ScrapedTextFile
from typing import Any, ClassVar, Optional
from core.models.pipeline_nodes.base.pipeline_stage import (
    STAGE_REQUEST_ID_TOKEN,
    PipelineStage,
)

logger = logging.getLogger(__name__)


class LLMPhraseFreehandGroundingNode(
    BaseLLMExtractionNode[LLMExtractedFieldTypeVar, RecordGroundingResults]
):
    """The keyword families' enumeration pass: every evidence-bearing record,
    no options at all — the model mints each candidate itself. Runs straight
    off the relationship stage (screening vets its mints downstream in v2)."""

    stage: ClassVar[PipelineStage] = PipelineStage.freehand_grounding

    def __init__(
        self,
        field_type: LLMExtractedFieldTypeVar,
        next_node: BaseLLMExtractionNode | ReconcileNode,
        phrase_freehand_grounding_prompt: Prompt,
    ):
        super().__init__(field_type=field_type, next_node=next_node)
        self.phrase_freehand_grounding_prompt = phrase_freehand_grounding_prompt

    def get_upstream_phrase_relationship_map(
        self, pipeline_context: PipelineContext
    ) -> dict[BatchRequestIDType, GPTBatchRequest]:
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement get_upstream_phrase_relationship_map"
        )

    def catalog(self) -> RuleCatalog:
        return get_rule_catalog(STAGE_FREEHAND_GROUNDING, self.field_type.name)

    async def _chunk_record_payloads(
        self,
        subject_unique_id: str,
        chunk_bounds: str,
        extraction_bundle: KeywordExtractionRequestBundle,
        upstream_relationship_map: dict[BatchRequestIDType, GPTBatchRequest],
        timestamp: datetime,
    ) -> dict[str, dict[str, Any]]:
        masked = await LLMPhraseRelationshipNode.get_result(
            subject_unique_id=subject_unique_id,
            field_type=self.field_type,
            chunk_bounds=chunk_bounds,
            extraction_bundle=extraction_bundle,
            completed_request_map=upstream_relationship_map,
            timestamp=timestamp,
        )
        return build_record_payloads(records_with_mentions(masked))

    async def embed_request_ids(
        self,
        subject_unique_id: str,
        pipeline_context: PipelineContext,
        metadata: KeywordExtractionMetadataV2,
        chunked_request_map: KeywordExtractionRequestMap,
        timestamp: datetime,
    ):
        if not chunked_request_map:
            raise ValueError(
                f"Cannot embed req ids for llm freehand grounding node, "
                f"as chunked_request_map found empty for subject:{subject_unique_id}, field:{self.field_type.name}."
            )

        # The relationship stage has fully executed by the time grounding embeds
        # ids, so the record set for a chunk is final and the group count can be
        # computed once, upfront.
        max_pairs_per_request = (
            metadata.llm_phrase_freehand_grounding.max_pairs_per_request
        )
        upstream_relationship_map = self.get_upstream_phrase_relationship_map(
            pipeline_context
        )

        for chunk_bounds, extraction_request_bundle in chunked_request_map.items():
            if extraction_request_bundle.llm_phrase_freehand_grounding_req_ids:
                continue  # already embedded; group count is stable once computed

            payloads = await self._chunk_record_payloads(
                subject_unique_id=subject_unique_id,
                chunk_bounds=chunk_bounds,
                extraction_bundle=extraction_request_bundle,
                upstream_relationship_map=upstream_relationship_map,
                timestamp=timestamp,
            )
            extraction_request_bundle.llm_phrase_freehand_grounding_req_ids = [
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

    def get_embedded_request_ids(
        self,
        subject_unique_id: str,
        chunked_request_map: KeywordExtractionRequestMap,
    ) -> set[BatchRequestIDType]:
        req_ids: set[BatchRequestIDType] = set()
        for chunk_bounds, extraction_bundle in chunked_request_map.items():
            if not extraction_bundle.llm_phrase_freehand_grounding_req_ids:
                raise ValueError(
                    f"Cannot get embedded request ids for subject_unique_id:{subject_unique_id}>{chunk_bounds} as "
                    f"llm_phrase_freehand_grounding_req_ids is empty in the extraction_bundle."
                )
            req_ids.update(extraction_bundle.llm_phrase_freehand_grounding_req_ids)
        return req_ids

    @staticmethod
    def get_request_custom_id(
        subject_unique_id: str,
        field_type: ExtractionFieldType,
        chunk_bounds: str,
        group_index: int,
        metadata: KeywordExtractionMetadataV2,
        group_payload: dict[str, dict[str, Any]],
    ) -> BatchRequestIDType:
        # `|ud=` (fork F12): the group's own record payloads are part of request
        # identity — see the relationship node's twin comment.
        return (
            f"{subject_unique_id}>{field_type.name}"
            f">{STAGE_REQUEST_ID_TOKEN[PipelineStage.freehand_grounding]}"
            f">group>{group_index}>chunk>{chunk_bounds}>"
            f"{metadata.llm_phrase_freehand_grounding.to_custom_id_segment()}"
            f"{upstream_digest_segment(group_payload)}"
        )

    async def create_batch_requests(
        self,
        subject_unique_id: str,
        scraped_text_file: ScrapedTextFile,
        missing_request_ids: set[BatchRequestIDType],
        metadata: KeywordExtractionMetadataV2,
        chunked_request_map: KeywordExtractionRequestMap,
        timestamp: datetime,
        pipeline_context: PipelineContext,
        eager: bool,
    ) -> list[GPTBatchRequest]:
        upstream_relationship_map = self.get_upstream_phrase_relationship_map(
            pipeline_context
        )
        chunk_payload_maps: dict[str, dict[str, dict[str, Any]]] = {}
        group_req_ids_by_chunk: dict[str, list[BatchRequestIDType]] = {}
        options_section_by_chunk: dict[str, Optional[str]] = {}
        for chunk_bounds, extraction_bundle in chunked_request_map.items():
            group_req_ids_by_chunk[chunk_bounds] = (
                extraction_bundle.llm_phrase_freehand_grounding_req_ids
            )
            if not (
                set(extraction_bundle.llm_phrase_freehand_grounding_req_ids)
                & missing_request_ids
            ):
                continue
            chunk_payload_maps[chunk_bounds] = await self._chunk_record_payloads(
                subject_unique_id=subject_unique_id,
                chunk_bounds=chunk_bounds,
                extraction_bundle=extraction_bundle,
                upstream_relationship_map=upstream_relationship_map,
                timestamp=timestamp,
            )
            # Freehand is the stage that is sent no options at all.
            options_section_by_chunk[chunk_bounds] = None

        return await create_missing_record_grounding_requests(
            stage_label="freehand grounding",
            subject_unique_id=subject_unique_id,
            field_name=self.field_type.name,
            chunk_payload_maps=chunk_payload_maps,
            group_req_ids_by_chunk=group_req_ids_by_chunk,
            missing_req_ids=missing_request_ids,
            prompt=self.phrase_freehand_grounding_prompt,
            catalog=self.catalog(),
            options_section_by_chunk=options_section_by_chunk,
            max_records_per_request=metadata.llm_phrase_freehand_grounding.max_pairs_per_request,
            deferred_at=timestamp,
            llm_model=metadata.llm_phrase_freehand_grounding.llm_model,
            model_params=metadata.llm_phrase_freehand_grounding.model_params,
            eager=eager,
            dummy_note=(
                "No freehand grounding needed - no record carries any evidence "
                "for this chunk."
            ),
        )

    @staticmethod
    async def get_result(
        subject_unique_id: str,
        field_type: ExtractionFieldType,
        chunk_bounds: str,
        extraction_bundle: KeywordExtractionRequestBundle,
        completed_request_map: dict[BatchRequestIDType, GPTBatchRequest],
        timestamp: datetime,  # for recording errors
    ) -> RecordGroundingResults:
        return await get_record_grounding_result(
            stage_label="freehand grounding",
            subject_unique_id=subject_unique_id,
            field_name=field_type.name,
            chunk_bounds=chunk_bounds,
            catalog=get_rule_catalog(STAGE_FREEHAND_GROUNDING, field_type.name),
            group_req_ids=extraction_bundle.llm_phrase_freehand_grounding_req_ids,
            completed_request_map=completed_request_map,
            timestamp=timestamp,
        )

    async def validate_own_responses(
        self,
        subject_unique_id: str,
        chunked_request_map: KeywordExtractionRequestMap,
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
        metadata: KeywordExtractionMetadataV2,
    ) -> GPTBatchResponse:
        return await dispatch_gpt_batch_request(
            gpt_batch_request=gpt_batch_request,
            gpt_model=metadata.llm_phrase_freehand_grounding.llm_model,
        )
