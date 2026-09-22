"""The Step 2 unit-screening stage (design draft §5; user decision 2026-09-20:
grounding works the recall axis, screening the accuracy axis as guards, and
screening VETS UNITS AND IDENTIFIES NOTHING).

A unit is one candidate label with the records grounding matched on. The
records tagged to the same label are regrouped in code
(``units_for_screening``, aliases folded to the vocabulary name — ruling 25),
packed into requests of at most the record cap (``pack_units``), and judged
record by record against the manufacturer named at the top: accepted with an
evidence distance and the record's words, or not accepted with the first
condition or guard that failed. The evidence distance is METADATA until the
census calibrates it (never a gate).

This node runs ONE WAVE — the keyword fields' whole screening (the freehand
candidates), and the base the concept fields' descent loop reuses per
vocabulary depth (substep 5, §6.3). Single pass, no under-answer retry: the
parser holds every response to the request's own units and records exactly,
so a breach is a parse error re-dispatched under its cap.
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

from core.models.deferred_extraction.deferred_phrase_extraction_requests import (
    LLMPhraseExtractionRequestBundle,
    LLMPhraseExtractionRequestMap,
)
from core.models.extraction_results.extraction_node_metadata import (
    BatchedScreeningNodeMetadata,
)
from core.models.extraction_results.llm_phrase_extraction_results import (
    LLMPhraseExtractionMetadata,
)
from core.models.extraction_schemas.screening import RecordScreeningResults
from core.models.extraction_schemas.synthesis import GroupRecords
from core.models.field_types import ExtractionFieldType, LLMExtractedFieldTypeVar
from core.models.pipeline_nodes.base.base_llm_extraction_node import (
    BaseLLMExtractionNode,
)
from core.models.pipeline_nodes.base.base_node import PipelineContext
from core.models.pipeline_nodes.base.base_reconcile_node import ReconcileNode
from core.models.pipeline_nodes.base.pipeline_stage import (
    STAGE_REQUEST_ID_TOKEN,
    PipelineStage,
)
from core.services.pipeline_nodes.multi_stage.llm_phrase_synthesis_node_service import (
    get_chunk_group_records,
)
from core.services.pipeline_nodes.multi_stage.llm_relationship_screening_node_service import (
    UnitRequestPayload,
    build_unit_request_payload,
    create_missing_unit_screening_requests,
    get_unit_screening_result,
    unit_screening_catalog_for,
)
from core.services.pipeline_nodes.multi_stage.stage_derivations import pack_units
from core.utils.request_custom_id_util import upstream_digest_segment

logger = logging.getLogger(__name__)


def unit_screening_metadata_of(
    metadata: LLMPhraseExtractionMetadata,
) -> BatchedScreeningNodeMetadata:
    if metadata.llm_phrase_unit_screening is None:
        raise ValueError(
            "the unit-screening node needs metadata.llm_phrase_unit_screening (the "
            "Step 2 stage's identity); the chain was built without it"
        )
    return metadata.llm_phrase_unit_screening


class LLMPhraseUnitScreeningNode(
    BaseLLMExtractionNode[LLMExtractedFieldTypeVar, RecordScreeningResults]
):
    stage: ClassVar[PipelineStage] = PipelineStage.unit_screening
    # The wave this node's requests belong to. The standalone node (keyword
    # fields) runs wave 1; the descent loop issues later waves.
    WAVE: ClassVar[int] = 1

    def __init__(
        self,
        field_type: LLMExtractedFieldTypeVar,
        next_node: BaseLLMExtractionNode | ReconcileNode,
        phrase_unit_screening_prompt: Prompt,
    ):
        super().__init__(field_type=field_type, next_node=next_node)
        self.prompt = phrase_unit_screening_prompt

    # --- what the subclasses supply -----------------------------------------

    def get_upstream_synthesis_map(
        self, pipeline_context: PipelineContext
    ) -> dict[BatchRequestIDType, GPTBatchRequest]:
        raise NotImplementedError(f"{self.__class__.__name__} must implement get_upstream_synthesis_map")

    async def get_chunk_units(
        self,
        subject_unique_id: str,
        chunk_bounds: str,
        extraction_bundle: LLMPhraseExtractionRequestBundle,
        pipeline_context: PipelineContext,
        timestamp: datetime,
    ) -> dict[str, list[str]]:
        """The wave's units for this chunk: label → sorted record ids, aliases
        already folded (``units_for_screening``)."""
        raise NotImplementedError(f"{self.__class__.__name__} must implement get_chunk_units")

    def meaning_of(self, label: str) -> Optional[str]:
        """The vocabulary's definition of a label, or None (keyword fields,
        proposals)."""
        return None

    # --- helpers -----------------------------------------------------------

    @staticmethod
    def _subject_text_of(pipeline_context: PipelineContext, subject_unique_id: str, field: str) -> str:
        if pipeline_context.subject_text is None:
            raise ValueError(
                f"Cannot embed req ids for unit screening: PipelineContext.subject_text is None "
                f"for subject:{subject_unique_id}, field:{field}."
            )
        return pipeline_context.subject_text

    @staticmethod
    def _subject_name_of(pipeline_context: PipelineContext, subject_unique_id: str, field: str) -> str:
        if not pipeline_context.subject_name:
            raise ValueError(
                f"Cannot create unit screening requests: PipelineContext.subject_name is empty "
                f"for subject:{subject_unique_id}, field:{field}."
            )
        return pipeline_context.subject_name

    async def _chunk_request_payloads(
        self,
        subject_unique_id: str,
        chunk_bounds: str,
        extraction_bundle: LLMPhraseExtractionRequestBundle,
        pipeline_context: PipelineContext,
        subject_text: str,
        metadata: LLMPhraseExtractionMetadata,
        timestamp: datetime,
    ) -> list[UnitRequestPayload]:
        """The chunk's packed request payloads for this wave, in group order —
        derived identically at embed time and at create time."""
        group_records: GroupRecords = await get_chunk_group_records(
            subject_unique_id, self.field_type, chunk_bounds, extraction_bundle, timestamp,
            synthesis_completed_request_map=self.get_upstream_synthesis_map(pipeline_context),
            subject_text=subject_text, metadata=metadata,
        )
        units = await self.get_chunk_units(
            subject_unique_id=subject_unique_id, chunk_bounds=chunk_bounds,
            extraction_bundle=extraction_bundle, pipeline_context=pipeline_context, timestamp=timestamp,
        )
        cap = unit_screening_metadata_of(metadata).max_pairs_per_request
        return [
            build_unit_request_payload(group_records, group, meaning_of=self.meaning_of)
            for group in pack_units(units, cap)
        ]

    def _wave_ids(self, bundle: LLMPhraseExtractionRequestBundle) -> list[BatchRequestIDType]:
        return bundle.llm_phrase_unit_screening_req_ids.get(self.WAVE, [])

    # --- the node contract ---------------------------------------------------

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
                f"Cannot embed req ids for unit screening, as chunked_request_map found "
                f"empty for subject:{subject_unique_id}, field:{self.field_type.name}."
            )
        subject_text = self._subject_text_of(pipeline_context, subject_unique_id, self.field_type.name)
        for chunk_bounds, bundle in chunked_request_map.items():
            if self.WAVE in bundle.llm_phrase_unit_screening_req_ids:
                continue  # already embedded; the packing is stable once computed
            payloads = await self._chunk_request_payloads(
                subject_unique_id=subject_unique_id, chunk_bounds=chunk_bounds, extraction_bundle=bundle,
                pipeline_context=pipeline_context, subject_text=subject_text, metadata=metadata, timestamp=timestamp,
            )
            bundle.llm_phrase_unit_screening_req_ids[self.WAVE] = [
                self.get_request_custom_id(
                    subject_unique_id=subject_unique_id, field_type=self.field_type, chunk_bounds=chunk_bounds,
                    group_index=group_index, metadata=metadata, group_payload=payload, wave=self.WAVE,
                )
                for group_index, payload in enumerate(payloads)
            ]

    def get_embedded_request_ids(
        self,
        subject_unique_id: str,
        chunked_request_map: LLMPhraseExtractionRequestMap,
    ) -> set[BatchRequestIDType]:
        req_ids: set[BatchRequestIDType] = set()
        for chunk_bounds, bundle in chunked_request_map.items():
            if self.WAVE not in bundle.llm_phrase_unit_screening_req_ids:
                raise ValueError(
                    f"get_embedded_request_ids was called for {subject_unique_id}:{self.field_type.name} "
                    f"but wave {self.WAVE} of unit screening is not embedded for chunk bounds {chunk_bounds}."
                )
            req_ids.update(self._wave_ids(bundle))
        return req_ids

    @staticmethod
    def get_request_custom_id(
        subject_unique_id: str,
        field_type: ExtractionFieldType,
        chunk_bounds: str,
        group_index: int,
        metadata: LLMPhraseExtractionMetadata,
        group_payload: dict[str, Any],
        wave: int = 1,
    ) -> BatchRequestIDType:
        # `|ud=`: the request's own payload — its records AND its units — is
        # part of request identity, so a grounding change re-asks the
        # screening question instead of replaying verdicts on units that no
        # longer exist. The wave sits before the group index.
        return (
            f"{subject_unique_id}>{field_type.name}"
            f">{STAGE_REQUEST_ID_TOKEN[PipelineStage.unit_screening]}"
            f">wave>{wave}>group>{group_index}>chunk>{chunk_bounds}>"
            f"{unit_screening_metadata_of(metadata).to_custom_id_segment()}"
            f"{upstream_digest_segment(group_payload)}"
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
        stage_metadata = unit_screening_metadata_of(metadata)
        payloads_by_req_id: dict[BatchRequestIDType, UnitRequestPayload] = {}
        for chunk_bounds, bundle in chunked_request_map.items():
            wave_ids = self._wave_ids(bundle)
            if not set(wave_ids) & missing_request_ids:
                continue
            payloads = await self._chunk_request_payloads(
                subject_unique_id=subject_unique_id, chunk_bounds=chunk_bounds, extraction_bundle=bundle,
                pipeline_context=pipeline_context, subject_text=scraped_text_file.text, metadata=metadata, timestamp=timestamp,
            )
            if len(payloads) != len(wave_ids):
                raise ValueError(
                    f"unit screening: embedded request count ({len(wave_ids)}) does not match the "
                    f"computed packing ({len(payloads)}) for chunk {chunk_bounds} in "
                    f"{subject_unique_id}:{self.field_type.name}; the upstream state changed under "
                    f"the stored request ids (re-defer)."
                )
            payloads_by_req_id.update(zip(wave_ids, payloads))
        return await create_missing_unit_screening_requests(
            subject_unique_id=subject_unique_id, field_name=self.field_type.name,
            payloads_by_req_id=payloads_by_req_id, missing_req_ids=missing_request_ids,
            prompt=self.prompt, catalog=unit_screening_catalog_for(self.field_type.name),
            subject_name=self._subject_name_of(pipeline_context, subject_unique_id, self.field_type.name),
            deferred_at=timestamp, llm_model=stage_metadata.llm_model, model_params=stage_metadata.model_params, eager=eager,
        )

    @classmethod
    async def get_result(
        cls,
        subject_unique_id: str,
        field_type: ExtractionFieldType,
        chunk_bounds: str,
        extraction_bundle: LLMPhraseExtractionRequestBundle,
        completed_request_map: dict[BatchRequestIDType, GPTBatchRequest],
        timestamp: datetime,
    ) -> RecordScreeningResults:
        """This wave's verdicts for the chunk: record → candidate → verdict."""
        return await get_unit_screening_result(
            subject_unique_id=subject_unique_id, field_name=field_type.name,
            catalog=unit_screening_catalog_for(field_type.name),
            req_ids=extraction_bundle.llm_phrase_unit_screening_req_ids.get(cls.WAVE, []),
            completed_request_map=completed_request_map, timestamp=timestamp,
        )

    async def validate_own_responses(
        self,
        subject_unique_id: str,
        chunked_request_map: LLMPhraseExtractionRequestMap,
        completed_request_map: dict[BatchRequestIDType, GPTBatchRequest],
        timestamp: datetime,
    ) -> None:
        for chunk_bounds, bundle in chunked_request_map.items():
            await self.get_result(
                subject_unique_id=subject_unique_id, field_type=self.field_type, chunk_bounds=chunk_bounds,
                extraction_bundle=bundle, completed_request_map=completed_request_map, timestamp=timestamp,
            )

    async def dispatch_batch_request(
        self,
        gpt_batch_request: GPTBatchRequest,
        metadata: LLMPhraseExtractionMetadata,
    ) -> GPTBatchResponse:
        return await dispatch_gpt_batch_request(
            gpt_batch_request=gpt_batch_request, gpt_model=unit_screening_metadata_of(metadata).llm_model,
        )
