"""The Step 2 proposal pass (design doc §54.2, user decision 2026-09-21; the
tryout's arm ``p``): the records the one grounding call left with NO label —
declined, or still unanswered after its retry — read again under the
``phrase_proposal`` catalog, whose job paragraph says what this reading is
for: match after all at the most specific option the words support, propose
what the vocabulary lacks, or confirm the subject is none.

A run flag: ``metadata.llm_phrase_proposal`` None = the pass is OFF, and the
node embeds no ids at all (the OOV pass's convention — zero ids read as a
complete stage with an empty result). Everything else — the request layout
with the vocabulary in the system text, the group cap, the ``|ud=`` digest,
the under-answer retry, the structural parse with the vocabulary hold — is
the grounding node's, through its hooks. What it stores is the pass's own
trail (``llm_phrase_proposal`` in the stats); the reconcile step merges its
proposals into the out-of-vocabulary bucket and its matches into the
vocabulary one.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, ClassVar, Optional

from llm_providers.db_models.gpt_batch_request import GPTBatchRequest
from llm_providers.field_types import BatchRequestIDType

from core.models.deferred_extraction.deferred_concept_extraction import (
    ConceptExtractionRequestBundle,
)
from core.models.extraction_results.concept_extraction_results import (
    BatchedInitialGroundingNodeMetadata,
)
from core.models.extraction_results.llm_phrase_extraction_results import (
    ConceptExtractionMetadata,
)
from core.models.field_types import ExtractionFieldType
from core.models.pipeline_nodes.base.base_node import PipelineContext
from core.models.pipeline_nodes.base.pipeline_stage import (
    STAGE_REQUEST_ID_TOKEN,
    PipelineStage,
)
from core.models.pipeline_nodes.multi_stage.base.llm_phrase_grounding_node import (
    LLMPhraseGroundingNode,
)
from core.models.rule_catalog import STAGE_PROPOSAL
from core.services.pipeline_nodes.multi_stage.llm_grounding_node_service import (
    build_group_record_payloads,
    subject_keyed_payloads,
)
from core.services.pipeline_nodes.multi_stage.llm_phrase_synthesis_node_service import (
    get_chunk_group_records,
)
from core.utils.request_custom_id_util import upstream_digest_segment

logger = logging.getLogger(__name__)


class LLMPhraseProposalNode(LLMPhraseGroundingNode):
    stage: ClassVar[PipelineStage] = PipelineStage.proposal
    STAGE_LABEL: ClassVar[str] = "proposal"
    CATALOG_STAGE: ClassVar[str] = STAGE_PROPOSAL
    DUMMY_NOTE: ClassVar[str] = (
        "No proposal pass needed - the grounding call left no record of this chunk without a label."
    )
    EMPTY_IDS_ARE_A_COMPLETE_STAGE: ClassVar[bool] = True

    def get_upstream_grounding_map(
        self, pipeline_context: PipelineContext
    ) -> dict[BatchRequestIDType, GPTBatchRequest]:
        """The completed grounding request map — the labels each record got
        are read from its held answers."""
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement get_upstream_grounding_map"
        )

    @classmethod
    def stage_metadata(
        cls, metadata: ConceptExtractionMetadata
    ) -> Optional[BatchedInitialGroundingNodeMetadata]:
        return metadata.llm_phrase_proposal

    @staticmethod
    def _req_ids(bundle: ConceptExtractionRequestBundle) -> list[BatchRequestIDType]:
        return bundle.llm_phrase_proposal_req_ids

    @staticmethod
    def _set_req_ids(bundle: ConceptExtractionRequestBundle, ids: list[BatchRequestIDType]) -> None:
        bundle.llm_phrase_proposal_req_ids = ids

    @staticmethod
    def _retry_record_ids(bundle: ConceptExtractionRequestBundle) -> Optional[list[str]]:
        return bundle.llm_phrase_proposal_retry_record_ids

    @staticmethod
    def _set_retry_record_ids(bundle: ConceptExtractionRequestBundle, ids: list[str]) -> None:
        bundle.llm_phrase_proposal_retry_record_ids = ids

    @staticmethod
    def _retry_req_ids(bundle: ConceptExtractionRequestBundle) -> list[BatchRequestIDType]:
        return bundle.llm_phrase_proposal_retry_req_ids

    @staticmethod
    def _set_retry_req_ids(bundle: ConceptExtractionRequestBundle, ids: list[BatchRequestIDType]) -> None:
        bundle.llm_phrase_proposal_retry_req_ids = ids

    async def _chunk_record_payloads(
        self,
        subject_unique_id: str,
        chunk_bounds: str,
        extraction_bundle: ConceptExtractionRequestBundle,
        synthesis_map: dict[BatchRequestIDType, GPTBatchRequest],
        subject_text: str,
        metadata: ConceptExtractionMetadata,
        timestamp: datetime,
    ) -> dict[str, dict[str, Any]]:
        """The chunk's group records the grounding call left with NO label:
        those it declined (an entry with no tags) and those still unanswered
        after its retry (absent from its result). Subject-keyed, like the
        grounding request's; the ``|ud=`` digest is over exactly this set, so
        a change in what grounding labelled re-asks the pass."""
        group_records = await get_chunk_group_records(
            subject_unique_id,
            self.field_type,
            chunk_bounds,
            extraction_bundle,
            timestamp,
            synthesis_completed_request_map=synthesis_map,
            subject_text=subject_text,
            metadata=metadata,
        )
        if self._grounding_map is None:
            raise ValueError(
                f"{self.__class__.__name__}: the grounding map is read once per embed / "
                f"create call; _chunk_record_payloads was called outside one"
            )
        grounding = await LLMPhraseGroundingNode.get_result(
            subject_unique_id=subject_unique_id,
            field_type=self.field_type,
            chunk_bounds=chunk_bounds,
            extraction_bundle=extraction_bundle,
            completed_request_map=self._grounding_map,
            timestamp=timestamp,
            allowed_labels=self.allowed_labels(),
        )
        labelled = {rid for rid, entry in grounding.items() if entry.tags}
        payloads = build_group_record_payloads(group_records)
        unlabelled = {rid: payload for rid, payload in payloads.items() if rid not in labelled}
        return subject_keyed_payloads(unlabelled)

    # The base's embed / create call ``_chunk_record_payloads`` without the
    # grounding map; these two wrap them so the map is read once per call.

    async def embed_request_ids(  # prefill folded into this function
        self,
        subject_unique_id: str,
        pipeline_context: PipelineContext,
        metadata: ConceptExtractionMetadata,
        chunked_request_map: dict[str, ConceptExtractionRequestBundle],
        timestamp: datetime,
    ):
        if self.stage_metadata(metadata) is None:
            return  # OFF for this run
        self._grounding_map = self.get_upstream_grounding_map(pipeline_context)
        try:
            await super().embed_request_ids(
                subject_unique_id=subject_unique_id,
                pipeline_context=pipeline_context,
                metadata=metadata,
                chunked_request_map=chunked_request_map,
                timestamp=timestamp,
            )
        finally:
            self._grounding_map = None

    async def create_batch_requests(
        self,
        subject_unique_id: str,
        scraped_text_file,
        missing_request_ids: set[BatchRequestIDType],
        metadata: ConceptExtractionMetadata,
        chunked_request_map: dict[str, ConceptExtractionRequestBundle],
        pipeline_context: PipelineContext,
        timestamp: datetime,
        eager: bool,
    ) -> list[GPTBatchRequest]:
        self._grounding_map = self.get_upstream_grounding_map(pipeline_context)
        try:
            return await super().create_batch_requests(
                subject_unique_id=subject_unique_id,
                scraped_text_file=scraped_text_file,
                missing_request_ids=missing_request_ids,
                metadata=metadata,
                chunked_request_map=chunked_request_map,
                pipeline_context=pipeline_context,
                timestamp=timestamp,
                eager=eager,
            )
        finally:
            self._grounding_map = None

    _grounding_map: Optional[dict[BatchRequestIDType, GPTBatchRequest]] = None

    @staticmethod
    def get_request_custom_id(
        subject_unique_id: str,
        field_type: ExtractionFieldType,
        chunk_bounds: str,
        group_index: int,
        metadata: ConceptExtractionMetadata,
        group_payload: dict[str, dict[str, Any]],
        retry_index: int | None = None,
    ) -> BatchRequestIDType:
        node = metadata.llm_phrase_proposal
        if node is None:
            raise ValueError(
                "get_request_custom_id was called for the proposal pass while its "
                "metadata node is None; an OFF pass embeds no ids at all."
            )
        retry = f"retry>{retry_index}>" if retry_index is not None else ""
        return (
            f"{subject_unique_id}>{field_type.name}"
            f">{STAGE_REQUEST_ID_TOKEN[PipelineStage.proposal]}"
            f">{retry}group>{group_index}>chunk>{chunk_bounds}>"
            f"{node.to_custom_id_segment()}"
            f"{upstream_digest_segment(group_payload)}"
        )
