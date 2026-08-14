from __future__ import annotations
import logging
from typing import Optional
from datetime import datetime
from math import ceil

from llm_providers.db_models.gpt_batch_request import (
    GPTBatchRequest,
)
from llm_providers.models.open_ai.gpt_batch_response_blob import (
    GPTBatchResponse,
)
from core.models.deferred_extraction.deferred_concept_extraction import (
    ConceptExtractionRequestBundle,
    ConceptExtractionRequestMap,
    ConceptExtractionMetadata,
)
from core.models.extraction_schemas.grounding import (
    PhraseToTagAndRulesMap,
)
from llm_providers.models.file_objects.prompt import Prompt
from core.models.skos_concept import Concept
from core.models.field_types import (
    ConceptFieldType,
    ExtractionFieldType,
)
from core.models.pipeline_nodes.base.base_node import (
    PipelineContext,
)
from core.models.pipeline_nodes.base.base_reconcile_node import (
    ReconcileNode,
)
from core.models.pipeline_nodes.base.base_llm_extraction_node import (
    BaseLLMExtractionNode,
)
from llm_providers.field_types import BatchRequestIDType
from scraper.models.s3.scraped_text_file import ScrapedTextFile

from llm_providers.services.gpt_batch_request.gpt_batch_request_service import (
    dispatch_gpt_batch_request,
)
from core.services.pipeline_nodes.multi_stage.llm_initial_grounding_service import (
    create_missing_phrase_initial_grounding_requests,
    get_initial_grounding_result,
    get_verified_out_of_vocab_phrases_w_summary,
)

from core.utils.rdf_to_graph_util import (
    get_match_label_to_concept_map,
)
from typing import ClassVar
from core.models.pipeline_nodes.base.pipeline_stage import PipelineStage

logger = logging.getLogger(__name__)


class LLMPhraseInitialGroundingNode(
    BaseLLMExtractionNode[ConceptFieldType, PhraseToTagAndRulesMap]
):
    stage: ClassVar[PipelineStage] = PipelineStage.initial_grounding

    def __init__(
        self,
        field_type: ConceptFieldType,
        next_node: BaseLLMExtractionNode | ReconcileNode,
        phrase_initial_grounding_prompt: Prompt,
        known_concepts: set[Concept],
    ):
        super().__init__(
            field_type=field_type,
            next_node=next_node,
        )
        self.phrase_initial_grounding_prompt = phrase_initial_grounding_prompt
        self.known_concepts = known_concepts
        self.match_label_to_concept_map = get_match_label_to_concept_map(known_concepts)

    def get_upstream_phrase_relationship_map(
        self, pipeline_context: PipelineContext
    ) -> dict[BatchRequestIDType, GPTBatchRequest]:
        """Return the completed phrase-relationship request map from pipeline context."""
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement get_upstream_phrase_relationship_map"
        )

    def get_upstream_phrase_screening_map(
        self, pipeline_context: PipelineContext
    ) -> dict[BatchRequestIDType, GPTBatchRequest]:
        """Return the completed phrase-screening request map from pipeline context."""
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement get_upstream_phrase_screening_map"
        )

    async def embed_request_ids(  # prefill folded into this function
        self,
        subject_unique_id: str,
        pipeline_context: PipelineContext,
        metadata: ConceptExtractionMetadata,
        chunked_request_map: ConceptExtractionRequestMap,
        timestamp: datetime,
    ):
        if not chunked_request_map:
            raise ValueError(
                f"Cannot embed req ids for llm initial grounding node, "
                f"as chunked_request_map found empty for subject:{subject_unique_id}, field:{self.field_type.name}."
            )

        # The screened out-of-vocab phrase set for a chunk is already complete by
        # the time grounding embeds ids (relationship + screening have fully
        # executed), so the group count can be computed once, upfront.
        max_pairs_per_request = (
            metadata.llm_phrase_initial_grounding.max_pairs_per_request
        )
        upstream_relationship_map = self.get_upstream_phrase_relationship_map(
            pipeline_context
        )
        upstream_screening_map = self.get_upstream_phrase_screening_map(
            pipeline_context
        )

        for (
            chunk_bounds,
            extraction_request_bundle,
        ) in chunked_request_map.items():
            if extraction_request_bundle.llm_phrase_initial_grounding_req_ids:
                continue  # already embedded; group count is stable once computed

            verified_out_of_vocab_phrases_w_summary = (
                await get_verified_out_of_vocab_phrases_w_summary(
                    subject_unique_id=subject_unique_id,
                    field_type=self.field_type,
                    chunk_bounds=chunk_bounds,
                    extraction_bundle=extraction_request_bundle,
                    llm_phrase_relationship_gpt_request_map=upstream_relationship_map,
                    llm_phrase_screening_gpt_request_map=upstream_screening_map,
                    timestamp=timestamp,
                )
            )
            num_groups = max(
                1,
                ceil(
                    len(verified_out_of_vocab_phrases_w_summary) / max_pairs_per_request
                ),
            )
            extraction_request_bundle.llm_phrase_initial_grounding_req_ids = [
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
        chunked_request_map: ConceptExtractionRequestMap,
    ) -> set[BatchRequestIDType]:
        llm_phrase_initial_grounding_req_ids: set[BatchRequestIDType] = set()
        for (
            chunk_bounds,
            extraction_bundle,
        ) in chunked_request_map.items():
            if not extraction_bundle.llm_phrase_initial_grounding_req_ids:
                raise ValueError(
                    f"Cannot get embedded request ids for subject_unique_id:{subject_unique_id}>{chunk_bounds} as "
                    f"llm_phrase_initial_grounding_req_ids is empty in the extraction_bundle."
                )

            llm_phrase_initial_grounding_req_ids.update(
                extraction_bundle.llm_phrase_initial_grounding_req_ids
            )

        return llm_phrase_initial_grounding_req_ids

    @staticmethod
    def get_request_custom_id(
        subject_unique_id: str,
        field_type: ExtractionFieldType,
        chunk_bounds: str,
        group_index: int,
        metadata: ConceptExtractionMetadata,
    ) -> BatchRequestIDType:
        return (
            f"{subject_unique_id}>{field_type.name}>llm_phrase_initial_grounding>group>{group_index}>chunk>{chunk_bounds}>"
            f"{metadata.llm_phrase_initial_grounding.to_custom_id_segment()}"
        )

    async def create_batch_requests(
        self,
        subject_unique_id: str,
        scraped_text_file: ScrapedTextFile,
        missing_request_ids: set[BatchRequestIDType],
        metadata: ConceptExtractionMetadata,
        chunked_request_map: ConceptExtractionRequestMap,
        pipeline_context: PipelineContext,
        timestamp: datetime,
        eager: bool,
    ) -> list[GPTBatchRequest]:

        subject_name = pipeline_context.subject_name
        if not subject_name:
            raise ValueError(
                f"phrase_relationship_node.create_batch_requests was called for {self.field_type.name} in {self.__class__.__name__} but pipeline_context.subject_name is not set. Ensure business_desc is extracted before phrase_relationship."
            )

        # create_missing_phrase_relationship_requests only creates batch requests fresh or only missing ones,
        # for e.g., new subject or some batch requests failed earlier and were deleted to allow re-processing
        batch_requests = await create_missing_phrase_initial_grounding_requests(
            deferred_at=timestamp,
            subject_unique_id=subject_unique_id,
            subject_name=subject_name,
            field_type=self.field_type,
            missing_phrase_initial_grounding_req_ids=missing_request_ids,
            chunked_request_map=chunked_request_map,
            phrase_initial_grounding_prompt=self.phrase_initial_grounding_prompt,
            llm_phrase_relationship_gpt_request_map=self.get_upstream_phrase_relationship_map(
                pipeline_context
            ),
            llm_phrase_screening_gpt_request_map=self.get_upstream_phrase_screening_map(
                pipeline_context
            ),
            known_concepts=self.known_concepts,
            match_label_to_concept_map=self.match_label_to_concept_map,
            llm_model=metadata.llm_phrase_initial_grounding.llm_model,
            model_params=metadata.llm_phrase_initial_grounding.model_params,
            max_pairs_per_request=metadata.llm_phrase_initial_grounding.max_pairs_per_request,
            eager=eager,
        )

        return batch_requests

    @staticmethod
    async def get_result(
        subject_unique_id: str,
        field_type: ConceptFieldType,
        chunk_bounds: str,
        extraction_bundle: ConceptExtractionRequestBundle,
        completed_request_map: dict[BatchRequestIDType, GPTBatchRequest],
        timestamp: datetime,  # for recording errors
        repairs: Optional[dict[str, str]] = None,
    ) -> PhraseToTagAndRulesMap:
        return await get_initial_grounding_result(
            subject_unique_id=subject_unique_id,
            field_type=field_type,
            chunk_bounds=chunk_bounds,
            extraction_bundle=extraction_bundle,
            completed_request_map=completed_request_map,
            timestamp=timestamp,
            repairs=repairs,
        )

    async def dispatch_batch_request(
        self,
        gpt_batch_request: GPTBatchRequest,
        metadata: ConceptExtractionMetadata,
    ) -> GPTBatchResponse:
        return await dispatch_gpt_batch_request(
            gpt_batch_request=gpt_batch_request,
            gpt_model=metadata.llm_phrase_initial_grounding.llm_model,
            model_params=metadata.llm_phrase_initial_grounding.model_params,
        )
