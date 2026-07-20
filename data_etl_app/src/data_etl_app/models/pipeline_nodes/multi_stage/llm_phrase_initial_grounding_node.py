from __future__ import annotations
import logging
from datetime import datetime

from core.models.db.gpt_batch_request import GPTBatchRequest
from core.models.batch_request_objects.gpt_batch_response_blob import GPTBatchResponse
from core.models.deferred_extraction.deferred_concept_extraction import (
    ConceptExtractionRequestBundle,
    ConceptExtractionRequestMap,
    ConceptExtractionMetadata,
)
from core.models.field_types import PhraseToTagAndReasonMap
from core.models.file_objects.prompt import Prompt
from data_etl_app.models.skos_concept import Concept
from data_etl_app.models.types_and_enums import (
    ConceptTypeEnum,
    LLMExtractedFieldTypeEnum,
)
from data_etl_app.models.pipeline_nodes.base.base_node import (
    PipelineContext,
)
from data_etl_app.models.pipeline_nodes.base.base_reconcile_node import ReconcileNode
from data_etl_app.models.pipeline_nodes.base.base_llm_extraction_node import (
    BaseLLMExtractionNode,
)
from open_ai_key_app.models.field_types import GPTBatchRequestCustomID
from scraper_app.models.scraped_text_file import ScrapedTextFile

from core.services.gpt_batch_request_service import (
    dispatch_gpt_batch_request,
)
from data_etl_app.services.extraction.deferred_llm_initial_grounding_service import (
    create_missing_phrase_initial_grounding_requests,
    get_initial_grounding_result,
)

from data_etl_app.utils.rdf_to_graph_util import get_match_label_to_concept_map

logger = logging.getLogger(__name__)


class LLMPhraseInitialGroundingNode(
    BaseLLMExtractionNode[ConceptTypeEnum, PhraseToTagAndReasonMap]
):
    def __init__(
        self,
        field_type: ConceptTypeEnum,
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
    ) -> dict[GPTBatchRequestCustomID, GPTBatchRequest]:
        """Return the completed phrase-relationship request map from pipeline context."""
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement get_upstream_phrase_relationship_map"
        )

    def get_upstream_phrase_screening_map(
        self, pipeline_context: PipelineContext
    ) -> dict[GPTBatchRequestCustomID, GPTBatchRequest]:
        """Return the completed phrase-screening request map from pipeline context."""
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement get_upstream_phrase_screening_map"
        )

    async def embed_request_ids(  # prefill folded into this function
        self,
        mfg_etld1: str,
        pipeline_context: PipelineContext,
        metadata: ConceptExtractionMetadata,
        chunked_request_map: ConceptExtractionRequestMap,
        timestamp: datetime,
    ):
        if not chunked_request_map:
            raise ValueError(
                f"Cannot embed req ids for llm initial grounding node, "
                f"as chunked_request_map found empty for mfg:{mfg_etld1}, field:{self.field_type.name}."
            )

        for (
            chunk_bounds,
            extraction_request_bundle,
        ) in chunked_request_map.items():
            if not extraction_request_bundle.llm_phrase_initial_grounding_req_id:
                extraction_request_bundle.llm_phrase_initial_grounding_req_id = (
                    self.get_request_custom_id(
                        mfg_etld1=mfg_etld1,
                        field_type=self.field_type,
                        chunk_bounds=chunk_bounds,
                        metadata=metadata,
                    )
                )

    def get_embedded_request_ids(
        self,
        mfg_etld1: str,
        chunked_request_map: ConceptExtractionRequestMap,
    ) -> set[GPTBatchRequestCustomID]:
        llm_phrase_initial_grounding_req_ids: set[GPTBatchRequestCustomID] = set()
        for (
            chunk_bounds,
            extraction_bundle,
        ) in chunked_request_map.items():
            if not extraction_bundle.llm_phrase_initial_grounding_req_id:
                raise ValueError(
                    f"Cannot get embedded request ids for mfg_etld1:{mfg_etld1}>{chunk_bounds} as "
                    f"llm_phrase_initial_grounding_request is absent in the extraction_bundle."
                )

            llm_phrase_initial_grounding_req_ids.add(
                extraction_bundle.llm_phrase_initial_grounding_req_id
            )

        return llm_phrase_initial_grounding_req_ids

    @staticmethod
    def get_request_custom_id(
        mfg_etld1: str,
        field_type: LLMExtractedFieldTypeEnum,
        chunk_bounds: str,
        metadata: ConceptExtractionMetadata,
    ) -> GPTBatchRequestCustomID:
        return (
            f"{mfg_etld1}>{field_type.name}>llm_phrase_initial_grounding>chunk>{chunk_bounds}>"
            f"{metadata.llm_phrase_initial_grounding.model_params.to_custom_id_segment(metadata.llm_phrase_initial_grounding.llm_model.name)}"
        )

    async def create_batch_requests(
        self,
        mfg_etld1: str,
        scraped_text_file: ScrapedTextFile,
        missing_request_ids: set[GPTBatchRequestCustomID],
        metadata: ConceptExtractionMetadata,
        chunked_request_map: ConceptExtractionRequestMap,
        pipeline_context: PipelineContext,
        timestamp: datetime,
        eager: bool,
    ) -> list[GPTBatchRequest]:

        mfg_name = pipeline_context.mfg_name
        if not mfg_name:
            raise ValueError(
                f"phrase_relationship_node.create_batch_requests was called for {self.field_type.name} in {self.__class__.__name__} but pipeline_context.mfg_name is not set. Ensure business_desc is extracted before phrase_relationship."
            )

        # create_missing_phrase_relationship_requests only creates batch requests fresh or only missing ones,
        # for e.g., new mfg or some batch requests failed earlier and were deleted to allow re-processing
        batch_requests = await create_missing_phrase_initial_grounding_requests(
            deferred_at=timestamp,
            mfg_etld1=mfg_etld1,
            mfg_name=mfg_name,
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
            eager=eager,
        )

        return batch_requests

    @staticmethod
    async def get_result(
        mfg_etld1: str,
        field_type: LLMExtractedFieldTypeEnum,
        chunk_bounds: str,
        extraction_bundle: ConceptExtractionRequestBundle,
        completed_request_map: dict[GPTBatchRequestCustomID, GPTBatchRequest],
        timestamp: datetime,  # for recording errors
    ) -> PhraseToTagAndReasonMap:
        return await get_initial_grounding_result(
            mfg_etld1=mfg_etld1,
            field_type=field_type,
            chunk_bounds=chunk_bounds,
            extraction_bundle=extraction_bundle,
            completed_request_map=completed_request_map,
            timestamp=timestamp,
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
