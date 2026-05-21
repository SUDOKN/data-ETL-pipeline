import logging
import traceback
from datetime import datetime
from typing import Optional

from core.models.db.deferred_manufacturer import DeferredManufacturer
from core.models.db.gpt_batch_request import GPTBatchRequest
from core.models.deferred_concept_extraction import (
    DeferredConceptExtractionRequests,
    ConceptExtractionRequestBundle,
)
from core.models.prompt import Prompt
from data_etl_app.models.pipeline_nodes.concept.concept_evidence_node import (
    ConceptEvidenceNode,
)
from data_etl_app.models.pipeline_nodes.concept.concept_reconcile_node import (
    ConceptReconcileNode,
)
from data_etl_app.models.types_and_enums import ConceptTypeEnum, PipelineContext
from data_etl_app.models.skos_concept import Concept
from data_etl_app.models.pipeline_nodes.llm_extraction_node import (
    LLMExtractionNode,
)
from open_ai_key_app.models.field_types import GPTBatchRequestCustomID
from open_ai_key_app.models.gpt_model import (
    LLM_Model,
)
from scraper_app.models.scraped_text_file import ScrapedTextFile

from core.services.gpt_batch_request_writes import record_response_parse_error
from data_etl_app.services.extraction.deferred_concept_mapping_service import (
    create_missing_mapping_requests,
    parse_llm_mapping_result,
)

logger = logging.getLogger(__name__)


class ConceptMappingNode(LLMExtractionNode[ConceptTypeEnum, dict[str, str]]):
    """Phase 3: Map unknown terms to known ontology"""

    def __init__(
        self,
        concept_type: ConceptTypeEnum,
        mapping_prompt: Prompt,
        llm_model: LLM_Model,
        known_concepts: set[Concept],
        next_node: ConceptReconcileNode,
    ):
        super().__init__(field_type=concept_type, next_node=next_node)
        self.mapping_prompt = mapping_prompt
        self.llm_model = llm_model
        self.known_concepts = known_concepts

    def get_embedded_request_ids(
        self,
        mfg_etld1: str,
        extraction_requests: DeferredConceptExtractionRequests,
    ) -> set[GPTBatchRequestCustomID]:
        llm_mapping_req_ids: set[GPTBatchRequestCustomID] = set()
        for (
            _chunk_bounds,
            extraction_bundle,
        ) in extraction_requests.request_map.items():
            if not extraction_bundle.llm_mapping_request_id:
                raise ValueError(
                    f"get_embedded_request_ids was called for {mfg_etld1}:{self.field_type.name} but llm_mapping_request_id is None for chunk bounds {_chunk_bounds}."
                )
            llm_mapping_req_ids.add(extraction_bundle.llm_mapping_request_id)
        return llm_mapping_req_ids

    @staticmethod
    def get_request_custom_id(
        mfg_etld1: str, field_type: ConceptTypeEnum, chunk_bounds: str
    ) -> GPTBatchRequestCustomID:
        return f"{mfg_etld1}>{field_type.name}>llm_mapping>chunk>{chunk_bounds}"

    @staticmethod
    async def parse_batch_request_result(
        mfg_etld1: str,
        field_type: ConceptTypeEnum,
        chunk_bounds: str,
        extraction_bundle: ConceptExtractionRequestBundle,
        completed_request_map: dict[GPTBatchRequestCustomID, GPTBatchRequest],
        deferred_at: datetime,
    ) -> dict[str, str]:
        llm_mapping_request_id = extraction_bundle.llm_mapping_request_id
        if not llm_mapping_request_id:
            raise ValueError(
                f"concept_mapping_node.get_batch_request_result: llm_mapping_request_id is None for chunk bounds {chunk_bounds} in {mfg_etld1}:{field_type.name}"
            )

        llm_mapping_req = completed_request_map.get(llm_mapping_request_id)
        if not llm_mapping_req:
            raise ValueError(
                f"concept_mapping_node.get_batch_request_result: Missing GPTBatchRequest for mapping request ID {llm_mapping_request_id} in {mfg_etld1}:{field_type.name}"
            )
        elif not llm_mapping_req.response_blob:
            raise ValueError(
                f"concept_mapping_node.get_batch_request_result: GPTBatchRequest for mapping request ID {llm_mapping_request_id} has no response_blob in {mfg_etld1}:{field_type.name}"
            )

        try:
            llm_mapping_results = parse_llm_mapping_result(
                llm_mapping_req.response_blob.result
            )
            return llm_mapping_results
        except Exception as e:
            await record_response_parse_error(
                gpt_batch_request=llm_mapping_req,
                error_message=str(e),
                timestamp=deferred_at,
                traceback_str=traceback.format_exc(),
            )
            logger.error(
                f"concept_mapping_node.get_batch_request_result: Error parsing concept mapping results for manufacturer {mfg_etld1} from GPT response: {e}"
            )
            raise

    async def create_batch_requests(
        self,
        missing_request_ids: set[GPTBatchRequestCustomID],
        deferred_mfg: DeferredManufacturer,
        scraped_text_file: ScrapedTextFile,
        timestamp: datetime,
        pipeline_context: PipelineContext,
    ) -> list[GPTBatchRequest]:
        """Create batch requests for concept mapping phase."""

        extraction_requests: Optional[DeferredConceptExtractionRequests] = getattr(
            deferred_mfg, self.field_type.name
        )
        if not extraction_requests:
            raise ValueError(
                f"create_batch_requests was called for {self.field_type.name} but no concept extraction requests exist."
            )

        batch_requests = await create_missing_mapping_requests(
            deferred_at=timestamp,
            mfg_etld1=deferred_mfg.mfg_etld1,
            concept_type=self.field_type,
            extraction_requests=extraction_requests,
            missing_mapping_req_ids=missing_request_ids,
            known_concepts=self.known_concepts,
            mapping_prompt=self.mapping_prompt,
            upstream_completed_batch_req_map=pipeline_context[ConceptEvidenceNode],
            llm_model=self.llm_model,
        )

        return batch_requests
