from __future__ import annotations
import logging
from datetime import datetime
import traceback
from typing import TYPE_CHECKING, Optional

from core.models.db.deferred_manufacturer import DeferredManufacturer
from core.models.prompt import Prompt
from core.models.db.gpt_batch_request import GPTBatchRequest
from core.models.deferred_concept_extraction import (
    DeferredConceptExtractionRequests,
    ConceptExtractionRequestBundle,
)
from data_etl_app.models.types_and_enums import ConceptTypeEnum
from data_etl_app.models.pipeline_nodes.concept.concept_search_node import (
    ConceptSearchNode,
)

if TYPE_CHECKING:
    from data_etl_app.models.pipeline_nodes.concept.concept_mapping_node import (
        ConceptMappingNode,
    )

from data_etl_app.models.pipeline_nodes.base_node import PipelineContext
from data_etl_app.models.pipeline_nodes.llm_extraction_node import (
    LLMExtractionNode,
)
from open_ai_key_app.models.field_types import GPTBatchRequestCustomID
from litellm_proxy_app.models.llm_model import LLM_Model
from open_ai_key_app.models.gpt_model_params import GPTModelParams
from scraper_app.models.scraped_text_file import ScrapedTextFile

from data_etl_app.services.extraction.deferred_concept_evidence_service import (
    create_missing_concept_evidence_requests,
    parse_llm_concept_evidence_result,
)
from core.services.gpt_batch_request_writes import record_response_parse_error

logger = logging.getLogger(__name__)


class ConceptEvidenceNode(LLMExtractionNode[ConceptTypeEnum, dict[str, str]]):
    """Phase 2: LLM finds evidence in the text"""

    def __init__(
        self,
        concept_type: ConceptTypeEnum,
        evidence_prompt: Prompt,
        next_node: ConceptMappingNode,
    ):
        super().__init__(field_type=concept_type, next_node=next_node)
        self.evidence_prompt = evidence_prompt

    def get_embedded_request_ids(
        self,
        mfg_etld1: str,
        extraction_requests: DeferredConceptExtractionRequests,
    ) -> set[GPTBatchRequestCustomID]:
        llm_evidence_req_ids: set[GPTBatchRequestCustomID] = set()
        for (
            chunk_bounds,
            extraction_bundle,
        ) in extraction_requests.request_map.items():
            if not extraction_bundle.llm_evidence_request_id:
                raise ValueError(
                    f"get_embedded_request_ids was called for {mfg_etld1}:{self.field_type.name} but llm_evidence_request_id is None for chunk bounds {chunk_bounds}."
                )
            llm_evidence_req_ids.add(extraction_bundle.llm_evidence_request_id)
        return llm_evidence_req_ids

    @staticmethod
    def get_request_custom_id(
        mfg_etld1: str,
        field_type: ConceptTypeEnum,
        chunk_bounds: str,
        llm_model: LLM_Model,
        model_params: GPTModelParams,
    ) -> GPTBatchRequestCustomID:
        return f"{mfg_etld1}>{field_type.name}>llm_evidence>chunk>{chunk_bounds}>{model_params.to_custom_id_segment(llm_model.model_name)}"

    @staticmethod
    async def parse_batch_request_result(
        mfg_etld1: str,
        field_type: ConceptTypeEnum,
        chunk_bounds: str,
        extraction_bundle: ConceptExtractionRequestBundle,
        completed_request_map: dict[GPTBatchRequestCustomID, GPTBatchRequest],
        deferred_at: datetime,
    ) -> dict[str, str]:
        llm_evidence_request_id = extraction_bundle.llm_evidence_request_id
        if not llm_evidence_request_id:
            raise ValueError(
                f"concept_evidence_node.parse_batch_request_result: llm_evidence_request_id is None for chunk bounds {chunk_bounds} in {mfg_etld1}:{field_type.name}"
            )

        llm_evidence_req = completed_request_map.get(llm_evidence_request_id)
        if not llm_evidence_req:
            raise ValueError(
                f"concept_evidence_node.parse_batch_request_result: Missing GPTBatchRequest for evidence request ID {llm_evidence_request_id} in {mfg_etld1}:{field_type.name}"
            )
        elif not llm_evidence_req.response:
            raise ValueError(
                f"concept_evidence_node.parse_batch_request_result: GPTBatchRequest for evidence request ID {llm_evidence_request_id} has no response_blob in {mfg_etld1}:{field_type.name}"
            )

        try:
            llm_evidence_results = parse_llm_concept_evidence_result(
                llm_evidence_req.response.result
            )
            return llm_evidence_results
        except Exception as e:
            await record_response_parse_error(
                gpt_batch_request=llm_evidence_req,
                error_message=str(e),
                timestamp=deferred_at,
                traceback_str=traceback.format_exc(),
            )
            logger.error(
                f"concept_evidence_node.parse_batch_request_result: Error parsing concept evidence results for manufacturer {mfg_etld1} from GPT response: {e}"
            )
            raise

    async def create_batch_requests(
        self,
        missing_request_ids: set[GPTBatchRequestCustomID],
        deferred_mfg: DeferredManufacturer,
        scraped_text_file: ScrapedTextFile,
        timestamp: datetime,
        eager: bool,
        pipeline_context: PipelineContext,
        llm_model: LLM_Model,
        model_params: GPTModelParams,
    ) -> list[GPTBatchRequest]:
        """Create batch requests for concept evidence phase."""

        extraction_requests: Optional[DeferredConceptExtractionRequests] = getattr(
            deferred_mfg, self.field_type.name
        )
        if not extraction_requests:
            raise ValueError(
                f"create_batch_requests was called for {self.field_type.name} in {__class__.__name__} but no deferred concept extraction exists."
            )

        # create_missing_concept_evidence_requests only creates batch requests fresh or only missing ones,
        # for e.g., new mfg or some batch requests failed earlier and were deleted to allow re-processing
        batch_requests = await create_missing_concept_evidence_requests(
            deferred_at=timestamp,
            mfg_etld1=deferred_mfg.etld1,
            concept_type=self.field_type,
            missing_evidence_req_ids=missing_request_ids,
            extraction_requests=extraction_requests,
            mfg_text=scraped_text_file.text,
            evidence_prompt=self.evidence_prompt,
            upstream_completed_batch_req_map=pipeline_context[ConceptSearchNode],
            llm_model=llm_model,
            model_params=model_params,
            eager=eager,
        )

        return batch_requests
