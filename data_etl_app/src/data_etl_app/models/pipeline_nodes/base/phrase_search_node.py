import logging
import traceback
from datetime import datetime
from typing import Optional

from core.models.db.deferred_manufacturer import DeferredManufacturer
from core.models.db.gpt_batch_request import GPTBatchRequest
from core.models.llm_model import LLM_Model
from core.models.prompt import Prompt
from core.models.deferred_phrase_extraction_requests import (
    DeferredLLMPhraseExtractionRequests,
    LLMPhraseExtractionRequestBundle,
)
from core.models.deferred_concept_extraction import (
    DeferredConceptExtractionRequests,
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
from open_ai_key_app.models.gpt_model_params import GPTModelParams
from open_ai_key_app.models.field_types import GPTBatchRequestCustomID
from scraper_app.models.scraped_text_file import ScrapedTextFile

from core.services.gpt_batch_request_writes import record_response_parse_error
from data_etl_app.services.extraction.deferred_llm_phrase_search_node_service import (
    parse_llm_search_response,
)
from data_etl_app.services.extraction.deferred_llm_phrase_search_node_service import (
    create_missing_phrase_search_requests,
)

logger = logging.getLogger(__name__)


class LLMPhraseSearchNode(BaseLLMExtractionNode[LLMExtractedFieldTypeVar, set[str]]):
    """Phase 1: LLM Search for terms related to field type."""

    def __init__(
        self,
        field_type: LLMExtractedFieldTypeVar,
        next_node: BaseLLMExtractionNode | ReconcileNode,
        phrase_search_prompt: Prompt,
        llm_model: LLM_Model,
        model_params: GPTModelParams,
    ):
        super().__init__(
            field_type=field_type,
            next_node=next_node,
            llm_model=llm_model,
            model_params=model_params,
        )
        self.phrase_search_prompt = phrase_search_prompt

    def get_embedded_request_ids(
        self,
        mfg_etld1: str,
        extraction_requests: DeferredLLMPhraseExtractionRequests,
    ) -> set[GPTBatchRequestCustomID]:
        llm_search_req_ids: set[GPTBatchRequestCustomID] = set()
        for (
            chunk_bounds,
            extraction_bundle,
        ) in extraction_requests.chunked_request_map.items():
            llm_phrase_search_request_id = extraction_bundle.llm_phrase_search_req_id
            if not llm_phrase_search_request_id:
                raise ValueError(
                    f"get_search_request_ids was called for {mfg_etld1}:{self.field_type.name} but llm_phrase_search_request_id is None for chunk bounds {chunk_bounds}."
                )
            llm_search_req_ids.add(llm_phrase_search_request_id)
        return llm_search_req_ids

    @staticmethod
    def get_request_custom_id(
        mfg_etld1: str,
        field_type: LLMExtractedFieldTypeEnum,
        chunk_bounds: str,
        llm_model: LLM_Model,
        model_params: GPTModelParams,
    ) -> GPTBatchRequestCustomID:
        return f"{mfg_etld1}>{field_type.name}>llm_search>chunk>{chunk_bounds}>{model_params.to_custom_id_segment(llm_model.name)}"

    @staticmethod
    async def parse_batch_request_result(
        mfg_etld1: str,
        field_type: LLMExtractedFieldTypeEnum,
        chunk_bounds: str,
        extraction_bundle: LLMPhraseExtractionRequestBundle,
        completed_request_map: dict[GPTBatchRequestCustomID, GPTBatchRequest],
        deferred_at: datetime,
    ) -> set[str]:
        llm_phrase_search_request_id = extraction_bundle.llm_phrase_search_req_id
        if not llm_phrase_search_request_id:
            raise ValueError(
                f"search_node.parse_batch_request_result: llm_phrase_search_request_id is None for chunk bounds {chunk_bounds} in {mfg_etld1}:{field_type.name}"
            )

        llm_search_req = completed_request_map.get(llm_phrase_search_request_id)
        if not llm_search_req:
            raise ValueError(
                f"search_node.parse_batch_request_result: Missing GPTBatchRequest for search request ID {llm_phrase_search_request_id} in {mfg_etld1}:{field_type.name}"
            )
        elif not llm_search_req.response:
            raise ValueError(
                f"search_node.parse_batch_request_result: GPTBatchRequest for search request ID {llm_phrase_search_request_id} has no response_blob in {mfg_etld1}:{field_type.name}"
            )

        try:
            llm_search_results = parse_llm_search_response(
                llm_search_req.response.result
            )
            return llm_search_results
        except Exception as e:
            await record_response_parse_error(
                gpt_batch_request=llm_search_req,
                error_message=str(e),
                timestamp=deferred_at,
                traceback_str=traceback.format_exc(),
            )
            logger.error(
                f"search_node.parse_batch_request_result: Error parsing concept search results for manufacturer {mfg_etld1} from GPT response: {e}"
            )
            raise

    async def create_batch_requests(
        self,
        missing_request_ids: set[GPTBatchRequestCustomID],
        deferred_mfg: DeferredManufacturer,
        scraped_text_file: ScrapedTextFile,
        timestamp: datetime,
        pipeline_context: PipelineContext,
        llm_model: LLM_Model,
        model_params: GPTModelParams,
        eager: bool,
    ) -> list[GPTBatchRequest]:
        """Create batch requests for concept search phase."""

        extraction_requests: Optional[DeferredConceptExtractionRequests] = getattr(
            deferred_mfg, self.field_type.name
        )
        if not extraction_requests:
            raise ValueError(
                f"create_batch_requests was called for {self.field_type.name} in {__class__.__name__} but no deferred concept extraction exists."
            )

        # create_missing_concept_search_requests only creates batch requests fresh or only missing ones,
        # for e.g., new mfg or some batch requests failed earlier and were deleted to allow re-processing
        batch_requests = await create_missing_phrase_search_requests(
            deferred_at=timestamp,
            field_type=self.field_type,
            missing_search_req_ids=missing_request_ids,
            chunked_request_map=extraction_requests.chunked_request_map,
            mfg_etld1=deferred_mfg.etld1,
            mfg_text=scraped_text_file.text,
            search_prompt=self.phrase_search_prompt,
            llm_model=llm_model,
            model_params=model_params,
            eager=eager,
        )

        return batch_requests
