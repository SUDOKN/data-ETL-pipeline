import logging
from abc import abstractmethod
from datetime import datetime
from typing import Optional

from core.models.deferred_single_stage_extraction_requests import (
    SingleStageExtractionRequestBundle,
    DeferredSingleStageExtractionRequests,
)
from core.models.prompt import Prompt
from core.models.db.deferred_manufacturer import DeferredManufacturer
from core.models.db.gpt_batch_request import GPTBatchRequest
from data_etl_app.models.pipeline_nodes.reconcile_node import ReconcileNode
from data_etl_app.models.types_and_enums import (
    BasicFieldTypeEnum,
    BinaryClassificationTypeEnum,
    SingleStageFieldTypeEnum,
    PipelineContext,
)
from data_etl_app.models.pipeline_nodes.llm_extraction_node import (
    LLMExtractionNode,
    ResultT,
)
from open_ai_key_app.models.gpt_model import LLM_Model
from open_ai_key_app.models.field_types import GPTBatchRequestCustomID
from scraper_app.models.scraped_text_file import ScrapedTextFile

from data_etl_app.services.extraction.deferred_basic_field_service import (
    create_missing_basic_extraction_requests,
)

logger = logging.getLogger(__name__)


class SingleStageExtractionNode(LLMExtractionNode[SingleStageFieldTypeEnum, ResultT]):
    def __init__(
        self,
        field_type: "BasicFieldTypeEnum | BinaryClassificationTypeEnum",
        prompt: Prompt,
        llm_model: LLM_Model,
        next_node: ReconcileNode,
    ):
        super().__init__(field_type=field_type, next_node=next_node)
        self.prompt = prompt
        self.llm_model = llm_model

    async def get_embedded_request_ids(
        self,
        mfg_etld1: str,
        extraction_requests: DeferredSingleStageExtractionRequests,
    ) -> set[GPTBatchRequestCustomID]:
        all_llm_req_ids: set[GPTBatchRequestCustomID] = set()
        for (
            _chunk_bounds,
            extraction_bundle,
        ) in extraction_requests.request_map.items():
            if not extraction_bundle.llm_request_id:
                raise ValueError(
                    f"get_embedded_request_ids was called for {mfg_etld1}:{self.field_type.name} but llm_request_id is None for chunk bounds {_chunk_bounds}."
                )
            all_llm_req_ids.add(extraction_bundle.llm_request_id)
        return all_llm_req_ids

    @staticmethod
    def get_request_custom_id(
        mfg_etld1: str,
        field_type: "BasicFieldTypeEnum | BinaryClassificationTypeEnum",
        chunk_bounds: str,
    ) -> GPTBatchRequestCustomID:
        return f"{mfg_etld1}>{field_type.name}>llm_request>chunk>{chunk_bounds}"

    @staticmethod
    @abstractmethod
    def parse_batch_request_result(
        mfg_etld1: str,
        field_type: "BasicFieldTypeEnum | BinaryClassificationTypeEnum",
        chunk_bounds: str,
        extraction_bundle: SingleStageExtractionRequestBundle,
        completed_request_map: dict[GPTBatchRequestCustomID, GPTBatchRequest],
        deferred_at: datetime,
    ) -> ResultT:
        pass

    async def create_batch_requests(
        self,
        missing_request_ids: set[GPTBatchRequestCustomID],
        deferred_mfg: DeferredManufacturer,
        scraped_text_file: ScrapedTextFile,
        timestamp: datetime,
        pipeline_context: PipelineContext,
    ) -> list[GPTBatchRequest]:
        """Create batch requests for business description extraction phase."""

        extraction_requests: Optional[DeferredSingleStageExtractionRequests] = getattr(
            deferred_mfg, self.field_type.name
        )
        if not extraction_requests:
            raise ValueError(
                f"create_batch_requests was called for {self.field_type.name} but no extraction requests exist."
            )

        batch_requests = await create_missing_basic_extraction_requests(
            deferred_at=timestamp,
            mfg_etld1=deferred_mfg.mfg_etld1,
            mfg_text=scraped_text_file.text,
            field_type=self.field_type,
            extraction_requests=extraction_requests,
            missing_request_ids=missing_request_ids,
            prompt=self.prompt,
            llm_model=self.llm_model,
        )

        return batch_requests
