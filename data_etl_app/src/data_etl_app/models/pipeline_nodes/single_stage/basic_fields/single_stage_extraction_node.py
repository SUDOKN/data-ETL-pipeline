import logging
from abc import abstractmethod
from datetime import datetime
from typing import Optional

from core.models.db.deferred_manufacturer import DeferredManufacturer
from core.models.db.gpt_batch_request import GPTBatchRequest
from core.models.prompt import Prompt
from core.models.single_stage_extraction_results import LLMSingleStageExtractionMetadata
from core.models.deferred_single_stage_extraction_requests import (
    SingleStageExtractionRequestMap,
    SingleStageExtractionRequestBundle,
    DeferredSingleStageExtractionRequests,
)
from data_etl_app.models.pipeline_nodes.base.base_reconcile_node import ReconcileNode
from data_etl_app.models.types_and_enums import (
    BasicFieldTypeEnum,
    BinaryClassificationTypeEnum,
    SingleStageFieldTypeEnum,
)
from data_etl_app.models.pipeline_nodes.base.base_node import (
    PipelineContext,
)
from data_etl_app.models.pipeline_nodes.base.base_llm_extraction_node import (
    BaseLLMExtractionNode,
    ResultT,
)
from open_ai_key_app.models.field_types import GPTBatchRequestCustomID
from scraper_app.models.scraped_text_file import ScrapedTextFile

from data_etl_app.services.extraction.deferred_basic_field_service import (
    create_missing_basic_extraction_requests,
)

logger = logging.getLogger(__name__)


class SingleStageExtractionNode(
    BaseLLMExtractionNode[SingleStageFieldTypeEnum, ResultT]
):
    def __init__(
        self,
        field_type: "BasicFieldTypeEnum | BinaryClassificationTypeEnum",
        next_node: ReconcileNode,
        prompt: Prompt,
    ):
        super().__init__(
            field_type=field_type,
            next_node=next_node,
        )
        self.prompt = prompt

    async def embed_request_ids(  # prefill folded into this function
        self,
        mfg_etld1: str,
        pipeline_context: PipelineContext,
        metadata: LLMSingleStageExtractionMetadata,
        request_map: SingleStageExtractionRequestMap,
        deferred_at: datetime,
    ):
        if not request_map:
            raise ValueError(
                f"Cannot embed req ids for llm phrase search node, "
                f"as request_map found empty for mfg:{mfg_etld1}, field:{self.field_type.name}."
            )

        for (
            chunk_bounds,
            extraction_request_bundle,
        ) in request_map.items():
            if not extraction_request_bundle.llm_request_id:
                extraction_request_bundle.llm_request_id = self.get_request_custom_id(
                    mfg_etld1=mfg_etld1,
                    field_type=self.field_type,
                    chunk_bounds=chunk_bounds,
                    metadata=metadata,
                )

    def get_embedded_request_ids(
        self,
        mfg_etld1: str,
        request_map: SingleStageExtractionRequestMap,
    ) -> set[GPTBatchRequestCustomID]:
        all_llm_req_ids: set[GPTBatchRequestCustomID] = set()
        for (
            _chunk_bounds,
            extraction_bundle,
        ) in request_map.items():
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
        metadata: LLMSingleStageExtractionMetadata,
    ) -> GPTBatchRequestCustomID:
        return (
            f"{mfg_etld1}>{field_type.name}>llm_request>chunk>{chunk_bounds}>"
            f"{metadata.single_stage.model_params.to_custom_id_segment(metadata.single_stage.llm_model.name)}"
        )

    @staticmethod
    @abstractmethod
    async def parse_batch_request_result(
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
        eager: bool,
    ) -> list[GPTBatchRequest]:
        """Create batch requests for business description extraction phase."""

        extraction_requests: Optional[DeferredSingleStageExtractionRequests] = getattr(
            deferred_mfg, self.field_type.name
        )
        if not extraction_requests:
            raise ValueError(
                f"create_batch_requests was called for {self.field_type.name} but no extraction requests exist."
            )
        metadata = extraction_requests.metadata.single_stage

        batch_requests = await create_missing_basic_extraction_requests(
            deferred_at=timestamp,
            mfg_etld1=deferred_mfg.etld1,
            mfg_text=scraped_text_file.text,
            field_type=self.field_type,
            extraction_requests=extraction_requests,
            missing_request_ids=missing_request_ids,
            prompt=self.prompt,
            llm_model=metadata.llm_model,
            model_params=metadata.model_params,
            eager=eager,
        )

        logger.info(
            f"create_batch_requests: Created {len(batch_requests)} GPTBatchRequest for {deferred_mfg.etld1}:{self.field_type.name}"
        )
        logger.info(f"{batch_requests}")

        return batch_requests
