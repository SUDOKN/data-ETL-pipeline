from __future__ import annotations

import logging
from abc import abstractmethod
from datetime import datetime
from typing import TYPE_CHECKING, Optional

from packages.llm_providers.src.llm_providers.db_models.gpt_batch_request import (
    GPTBatchRequest,
)
from packages.llm_providers.src.llm_providers.models.open_ai.gpt_batch_response_blob import (
    GPTBatchResponse,
)
from packages.llm_providers.src.llm_providers.models.file_objects.prompt import Prompt
from packages.core.src.core.models.extraction_results.single_stage_extraction_results import (
    LLMSingleStageExtractionMetadata,
)
from packages.core.src.core.models.deferred_extraction.deferred_single_stage_extraction_requests import (
    SingleStageExtractionRequestMap,
    SingleStageExtractionRequestBundle,
    DeferredSingleStageExtractionRequests,
)
from packages.core.src.core.models.pipeline_nodes.base.base_reconcile_node import (
    ReconcileNode,
)
from packages.core.src.core.models.types_and_enums import (
    BasicFieldTypeEnum,
    BinaryClassificationTypeEnum,
    SingleStageFieldTypeEnum,
)
from packages.core.src.core.models.pipeline_nodes.base.base_node import (
    PipelineContext,
)
from packages.core.src.core.models.pipeline_nodes.base.base_llm_extraction_node import (
    BaseLLMExtractionNode,
    ResultT,
)
from packages.llm_providers.src.llm_providers.field_types import BatchRequestIDType

if TYPE_CHECKING:
    from packages.infra.src.infra.models.s3.scraped_text_file import (
        ScrapedTextFile,
    )

from packages.llm_providers.src.llm_providers.services.gpt_batch_request.gpt_batch_request_service import (
    dispatch_gpt_batch_request,
)

from packages.core.src.core.services.pipeline_nodes.single_stage.llm_basic_field_extraction_service import (
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
        subject_unique_id: str,
        pipeline_context: PipelineContext,
        metadata: LLMSingleStageExtractionMetadata,
        chunked_request_map: SingleStageExtractionRequestMap,
        timestamp: datetime,
    ):
        if not chunked_request_map:
            raise ValueError(
                f"Cannot embed req ids for llm phrase search node, "
                f"as chunked_request_map found empty for mfg:{subject_unique_id}, field:{self.field_type.name}."
            )

        for (
            chunk_bounds,
            extraction_request_bundle,
        ) in chunked_request_map.items():
            if not extraction_request_bundle.llm_request_id:
                extraction_request_bundle.llm_request_id = self.get_request_custom_id(
                    subject_unique_id=subject_unique_id,
                    field_type=self.field_type,
                    chunk_bounds=chunk_bounds,
                    metadata=metadata,
                )

    def get_embedded_request_ids(
        self,
        subject_unique_id: str,
        chunked_request_map: SingleStageExtractionRequestMap,
    ) -> set[BatchRequestIDType]:
        all_llm_req_ids: set[BatchRequestIDType] = set()
        for (
            _chunk_bounds,
            extraction_bundle,
        ) in chunked_request_map.items():
            if not extraction_bundle.llm_request_id:
                raise ValueError(
                    f"get_embedded_request_ids was called for {subject_unique_id}:{self.field_type.name} but llm_request_id is None for chunk bounds {_chunk_bounds}."
                )
            all_llm_req_ids.add(extraction_bundle.llm_request_id)
        return all_llm_req_ids

    @staticmethod
    def get_request_custom_id(
        subject_unique_id: str,
        field_type: "BasicFieldTypeEnum | BinaryClassificationTypeEnum",
        chunk_bounds: str,
        metadata: LLMSingleStageExtractionMetadata,
    ) -> BatchRequestIDType:
        return (
            f"{subject_unique_id}>{field_type.name}>llm_request>chunk>{chunk_bounds}>"
            f"{metadata.single_stage.model_params.to_custom_id_segment(metadata.single_stage.llm_model.name)}"
        )

    async def create_batch_requests(
        self,
        subject_unique_id: str,
        scraped_text_file: ScrapedTextFile,
        missing_request_ids: set[BatchRequestIDType],
        metadata: LLMSingleStageExtractionMetadata,
        chunked_request_map: SingleStageExtractionRequestMap,
        pipeline_context: PipelineContext,
        timestamp: datetime,
        eager: bool,
    ) -> list[GPTBatchRequest]:
        """Create batch requests for business description extraction phase."""

        batch_requests = await create_missing_basic_extraction_requests(
            deferred_at=timestamp,
            subject_unique_id=subject_unique_id,
            mfg_text=scraped_text_file.text,
            field_type=self.field_type,
            chunked_request_map=chunked_request_map,
            missing_request_ids=missing_request_ids,
            prompt=self.prompt,
            llm_model=metadata.single_stage.llm_model,
            model_params=metadata.single_stage.model_params,
            eager=eager,
        )

        logger.info(
            f"create_batch_requests: Created {len(batch_requests)} GPTBatchRequest for {subject_unique_id}:{self.field_type.name}"
        )
        logger.info(f"{batch_requests}")

        return batch_requests

    @staticmethod
    @abstractmethod
    async def get_result(
        subject_unique_id: str,
        field_type: "BasicFieldTypeEnum | BinaryClassificationTypeEnum",
        chunk_bounds: str,
        extraction_bundle: SingleStageExtractionRequestBundle,
        completed_request_map: dict[BatchRequestIDType, GPTBatchRequest],
        timestamp: datetime,  # for recording errors
    ) -> ResultT:
        pass

    async def dispatch_batch_request(
        self,
        gpt_batch_request: GPTBatchRequest,
        metadata: LLMSingleStageExtractionMetadata,
    ) -> GPTBatchResponse:
        return await dispatch_gpt_batch_request(
            gpt_batch_request=gpt_batch_request,
            gpt_model=metadata.single_stage.llm_model,
            model_params=metadata.single_stage.model_params,
        )
