from __future__ import annotations
from datetime import datetime
import logging
import traceback
from typing import TYPE_CHECKING

from data_etl_app.models.extraction_results.business_description_extraction_result import (
    BusinessDescription,
    BusinessDescResponse,
)
from core.models.deferred_extraction.deferred_single_stage_extraction_requests import (
    SingleStageExtractionRequestBundle,
)
from llm_providers.models.file_objects.prompt import Prompt
from llm_providers.db_models.gpt_batch_request import (
    GPTBatchRequest,
)
from core.models.pipeline_nodes.single_stage.base.single_stage_extraction_node import (
    SingleStageExtractionNode,
)
from core.models.field_types import ExtractionFieldType
from data_etl_app.models.types_and_enums import (
    BasicFieldTypeEnum,
)
from core.models.extraction_schemas.response_format_util import (
    build_gpt_response_format,
)
from llm_providers.field_types import BatchRequestIDType

if TYPE_CHECKING:
    from data_etl_app.models.pipeline_nodes.single_stage.basic_fields.business_desc_reconcile_node import (
        BusinessDescReconcileNode,
    )

from llm_providers.services.gpt_batch_request.gpt_batch_request_writes import (
    record_response_parse_error,
)
from data_etl_app.services.extraction.deferred_business_desc_service import (
    parse_business_desc_from_gpt_response,
)

logger = logging.getLogger(__name__)

BUSINESS_DESC_RESPONSE_SCHEMA = build_gpt_response_format(
    BusinessDescResponse, name="business_desc_extraction"
)


class BusinessDescExtractionNode(SingleStageExtractionNode[BusinessDescription]):
    def __init__(
        self,
        extract_prompt: Prompt,
        next_node: BusinessDescReconcileNode,
    ):
        super().__init__(
            field_type=BasicFieldTypeEnum.business_desc,
            prompt=extract_prompt,
            next_node=next_node,
        )

    def get_response_schema(self) -> dict:
        return BUSINESS_DESC_RESPONSE_SCHEMA

    @staticmethod
    async def get_result(
        subject_unique_id: str,
        field_type: ExtractionFieldType,
        chunk_bounds: str,
        extraction_bundle: SingleStageExtractionRequestBundle,
        completed_request_map: dict[BatchRequestIDType, GPTBatchRequest],
        timestamp: datetime,
    ) -> BusinessDescription:
        llm_business_desc_request_id = extraction_bundle.llm_request_id
        if not llm_business_desc_request_id:
            raise ValueError(
                f"business_desc_extraction_node.get_result: llm_request_id is None for chunk bounds {chunk_bounds} in {subject_unique_id}:{field_type.name}"
            )

        llm_business_desc_req = completed_request_map.get(llm_business_desc_request_id)
        if not llm_business_desc_req:
            raise ValueError(
                f"business_desc_extraction_node.get_result: Missing GPTBatchRequest for mapping request ID {llm_business_desc_request_id} in {subject_unique_id}:{field_type.name}"
            )
        elif not llm_business_desc_req.response:
            raise ValueError(
                f"business_desc_extraction_node.get_result: GPTBatchRequest for mapping request ID {llm_business_desc_request_id} has no response_blob in {subject_unique_id}:{field_type.name}"
            )

        try:
            business_desc = parse_business_desc_from_gpt_response(
                llm_business_desc_req.response.result
            )
            return business_desc
        except Exception as e:
            await record_response_parse_error(
                gpt_batch_request=llm_business_desc_req,
                error_message=str(e),
                timestamp=timestamp,
                traceback_str=traceback.format_exc(),
            )
            logger.error(
                f"business_desc_extraction_node.get_result: Error parsing business description extraction results for manufacturer {subject_unique_id} and field type {field_type.name} from GPT response: {e}"
            )
            raise
