from __future__ import annotations
from datetime import datetime
import logging
import traceback
from typing import TYPE_CHECKING

from core.models.extraction_results.business_description_extraction_result import (
    BusinessDescription,
)
from core.models.deferred_extraction.deferred_single_stage_extraction_requests import (
    SingleStageExtractionRequestBundle,
)
from core.models.file_objects.prompt import Prompt
from core.models.db.gpt_batch_request import GPTBatchRequest
from data_etl_app.models.pipeline_nodes.single_stage.basic_fields.single_stage_extraction_node import (
    SingleStageExtractionNode,
)
from data_etl_app.models.types_and_enums import (
    BasicFieldTypeEnum,
)
from open_ai_key_app.models.field_types import GPTBatchRequestCustomID

if TYPE_CHECKING:
    from data_etl_app.models.pipeline_nodes.single_stage.basic_fields.business_desc_reconcile_node import (
        BusinessDescReconcileNode,
    )

from core.services.gpt_batch_request_writes import record_response_parse_error
from data_etl_app.services.extraction.deferred_business_desc_service import (
    parse_business_desc_from_gpt_response,
)

logger = logging.getLogger(__name__)


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

    @staticmethod
    async def get_result(
        mfg_etld1: str,
        chunk_bounds: str,
        extraction_bundle: SingleStageExtractionRequestBundle,
        completed_request_map: dict[GPTBatchRequestCustomID, GPTBatchRequest],
        timestamp: datetime,
        field_type: BasicFieldTypeEnum = BasicFieldTypeEnum.business_desc,
    ) -> BusinessDescription:
        llm_business_desc_request_id = extraction_bundle.llm_request_id
        if not llm_business_desc_request_id:
            raise ValueError(
                f"business_desc_extraction_node.get_result: llm_request_id is None for chunk bounds {chunk_bounds} in {mfg_etld1}:{field_type.name}"
            )

        llm_business_desc_req = completed_request_map.get(llm_business_desc_request_id)
        if not llm_business_desc_req:
            raise ValueError(
                f"business_desc_extraction_node.get_result: Missing GPTBatchRequest for mapping request ID {llm_business_desc_request_id} in {mfg_etld1}:{field_type.name}"
            )
        elif not llm_business_desc_req.response:
            raise ValueError(
                f"business_desc_extraction_node.get_result: GPTBatchRequest for mapping request ID {llm_business_desc_request_id} has no response_blob in {mfg_etld1}:{field_type.name}"
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
                f"business_desc_extraction_node.get_result: Error parsing business description extraction results for manufacturer {mfg_etld1} and field type {field_type.name} from GPT response: {e}"
            )
            raise
