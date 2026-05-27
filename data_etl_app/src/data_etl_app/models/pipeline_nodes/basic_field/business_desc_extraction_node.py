from __future__ import annotations
import logging
from datetime import datetime
import traceback
from typing import TYPE_CHECKING

from core.models.business_description_extraction_result import BusinessDescription
from core.models.deferred_single_stage_extraction_requests import (
    SingleStageExtractionRequestBundle,
)
from core.models.prompt import Prompt
from core.models.db.gpt_batch_request import GPTBatchRequest

if TYPE_CHECKING:
    from data_etl_app.models.pipeline_nodes.basic_field.business_desc_reconcile_node import (
        BusinessDescReconcileNode,
    )

from data_etl_app.models.pipeline_nodes.single_stage_extraction_node import (
    SingleStageExtractionNode,
)
from data_etl_app.models.types_and_enums import (
    BasicFieldTypeEnum,
)
from open_ai_key_app.models.field_types import GPTBatchRequestCustomID

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
    async def parse_batch_request_result(
        mfg_etld1: str,
        chunk_bounds: str,
        extraction_bundle: SingleStageExtractionRequestBundle,
        completed_request_map: dict[GPTBatchRequestCustomID, GPTBatchRequest],
        deferred_at: datetime,
        field_type: BasicFieldTypeEnum = BasicFieldTypeEnum.business_desc,
    ) -> BusinessDescription:
        llm_business_desc_request_id = extraction_bundle.llm_request_id
        if not llm_business_desc_request_id:
            raise ValueError(
                f"business_desc_extraction_node.parse_batch_request_result: llm_request_id is None for chunk bounds {chunk_bounds} in {mfg_etld1}:{field_type.name}"
            )

        llm_business_desc_req = completed_request_map.get(llm_business_desc_request_id)
        if not llm_business_desc_req:
            raise ValueError(
                f"business_desc_extraction_node.parse_batch_request_result: Missing GPTBatchRequest for mapping request ID {llm_business_desc_request_id} in {mfg_etld1}:{field_type.name}"
            )
        elif not llm_business_desc_req.response:
            raise ValueError(
                f"business_desc_extraction_node.parse_batch_request_result: GPTBatchRequest for mapping request ID {llm_business_desc_request_id} has no response_blob in {mfg_etld1}:{field_type.name}"
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
                timestamp=deferred_at,
                traceback_str=traceback.format_exc(),
            )
            logger.error(
                f"business_desc_extraction_node.parse_batch_request_result: Error parsing business description extraction results for manufacturer {mfg_etld1} and field type {field_type.name} from GPT response: {e}"
            )
            raise
