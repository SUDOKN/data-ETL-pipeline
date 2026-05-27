from __future__ import annotations
import logging
from datetime import datetime
import traceback
from typing import TYPE_CHECKING

from core.models.address_extraction_result import Address
from core.models.deferred_single_stage_extraction_requests import (
    SingleStageExtractionRequestBundle,
)
from core.models.prompt import Prompt
from core.models.db.gpt_batch_request import GPTBatchRequest
from data_etl_app.models.types_and_enums import (
    BasicFieldTypeEnum,
)
from data_etl_app.models.pipeline_nodes.single_stage_extraction_node import (
    SingleStageExtractionNode,
)

if TYPE_CHECKING:  # <-- guard the circular import
    from data_etl_app.models.pipeline_nodes.basic_field.address_reconcile_node import (
        AddressReconcileNode,
    )
from open_ai_key_app.models.field_types import GPTBatchRequestCustomID

from core.services.gpt_batch_request_writes import record_response_parse_error
from data_etl_app.services.extraction.deferred_address_service import (
    parse_address_list_from_gpt_response,
)

logger = logging.getLogger(__name__)


class AddressExtractionNode(SingleStageExtractionNode[list[Address]]):
    def __init__(
        self,
        extract_prompt: Prompt,
        next_node: AddressReconcileNode,
    ):
        super().__init__(
            field_type=BasicFieldTypeEnum.addresses,
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
        field_type: BasicFieldTypeEnum = BasicFieldTypeEnum.addresses,
    ) -> list[Address]:
        llm_address_request_id = extraction_bundle.llm_request_id
        if not llm_address_request_id:
            raise ValueError(
                f"address_extraction_node.parse_batch_request_result: llm_request_id is None for chunk bounds {chunk_bounds} in {mfg_etld1}:{field_type.name}"
            )

        llm_address_req = completed_request_map.get(llm_address_request_id)
        if not llm_address_req:
            raise ValueError(
                f"address_extraction_node.parse_batch_request_result: Missing GPTBatchRequest for mapping request ID {llm_address_request_id} in {mfg_etld1}:{field_type.name}"
            )
        elif not llm_address_req.response:
            raise ValueError(
                f"address_extraction_node.parse_batch_request_result: GPTBatchRequest for mapping request ID {llm_address_request_id} has no response_blob in {mfg_etld1}:{field_type.name}"
            )

        try:
            addresses = parse_address_list_from_gpt_response(
                llm_address_req.response.result
            )
            return addresses
        except Exception as e:
            await record_response_parse_error(
                gpt_batch_request=llm_address_req,
                error_message=str(e),
                timestamp=deferred_at,
                traceback_str=traceback.format_exc(),
            )
            logger.error(
                f"address_extraction_node.parse_batch_request_result: Error parsing address extraction results for manufacturer {mfg_etld1} from GPT response: {e}"
            )
            raise
