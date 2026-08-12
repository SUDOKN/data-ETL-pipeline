from __future__ import annotations
import logging
from datetime import datetime
import traceback
from typing import TYPE_CHECKING

from llm_providers.db_models.gpt_batch_request import (
    GPTBatchRequest,
)
from llm_providers.models.file_objects.prompt import Prompt
from data_etl_app.models.extraction_results.address_extraction_result import (
    Address,
    AddressExtractionResponse,
)
from core.models.deferred_extraction.deferred_single_stage_extraction_requests import (
    SingleStageExtractionRequestBundle,
)
from data_etl_app.models.types_and_enums import (
    BasicFieldTypeEnum,
)
from core.models.pipeline_nodes.single_stage.base.single_stage_extraction_node import (
    SingleStageExtractionNode,
)
from core.models.field_types import ExtractionFieldType
from core.models.extraction_schemas.response_format_util import (
    build_gpt_response_format,
)

if TYPE_CHECKING:  # <-- guard the circular import
    from data_etl_app.models.pipeline_nodes.single_stage.basic_fields.address_reconcile_node import (
        AddressReconcileNode,
    )
from llm_providers.field_types import BatchRequestIDType

from llm_providers.services.gpt_batch_request.gpt_batch_request_writes import (
    record_response_parse_error,
)
from data_etl_app.services.extraction.deferred_address_service import (
    parse_address_list_from_gpt_response,
)

logger = logging.getLogger(__name__)

ADDRESS_RESPONSE_SCHEMA = build_gpt_response_format(
    AddressExtractionResponse, name="address_extraction"
)


class AddressExtractionNode(SingleStageExtractionNode[list[Address]]):
    def __init__(
        self,
        next_node: AddressReconcileNode,
        extract_prompt: Prompt,
    ):
        super().__init__(
            field_type=BasicFieldTypeEnum.addresses,
            next_node=next_node,
            prompt=extract_prompt,
        )

    def get_response_schema(self) -> dict:
        return ADDRESS_RESPONSE_SCHEMA

    @staticmethod
    async def get_result(
        subject_unique_id: str,
        field_type: ExtractionFieldType,
        chunk_bounds: str,
        extraction_bundle: SingleStageExtractionRequestBundle,
        completed_request_map: dict[BatchRequestIDType, GPTBatchRequest],
        timestamp: datetime,
    ) -> list[Address]:
        llm_address_request_id = extraction_bundle.llm_request_id
        if not llm_address_request_id:
            raise ValueError(
                f"address_extraction_node.get_result: llm_request_id is None for chunk bounds {chunk_bounds} in {subject_unique_id}:{field_type.name}"
            )

        llm_address_req = completed_request_map.get(llm_address_request_id)
        if not llm_address_req:
            raise ValueError(
                f"address_extraction_node.get_result: Missing GPTBatchRequest for mapping request ID {llm_address_request_id} in {subject_unique_id}:{field_type.name}"
            )
        elif not llm_address_req.response:
            raise ValueError(
                f"address_extraction_node.get_result: GPTBatchRequest for mapping request ID {llm_address_request_id} has no response_blob in {subject_unique_id}:{field_type.name}"
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
                timestamp=timestamp,
                traceback_str=traceback.format_exc(),
            )
            logger.error(
                f"address_extraction_node.get_result: Error parsing address extraction results for manufacturer {subject_unique_id} from GPT response: {e}"
            )
            raise
