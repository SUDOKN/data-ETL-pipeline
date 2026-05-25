from __future__ import annotations
import logging
from datetime import datetime
import traceback
from typing import TYPE_CHECKING

from core.models.prompt import Prompt
from core.models.binary_classification_result import (
    LLMBinaryClassification,
)
from core.models.deferred_single_stage_extraction_requests import (
    SingleStageExtractionRequestBundle,
)
from core.models.db.gpt_batch_request import GPTBatchRequest
from core.services.gpt_batch_request_writes import record_response_parse_error
from data_etl_app.models.pipeline_nodes.single_stage_extraction_node import (
    SingleStageExtractionNode,
)

if TYPE_CHECKING:
    from data_etl_app.models.pipeline_nodes.classification.binary_reconcile_node import (
        BinaryReconcileNode,
    )
from data_etl_app.models.types_and_enums import BinaryClassificationTypeEnum
from data_etl_app.services.extraction.deferred_binary_classification_service import (
    parse_binary_classification_result_from_gpt_response,
)
from open_ai_key_app.models.field_types import GPTBatchRequestCustomID
from open_ai_key_app.models.llm_model import LLM_Model

logger = logging.getLogger(__name__)


class BinaryClassificationNode(SingleStageExtractionNode[LLMBinaryClassification]):
    """For is_manufacturer, is_product_manufacturer, etc. binary classification tasks."""

    def __init__(
        self,
        classification_prompt: Prompt,
        binary_field_type: BinaryClassificationTypeEnum,
        next_node: BinaryReconcileNode,
    ):
        super().__init__(
            field_type=binary_field_type,
            prompt=classification_prompt,
            next_node=next_node,
        )

    @staticmethod
    async def parse_batch_request_result(
        mfg_etld1: str,
        field_type: BinaryClassificationTypeEnum,
        chunk_bounds: str,
        extraction_bundle: SingleStageExtractionRequestBundle,
        completed_request_map: dict[GPTBatchRequestCustomID, GPTBatchRequest],
        deferred_at: datetime,
    ) -> LLMBinaryClassification:
        classification_request_id = extraction_bundle.llm_request_id
        if not classification_request_id:
            raise ValueError(
                f"binary_classification_node.parse_batch_request_result: llm_request_id is None for chunk bounds {chunk_bounds} in {mfg_etld1}:{field_type.name}"
            )

        classification_req = completed_request_map.get(classification_request_id)
        if not classification_req:
            raise ValueError(
                f"binary_classification_node.parse_batch_request_result: Missing GPTBatchRequest for mapping request ID {classification_request_id} in {mfg_etld1}:{field_type.name}"
            )
        elif not classification_req.response_blob:
            raise ValueError(
                f"binary_classification_node.parse_batch_request_result: GPTBatchRequest for mapping request ID {classification_request_id} has no response_blob in {mfg_etld1}:{field_type.name}"
            )

        try:
            classification_result = (
                parse_binary_classification_result_from_gpt_response(
                    classification_req.response_blob.result
                )
            )
            return classification_result
        except Exception as e:
            await record_response_parse_error(
                gpt_batch_request=classification_req,
                error_message=str(e),
                timestamp=deferred_at,
                traceback_str=traceback.format_exc(),
            )
            logger.error(
                f"binary_classification_node.parse_batch_request_result: Error parsing binary classification results for manufacturer {mfg_etld1} and field type {field_type.name} from GPT response: {e}"
            )
            raise
