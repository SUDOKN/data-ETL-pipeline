from __future__ import annotations
import logging
from datetime import datetime
import traceback

from llm_providers.models.file_objects.prompt import Prompt
from core.models.extraction_results.binary_classification_result import (
    LLMBinaryClassification,
)
from core.models.deferred_extraction.deferred_single_stage_extraction_requests import (
    SingleStageExtractionRequestBundle,
)
from llm_providers.db_models.gpt_batch_request import (
    GPTBatchRequest,
)
from llm_providers.services.gpt_batch_request.gpt_batch_request_writes import (
    record_response_parse_error,
)
from typing import TYPE_CHECKING

from core.models.pipeline_nodes.single_stage.base.single_stage_extraction_node import (
    SingleStageExtractionNode,
)
from core.models.field_types import ExtractionFieldType

if TYPE_CHECKING:
    from core.models.pipeline_nodes.single_stage.classification.binary_reconcile_node import (
        BinaryReconcileNode,
    )
from core.services.pipeline_nodes.single_stage.llm_binary_classification_service import (
    get_binary_classification_response_schema,
    parse_binary_classification_result_from_gpt_response,
)
from llm_providers.field_types import BatchRequestIDType

logger = logging.getLogger(__name__)



class BinaryClassificationNode(SingleStageExtractionNode[LLMBinaryClassification]):
    """For is_manufacturer, is_product_manufacturer, etc. binary classification tasks."""

    def __init__(
        self,
        classification_prompt: Prompt,
        binary_field_type: ExtractionFieldType,
        next_node: BinaryReconcileNode,
    ):
        super().__init__(
            field_type=binary_field_type,
            prompt=classification_prompt,
            next_node=next_node,
        )

    def get_response_schema(self) -> dict:
        return get_binary_classification_response_schema(self.field_type)

    @staticmethod
    async def get_result(
        subject_unique_id: str,
        field_type: ExtractionFieldType,
        chunk_bounds: str,
        extraction_bundle: SingleStageExtractionRequestBundle,
        completed_request_map: dict[BatchRequestIDType, GPTBatchRequest],
        timestamp: datetime,
    ) -> LLMBinaryClassification:
        classification_request_id = extraction_bundle.llm_request_id
        if not classification_request_id:
            raise ValueError(
                f"binary_classification_node.get_result: llm_request_id is None for chunk bounds {chunk_bounds} in {subject_unique_id}:{field_type.name}"
            )

        classification_req = completed_request_map.get(classification_request_id)
        if not classification_req:
            raise ValueError(
                f"binary_classification_node.get_result: Missing GPTBatchRequest for mapping request ID {classification_request_id} in {subject_unique_id}:{field_type.name}"
            )
        elif not classification_req.response:
            raise ValueError(
                f"binary_classification_node.get_result: GPTBatchRequest for mapping request ID {classification_request_id} has no response_blob in {subject_unique_id}:{field_type.name}"
            )

        try:
            classification_result = (
                parse_binary_classification_result_from_gpt_response(
                    classification_req.response.result,
                    field_type=field_type,
                )
            )
            return classification_result
        except Exception as e:
            await record_response_parse_error(
                gpt_batch_request=classification_req,
                error_message=str(e),
                timestamp=timestamp,
                traceback_str=traceback.format_exc(),
            )
            logger.error(
                f"binary_classification_node.get_result: Error parsing binary classification results for subject {subject_unique_id} and field type {field_type.name} from GPT response: {e}"
            )
            raise
