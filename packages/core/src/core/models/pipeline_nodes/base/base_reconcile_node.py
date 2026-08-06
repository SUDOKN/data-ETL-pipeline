from __future__ import annotations

import logging

from core.models.extraction_subject import (
    AbstractDeferredExtractionSubject,
)
from core.models.pipeline_nodes.base.base_node import (
    BaseNode,
    LLMExtractedFieldTypeVar,
    ResultT,
)
from llm_providers.field_types import BatchRequestIDType

logger = logging.getLogger(__name__)


# Strategy Pattern
class ReconcileNode(BaseNode[LLMExtractedFieldTypeVar, None]):
    """Base class for the phase of reconciliation for any deferred field. Assumes extraction is done."""

    def __init__(
        self,
        field_type: LLMExtractedFieldTypeVar,
    ) -> None:
        super().__init__(field_type=field_type, next_node=None)

    async def wipe_down(
        self,
        deferred_subject: AbstractDeferredExtractionSubject,
        associated_batch_request_custom_ids: list[BatchRequestIDType],
    ) -> None:
        """Wipe down deferred field and any completed GPT requests in pipeline context related to this field. Called at end of execute."""

        # logger.info(
        #     f"Reconciled {self.field_type.name} data for manufacturer {deferred_mfg.subject_unique_id}. "
        #     f"Attempting to clean up GPTBatchRequests."
        # )
        # await bulk_delete_gpt_batch_requests_by_custom_ids(
        #     gpt_batch_request_custom_ids=associated_batch_request_custom_ids,
        #     subject_unique_id=deferred_mfg.subject_unique_id,
        # )
        # logger.info(
        #     f"Cleaned up GPTBatchRequests for {self.field_type.name} of manufacturer {deferred_mfg.subject_unique_id}. "
        #     f"Attempting to clear deferred_mfg {self.field_type.name} extraction field."
        # )
        # setattr(deferred_mfg, self.field_type.name, None)
        # await deferred_mfg.save()
        # logger.info(
        #     f"Cleared deferred_mfg {self.field_type.name} extraction field for manufacturer {deferred_mfg.subject_unique_id}. Reconciliation complete."
        # )
