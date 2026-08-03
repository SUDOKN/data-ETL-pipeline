from __future__ import annotations

import logging

from core.models.base.extraction_subject import AbstractDeferredExtractionSubject
from data_etl_app.models.pipeline_nodes.base.base_node import (
    BaseNode,
    LLMExtractedFieldTypeVar,
    ResultT,
)
from open_ai_key_app.models.field_types import GPTBatchRequestCustomID

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
        deferred_mfg: AbstractDeferredExtractionSubject,
        associated_batch_request_custom_ids: list[GPTBatchRequestCustomID],
    ) -> None:
        """Wipe down deferred field and any completed GPT requests in pipeline context related to this field. Called at end of execute."""

        # logger.info(
        #     f"Reconciled {self.field_type.name} data for manufacturer {deferred_mfg.etld1}. "
        #     f"Attempting to clean up GPTBatchRequests."
        # )
        # await bulk_delete_gpt_batch_requests_by_custom_ids(
        #     gpt_batch_request_custom_ids=associated_batch_request_custom_ids,
        #     mfg_etld1=deferred_mfg.etld1,
        # )
        # logger.info(
        #     f"Cleaned up GPTBatchRequests for {self.field_type.name} of manufacturer {deferred_mfg.etld1}. "
        #     f"Attempting to clear deferred_mfg {self.field_type.name} extraction field."
        # )
        # setattr(deferred_mfg, self.field_type.name, None)
        # await deferred_mfg.save()
        # logger.info(
        #     f"Cleared deferred_mfg {self.field_type.name} extraction field for manufacturer {deferred_mfg.etld1}. Reconciliation complete."
        # )
