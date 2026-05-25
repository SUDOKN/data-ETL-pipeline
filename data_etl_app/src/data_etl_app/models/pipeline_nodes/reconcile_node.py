import logging
from datetime import datetime
from abc import abstractmethod

from core.models.db.manufacturer import Manufacturer
from core.models.db.deferred_manufacturer import DeferredManufacturer
from data_etl_app.models.pipeline_nodes.base_node import (
    BaseNode,
    PipelineContext,
    LLMExtractedFieldTypeVar,
)
from open_ai_key_app.models.field_types import GPTBatchRequestCustomID
from open_ai_key_app.models.llm_model import LLM_Model
from open_ai_key_app.models.gpt_model_params import GPTModelParams
from scraper_app.models.scraped_text_file import ScrapedTextFile

logger = logging.getLogger(__name__)


# Strategy Pattern
class ReconcileNode(BaseNode[LLMExtractedFieldTypeVar]):
    """Base class for the phase of reconciliation for any deferred field. Assumes extraction is done."""

    def __init__(self, field_type: LLMExtractedFieldTypeVar) -> None:
        super().__init__(field_type=field_type, next_node=None)

    @abstractmethod
    async def execute(
        self,
        mfg: Manufacturer,
        deferred_mfg: DeferredManufacturer,
        scraped_text_file: ScrapedTextFile,
        timestamp: datetime,
        pipeline_context: PipelineContext,
        llm_model: LLM_Model,
        model_params: GPTModelParams,
        eager: bool = False,  # doesn't do anything for reconcile node, but needed in signature for next_node calls from extraction node
    ) -> None:
        pass

    async def wipe_down(
        self,
        deferred_mfg: DeferredManufacturer,
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
