import logging
from datetime import datetime
from abc import abstractmethod

from core.models.db.manufacturer import Manufacturer
from core.models.db.deferred_manufacturer import DeferredManufacturer
from data_etl_app.models.pipeline_nodes.base_node import (
    BaseNode,
    LLMExtractedFieldTypeVar,
)
from data_etl_app.models.types_and_enums import PipelineContext
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
        eager: bool = False,  # doesn't do anything for reconcile node, but needed in signature for next_node calls from extraction node
    ) -> None:
        pass

    async def wipe_down(
        self,
        deferred_mfg: DeferredManufacturer,
        pipeline_context: PipelineContext,
    ) -> None:
        """Wipe down deferred field and any completed GPT requests in pipeline context related to this field. Called at end of execute."""
        logger.info(
            f"[{deferred_mfg.etld1}] 🧹 Setting deferred_mfg.{self.field_type.name} field to None for cleanup"
        )
        setattr(deferred_mfg, self.field_type.name, None)
        await deferred_mfg.save()
