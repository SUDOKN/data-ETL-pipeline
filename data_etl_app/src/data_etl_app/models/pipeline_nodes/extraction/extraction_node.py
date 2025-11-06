import logging
from datetime import datetime
from abc import abstractmethod

from typing import Optional

from core.models.db.manufacturer import Manufacturer
from data_etl_app.models.pipeline_nodes.base_node import BaseNode, GenericFieldTypeVar
from data_etl_app.models.pipeline_nodes.reconcile.reconcile_node import ReconcileNode
from core.models.db.deferred_manufacturer import DeferredManufacturer
from core.models.db.gpt_batch_request import GPTBatchRequest
from core.services.gpt_batch_request_service import (
    bulk_upsert_gpt_batch_requests_with_only_req_bodies,
)
from scraper_app.models.scraped_text_file import ScrapedTextFile

logger = logging.getLogger(__name__)


# Strategy Pattern
class ExtractionNode(BaseNode[GenericFieldTypeVar]):
    """Base class for single phase of extraction."""

    def __init__(
        self,
        field_type: GenericFieldTypeVar,
        next_node: Optional["ExtractionNode | ReconcileNode"] = None,
    ):
        self.field_type: GenericFieldTypeVar = field_type
        self.next_node = next_node

    @abstractmethod
    def is_mfg_missing_data(
        self,
        mfg: Manufacturer,
    ) -> bool:
        """
        Determine if the manufacturer needs this extraction phase.
        Child classes must implement this method.
        """
        pass

    @abstractmethod
    async def is_deferred_mfg_missing_any_requests(
        self, deferred_mfg: DeferredManufacturer
    ) -> bool:
        """
        Check if any requests are missing for the given deferred manufacturer.
        Child classes must implement this method.
        """
        pass

    @abstractmethod
    async def are_all_deferred_mfg_requests_complete(
        self, deferred_mfg: DeferredManufacturer
    ) -> bool:
        """
        Check if all requests are complete for the given deferred manufacturer.
        Child classes must implement this method.
        """
        pass

    @abstractmethod  # Child classes must implement this method
    async def create_batch_requests(
        self,
        mfg: Manufacturer,
        deferred_mfg: DeferredManufacturer,
        scraped_text_file: ScrapedTextFile,
        timestamp: datetime,
    ) -> list[GPTBatchRequest]:
        """
        Create GPT batch requests needed for this extraction phase.
        Child classes must implement this method.
        """
        pass

    async def execute(
        self,
        mfg: Manufacturer,
        deferred_mfg: DeferredManufacturer,
        scraped_text_file: ScrapedTextFile,
        timestamp: datetime,
    ) -> None:
        """Execute this extraction phase if needed, and proceed to the next phase."""
        logger.info(
            f"[{mfg.etld1}] 🔄 Executing {self.__class__.__name__} for field '{self.field_type.name}'"
        )

        # if not self.is_mfg_missing_data(mfg=mfg):
        #     logger.info(
        #         f"[{mfg.etld1}] ⏭️  Skipping {self.__class__.__name__} - mfg already has all required data for '{self.field_type.name}'"
        #     )
        #     logger.info(
        #         f"[{deferred_mfg.mfg_etld1}] 🧹 Setting deferred_mfg.{self.field_type.name} field to None for cleanup"
        #     )
        #     setattr(deferred_mfg, self.field_type.name, None)
        #     return

        # logger.info(
        #     f"[{mfg.etld1}] 📝 {self.__class__.__name__}: Data missing for '{self.field_type.name}', checking deferred requests..."
        # )

        if await self.is_deferred_mfg_missing_any_requests(deferred_mfg=deferred_mfg):
            logger.info(
                f"[{mfg.etld1}] 🆕 {self.__class__.__name__}: Missing requests detected for '{self.field_type.name}'. Creating batch requests..."
            )
            batch_requests = await self.create_batch_requests(
                mfg=mfg,
                deferred_mfg=deferred_mfg,
                scraped_text_file=scraped_text_file,
                timestamp=timestamp,
            )
            logger.info(
                f"[{mfg.etld1}] ✅ Created {len(batch_requests)} batch requests for {self.__class__.__name__} ('{self.field_type.name}')"
            )
            await bulk_upsert_gpt_batch_requests_with_only_req_bodies(
                batch_requests=batch_requests, mfg_etld1=mfg.etld1
            )
            await deferred_mfg.save()
        else:
            logger.info(
                f"[{mfg.etld1}] ✓ {self.__class__.__name__}: All requests already exist for '{self.field_type.name}'"
            )

        # if no requests are missing, check if all requests are complete
        if await self.are_all_deferred_mfg_requests_complete(deferred_mfg=deferred_mfg):
            logger.info(
                f"[{mfg.etld1}] ✅ {self.__class__.__name__} is COMPLETE for '{self.field_type.name}'. "
                f"Proceeding to next phase: {self.next_node.__class__.__name__ if self.next_node else 'None'}"
            )
            if self.next_node:
                # next phase isn't executed unless this phase is complete
                # chain of responsibility
                if isinstance(self.next_node, ExtractionNode):
                    logger.info(
                        f"[{mfg.etld1}] ➡️  Proceeding to next ExtractionNode: {self.next_node.__class__.__name__}"
                    )
                    await self.next_node.execute(
                        mfg=mfg,
                        deferred_mfg=deferred_mfg,
                        scraped_text_file=scraped_text_file,
                        timestamp=timestamp,
                    )
                elif isinstance(self.next_node, ReconcileNode):
                    logger.info(
                        f"[{mfg.etld1}] ➡️  Proceeding to ReconcileNode: {self.next_node.__class__.__name__}"
                    )
                    await self.next_node.reconcile(
                        mfg=mfg,
                        deferred_mfg=deferred_mfg,
                        timestamp=timestamp,
                    )
        else:
            # phase not complete yet, so no batch requests to return and no next phase executed
            logger.info(
                f"[{mfg.etld1}] ⏸️  {self.__class__.__name__} is NOT complete for '{self.field_type.name}'. "
                f"Waiting for requests to complete. Cannot proceed to: {self.next_node.__class__.__name__ if self.next_node else 'None'}"
            )
