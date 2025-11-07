from datetime import datetime
import logging

from core.models.db.manufacturer import Manufacturer
from core.models.db.deferred_manufacturer import DeferredManufacturer
from core.services.deferred_manufacturer_service import (
    delete_deferred_manufacturer_if_empty,
)
from core.services.gpt_batch_request_service import (
    bulk_delete_gpt_batch_requests_by_mfg_etld1_and_field,
)
from data_etl_app.services.extraction_pipeline_factory import ExtractionPipelineFactory
from scraper_app.models.scraped_text_file import ScrapedTextFile

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logging.getLogger("botocore").setLevel(logging.WARNING)
logging.getLogger("boto3").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)


class ManufacturerExtractionOrchestrator:
    """
    Orchestrates the intake of manufacturers into the deferred extraction pipeline.
    Determines what's incomplete and creates appropriate batch requests.
    """

    def __init__(self):
        self.pipelines = ExtractionPipelineFactory.create_pipelines()

    async def process_manufacturer(
        self, timestamp: datetime, mfg: Manufacturer, scraped_text_file: ScrapedTextFile
    ) -> None:
        """
        Main entry point: process a manufacturer and create/update deferred extraction.

        Args:
            timestamp (datetime): Current timestamp.
            mfg (Manufacturer): Manufacturer to process.
        """

        # Create or update the deferred manufacturer
        deferred_mfg = await self._get_or_create_deferred(timestamp, mfg)

        logger.debug(
            f"[{mfg.etld1}] Starting extraction pipeline with {len(self.pipelines)} field types"
        )

        for field_type, pipeline in self.pipelines.items():
            logger.debug(
                f"[{mfg.etld1}] Processing extraction pipeline for field '{field_type.name}', stats: {mfg.scraped_text_file_num_tokens} tokens"
            )
            if pipeline.is_mfg_missing_data(mfg):
                logger.info(
                    f"[{mfg.etld1}] ❌ Missing data for field '{field_type.name}'. Processing pipeline..."
                )
                await pipeline.execute(
                    mfg=mfg,
                    deferred_mfg=deferred_mfg,
                    scraped_text_file=scraped_text_file,
                    timestamp=timestamp,
                )
            else:
                logger.debug(
                    f"[{mfg.etld1}] ✓ Already has data for field '{field_type.name}'. Setting deferred.{field_type.name} to None..."
                )
                setattr(deferred_mfg, field_type.name, None)
                await bulk_delete_gpt_batch_requests_by_mfg_etld1_and_field(
                    mfg_etld1=mfg.etld1,
                    field_type=field_type,
                )

        logger.info(f"[{mfg.etld1}] Completed extraction pipeline for all fields")
        await delete_deferred_manufacturer_if_empty(deferred_mfg)

    async def _get_or_create_deferred(
        self, timestamp: datetime, mfg: Manufacturer
    ) -> DeferredManufacturer:
        """Get existing DeferredManufacturer or create new one"""
        deferred_manufacturer = await DeferredManufacturer.find_one(
            DeferredManufacturer.mfg_etld1 == mfg.etld1
        )

        if not deferred_manufacturer:
            deferred_manufacturer = DeferredManufacturer(
                created_at=timestamp,
                mfg_etld1=mfg.etld1,
                scraped_text_file_num_tokens=mfg.scraped_text_file_num_tokens,
                scraped_text_file_version_id=mfg.scraped_text_file_version_id,
                is_manufacturer=None,
                is_contract_manufacturer=None,
                is_product_manufacturer=None,
                addresses=None,
                business_desc=None,
                products=None,
                certificates=None,
                industries=None,
                process_caps=None,
                material_caps=None,
            )
            await deferred_manufacturer.insert()

        return deferred_manufacturer
