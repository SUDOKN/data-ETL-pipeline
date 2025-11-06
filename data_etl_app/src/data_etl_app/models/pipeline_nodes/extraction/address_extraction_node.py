import logging
from datetime import datetime
from typing import Optional

from core.models.db.manufacturer import Manufacturer
from core.models.deferred_basic_extraction import DeferredBasicExtraction
from data_etl_app.models.types_and_enums import BasicFieldTypeEnum
from data_etl_app.services.llm_powered.extraction.extract_basic_deferred_service import (
    find_addresses_from_first_chunk_deferred,
)
from open_ai_key_app.models.field_types import GPTBatchRequestCustomID
from core.models.db.deferred_manufacturer import DeferredManufacturer
from core.models.db.gpt_batch_request import GPTBatchRequest
from data_etl_app.models.pipeline_nodes.extraction.extraction_node import (
    ExtractionNode,
)
from data_etl_app.models.pipeline_nodes.reconcile.address_reconcile_node import (
    AddressReconcileNode,
)
from core.services.gpt_batch_request_service import (
    find_gpt_batch_request_ids_only,
    find_completed_gpt_batch_request_ids_only,
)
from scraper_app.models.scraped_text_file import ScrapedTextFile

logger = logging.getLogger(__name__)


class AddressExtractionNode(ExtractionNode):
    field_type: BasicFieldTypeEnum
    next_node: Optional[AddressReconcileNode]

    def __init__(
        self,
        field_type: BasicFieldTypeEnum,
        next_node: Optional[AddressReconcileNode],
    ):
        super().__init__(field_type=field_type, next_node=next_node)

    def is_mfg_missing_data(
        self,
        mfg: Manufacturer,
    ) -> bool:
        return mfg.addresses is None

    async def is_deferred_mfg_missing_any_requests(
        self, deferred_mfg: DeferredManufacturer
    ) -> bool:
        deferred_address_extraction: Optional[DeferredBasicExtraction] = (
            deferred_mfg.addresses
        )
        if not deferred_address_extraction:
            return True

        gpt_req_ids_to_lookup = set([deferred_address_extraction.gpt_request_id])
        gpt_req_ids_missing = gpt_req_ids_to_lookup - (
            await find_gpt_batch_request_ids_only(list(gpt_req_ids_to_lookup))
        )

        if gpt_req_ids_missing:
            return True

        return False

    async def are_all_deferred_mfg_requests_complete(
        self, deferred_mfg: DeferredManufacturer
    ) -> bool:
        deferred_address_extraction: Optional[DeferredBasicExtraction] = (
            deferred_mfg.addresses
        )
        if not deferred_address_extraction:
            raise ValueError(
                f"are_all_deferred_mfg_requests_complete was called for {self.field_type.name} in {__class__.__name__} but no deferred address extraction exists."
            )

        gpt_req_ids_to_lookup: set[GPTBatchRequestCustomID] = set(
            [deferred_address_extraction.gpt_request_id]
        )

        logger.info(f"Looking up GPT request IDs: {gpt_req_ids_to_lookup}")

        incomplete_gpt_req_ids = gpt_req_ids_to_lookup - (
            await find_completed_gpt_batch_request_ids_only(list(gpt_req_ids_to_lookup))
        )

        logger.info(f"Incomplete GPT request IDs: {incomplete_gpt_req_ids}")

        return not bool(incomplete_gpt_req_ids)

    async def create_batch_requests(
        self,
        mfg: Manufacturer,
        deferred_mfg: DeferredManufacturer,
        scraped_text_file: ScrapedTextFile,
        timestamp: datetime,
    ) -> list[GPTBatchRequest]:
        """Create batch requests for address extraction phase."""

        deferred_address_extraction: Optional[DeferredBasicExtraction] = (
            deferred_mfg.addresses
        )

        updated_deferred_address_extraction, batch_request = (
            await find_addresses_from_first_chunk_deferred(
                deferred_at=timestamp,
                deferred_address_extraction=deferred_address_extraction,
                mfg_etld1=mfg.etld1,
                mfg_text=scraped_text_file.text,
            )
        )

        deferred_mfg.addresses = updated_deferred_address_extraction

        return [batch_request]
