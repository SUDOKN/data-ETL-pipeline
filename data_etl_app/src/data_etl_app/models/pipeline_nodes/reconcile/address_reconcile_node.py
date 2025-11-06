import logging
from datetime import datetime
from typing import Optional

from core.models.db.manufacturer import Manufacturer
from core.services.manufacturer_service import update_manufacturer

from core.models.deferred_basic_extraction import DeferredBasicExtraction
from data_etl_app.models.types_and_enums import BasicFieldTypeEnum
from data_etl_app.services.llm_powered.extraction.extract_basic_service import (
    parse_address_list_from_gpt_response,
)
from core.models.db.deferred_manufacturer import DeferredManufacturer
from data_etl_app.models.pipeline_nodes.reconcile.reconcile_node import ReconcileNode
from core.services.gpt_batch_request_service import (
    find_gpt_batch_request_by_custom_id,
    bulk_delete_gpt_batch_requests_by_custom_ids,
)

logger = logging.getLogger(__name__)


class AddressReconcileNode(ReconcileNode):
    field_type: BasicFieldTypeEnum

    def __init__(self, field_type: BasicFieldTypeEnum) -> None:
        super().__init__(field_type)

    async def reconcile(
        self,
        mfg: Manufacturer,
        deferred_mfg: DeferredManufacturer,
        timestamp: datetime,
        force: bool = False,
    ) -> None:
        if mfg.addresses and not force:
            return

        deferred_address_extraction: Optional[DeferredBasicExtraction] = (
            deferred_mfg.addresses
        )
        if not deferred_address_extraction:
            raise ValueError(
                f"reconcile was called for addresses but no deferred address extraction exists."
            )

        gpt_request = await find_gpt_batch_request_by_custom_id(
            deferred_address_extraction.gpt_request_id
        )
        if not gpt_request or not gpt_request.response_blob:
            raise ValueError(
                f"Could not find completed GPTBatchRequest for address extraction with custom_id={deferred_address_extraction.gpt_request_id}"
            )

        mfg.addresses = parse_address_list_from_gpt_response(
            gpt_request.response_blob.result
        )
        await update_manufacturer(updated_at=timestamp, manufacturer=mfg)

        await bulk_delete_gpt_batch_requests_by_custom_ids(
            gpt_batch_request_custom_ids=[deferred_address_extraction.gpt_request_id],
            mfg_etld1=mfg.etld1,
        )

        # call super reconcile to clear deferred field
        await super().reconcile(
            mfg=mfg,
            deferred_mfg=deferred_mfg,
            timestamp=timestamp,
            force=force,
        )
