import logging
from datetime import datetime
from typing import Optional

from core.models.db.manufacturer import Manufacturer
from data_etl_app.services.llm_powered.classification.deferred_binary_classifier import (
    binary_classify_deferred,
)
from core.models.db.deferred_manufacturer import DeferredManufacturer
from core.models.db.gpt_batch_request import GPTBatchRequest
from data_etl_app.models.types_and_enums import BinaryClassificationTypeEnum
from core.models.deferred_binary_classification import (
    DeferredBinaryClassification,
)
from data_etl_app.models.pipeline_nodes.extraction.extraction_node import (
    ExtractionNode,
)
from data_etl_app.models.pipeline_nodes.reconcile.binary_reconcile_node import (
    BinaryReconcileNode,
)
from core.services.gpt_batch_request_service import (
    find_gpt_batch_request_ids_only,
    find_completed_gpt_batch_request_ids_only,
)
from open_ai_key_app.models.field_types import GPTBatchRequestCustomID
from scraper_app.models.scraped_text_file import ScrapedTextFile

logger = logging.getLogger(__name__)


class BinaryClassificationNode(ExtractionNode):
    """For is_manufacturer, is_product_manufacturer, etc. binary classification tasks."""

    field_type: BinaryClassificationTypeEnum
    next_node: Optional[BinaryReconcileNode]

    def __init__(
        self,
        binary_field_type: BinaryClassificationTypeEnum,
        next_node: Optional[BinaryReconcileNode],
    ):
        super().__init__(field_type=binary_field_type, next_node=next_node)

    def is_mfg_missing_data(
        self,
        mfg: Manufacturer,
    ) -> bool:
        return not bool(getattr(mfg, self.field_type.name))

    async def is_deferred_mfg_missing_any_requests(
        self, deferred_mfg: DeferredManufacturer
    ) -> bool:
        deferred_binary_classification: Optional[DeferredBinaryClassification] = (
            getattr(deferred_mfg, self.field_type.name)
        )
        if not deferred_binary_classification:
            return True

        # else check if all llm search GPT requests exist
        gpt_req_ids_to_lookup: set[GPTBatchRequestCustomID] = set()
        for (
            _chunk_bounds,
            gpt_req_id,
        ) in deferred_binary_classification.chunk_request_id_map.items():
            gpt_req_ids_to_lookup.add(gpt_req_id)

        gpt_req_ids_missing = gpt_req_ids_to_lookup - (
            await find_gpt_batch_request_ids_only(list(gpt_req_ids_to_lookup))
        )
        if gpt_req_ids_missing:
            return True

        return False

    async def are_all_deferred_mfg_requests_complete(
        self, deferred_mfg: DeferredManufacturer
    ) -> bool:
        deferred_binary_classification: Optional[DeferredBinaryClassification] = (
            getattr(deferred_mfg, self.field_type.name)
        )
        if not deferred_binary_classification:
            raise ValueError(
                f"are_all_deferred_mfg_requests_complete was called for {self.field_type.name} in {__class__.__name__} but no deferred binary classification exists."
            )

        # else check if all llm search GPT requests are complete
        gpt_req_ids_to_lookup = set()
        for (
            _chunk_bounds,
            gpt_req_id,
        ) in deferred_binary_classification.chunk_request_id_map.items():
            gpt_req_ids_to_lookup.add(gpt_req_id)

        incomplete_gpt_req_ids = gpt_req_ids_to_lookup - (
            await find_completed_gpt_batch_request_ids_only(list(gpt_req_ids_to_lookup))
        )

        return not bool(incomplete_gpt_req_ids)

    async def create_batch_requests(
        self,
        mfg: Manufacturer,
        deferred_mfg: DeferredManufacturer,
        scraped_text_file: ScrapedTextFile,
        timestamp: datetime,
    ) -> list[GPTBatchRequest]:
        """Create batch requests for binary classification phase."""

        deferred_binary_extraction: Optional[DeferredBinaryClassification] = getattr(
            deferred_mfg, self.field_type.name
        )

        updated_deferred_binary_extraction, batch_request = (
            await binary_classify_deferred(
                deferred_at=timestamp,
                deferred_binary_classification=deferred_binary_extraction,
                mfg_etld1=mfg.etld1,
                mfg_text=scraped_text_file.text,
                classification_type=self.field_type,
            )
        )

        setattr(
            deferred_mfg,
            self.field_type.name,
            updated_deferred_binary_extraction,
        )

        return [batch_request]
