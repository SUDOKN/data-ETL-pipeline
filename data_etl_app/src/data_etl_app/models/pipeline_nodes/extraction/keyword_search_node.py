import logging
from datetime import datetime
from typing import Optional

from core.models.db.manufacturer import Manufacturer

from open_ai_key_app.models.field_types import GPTBatchRequestCustomID
from core.models.db.deferred_manufacturer import DeferredManufacturer
from core.models.db.gpt_batch_request import GPTBatchRequest
from data_etl_app.models.types_and_enums import KeywordTypeEnum
from core.models.deferred_keyword_extraction import (
    DeferredKeywordExtraction,
)
from data_etl_app.models.pipeline_nodes.extraction.extraction_node import (
    ExtractionNode,
)
from data_etl_app.models.pipeline_nodes.reconcile.keyword_reconcile_node import (
    KeywordReconcileNode,
)
from data_etl_app.services.llm_powered.extraction.extract_keyword_deferred_service import (
    get_missing_keyword_search_requests,
)
from core.services.gpt_batch_request_service import (
    find_gpt_batch_request_ids_only,
    find_completed_gpt_batch_request_ids_only,
)
from scraper_app.models.scraped_text_file import ScrapedTextFile

logger = logging.getLogger(__name__)


class KeywordSearchNode(ExtractionNode):
    """Phase 1: LLM Search for concepts"""

    field_type: KeywordTypeEnum
    next_node: Optional[KeywordReconcileNode]

    def __init__(
        self,
        field_type: KeywordTypeEnum,
        next_node: Optional[KeywordReconcileNode],
    ):
        super().__init__(field_type=field_type, next_node=next_node)

    def is_mfg_missing_data(
        self,
        mfg: Manufacturer,
    ) -> bool:
        return not bool(getattr(mfg, self.field_type.name))

    async def is_deferred_mfg_missing_any_requests(
        self, deferred_mfg: DeferredManufacturer
    ) -> bool:
        deferred_keyword_extraction: Optional[DeferredKeywordExtraction] = getattr(
            deferred_mfg, self.field_type.name
        )
        if not deferred_keyword_extraction:
            return True

        # Check if all search requests exist
        llm_search_req_ids_to_lookup: set[GPTBatchRequestCustomID] = set()
        for (
            _chunk_bounds,
            llm_search_request_id,
        ) in deferred_keyword_extraction.chunk_request_id_map.items():
            llm_search_req_ids_to_lookup.add(llm_search_request_id)

        gpt_req_ids_missing = llm_search_req_ids_to_lookup - (
            await find_gpt_batch_request_ids_only(list(llm_search_req_ids_to_lookup))
        )

        if gpt_req_ids_missing:
            return True

        return False

    async def are_all_deferred_mfg_requests_complete(
        self, deferred_mfg: DeferredManufacturer
    ) -> bool:
        deferred_keyword_extraction: Optional[DeferredKeywordExtraction] = getattr(
            deferred_mfg, self.field_type.name
        )
        if not deferred_keyword_extraction:
            raise ValueError(
                f"are_all_deferred_mfg_requests_complete was called for {self.field_type.name} in {__class__.__name__} but no deferred concept extraction exists."
            )

        # Check if all search requests are complete
        llm_search_req_ids_to_lookup: set[GPTBatchRequestCustomID] = set()
        for (
            _chunk_bounds,
            llm_search_request_id,
        ) in deferred_keyword_extraction.chunk_request_id_map.items():
            llm_search_req_ids_to_lookup.add(llm_search_request_id)

        incomplete_gpt_req_ids = llm_search_req_ids_to_lookup - (
            await find_completed_gpt_batch_request_ids_only(
                list(llm_search_req_ids_to_lookup)
            )
        )

        return not bool(incomplete_gpt_req_ids)

    async def create_batch_requests(
        self,
        mfg: Manufacturer,
        deferred_mfg: DeferredManufacturer,
        scraped_text_file: ScrapedTextFile,
        timestamp: datetime,
    ) -> list[GPTBatchRequest]:
        """Create batch requests for concept search phase."""

        deferred_keyword_extraction: Optional[DeferredKeywordExtraction] = getattr(
            deferred_mfg, self.field_type.name
        )

        # get_missing_concept_mapping_requests only creates batch requests fresh or only missing ones,
        # for e.g., new mfg or some batch requests failed earlier and were deleted to allow re-processing
        deferred_keyword_extraction, batch_requests = (
            await get_missing_keyword_search_requests(
                deferred_at=timestamp,
                keyword_type=self.field_type,
                deferred_keyword_extraction=deferred_keyword_extraction,
                mfg_etld1=mfg.etld1,
                mfg_text=scraped_text_file.text,
            )
        )

        # Update deferred_mfg in memory only with new concept extraction
        setattr(deferred_mfg, self.field_type.name, deferred_keyword_extraction)

        return batch_requests
