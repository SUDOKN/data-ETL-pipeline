import logging
from datetime import datetime
from typing import Optional

from core.models.db.manufacturer import Manufacturer

from open_ai_key_app.models.field_types import GPTBatchRequestCustomID
from core.models.db.deferred_manufacturer import DeferredManufacturer
from core.models.db.gpt_batch_request import GPTBatchRequest
from data_etl_app.models.types_and_enums import ConceptTypeEnum
from core.models.deferred_concept_extraction import (
    DeferredConceptExtraction,
)
from data_etl_app.models.pipeline_nodes.extraction.concept_mapping_node import (
    ConceptMappingNode,
)
from data_etl_app.models.pipeline_nodes.extraction.extraction_node import (
    ExtractionNode,
)
from data_etl_app.services.llm_powered.extraction.extract_concept_deferred_service import (
    get_missing_concept_search_requests,
)
from core.services.gpt_batch_request_service import (
    find_gpt_batch_request_ids_only,
    find_completed_gpt_batch_request_ids_only,
)
from scraper_app.models.scraped_text_file import ScrapedTextFile

logger = logging.getLogger(__name__)


class ConceptSearchNode(ExtractionNode):
    """Phase 1: LLM Search for concepts"""

    field_type: ConceptTypeEnum
    next_node: Optional[ConceptMappingNode]

    def __init__(
        self,
        concept_type: ConceptTypeEnum,
        next_node: Optional[ConceptMappingNode],
    ):
        super().__init__(field_type=concept_type, next_node=next_node)

    def is_mfg_missing_data(
        self,
        mfg: Manufacturer,
    ) -> bool:
        return not bool(getattr(mfg, self.field_type.name))

    async def is_deferred_mfg_missing_any_requests(
        self, deferred_mfg: DeferredManufacturer
    ) -> bool:
        deferred_concept_extraction: Optional[DeferredConceptExtraction] = getattr(
            deferred_mfg, self.field_type.name
        )
        if not deferred_concept_extraction:
            return True

        # Check if all search requests exist
        llm_search_req_ids_to_lookup: set[GPTBatchRequestCustomID] = set()
        for (
            _chunk_bounds,
            extraction_bundle,
        ) in deferred_concept_extraction.chunk_request_bundle_map.items():
            llm_search_req_ids_to_lookup.add(extraction_bundle.llm_search_request_id)

        gpt_req_ids_missing = llm_search_req_ids_to_lookup - (
            await find_gpt_batch_request_ids_only(list(llm_search_req_ids_to_lookup))
        )

        if gpt_req_ids_missing:
            return True

        # because otherwise even though deferred_address_extraction exists,
        # the batch request wasn't created for some reason
        return False

    async def are_all_deferred_mfg_requests_complete(
        self, deferred_mfg: DeferredManufacturer
    ) -> bool:
        deferred_concept_extraction: Optional[DeferredConceptExtraction] = getattr(
            deferred_mfg, self.field_type.name
        )
        if not deferred_concept_extraction:
            raise ValueError(
                f"are_all_deferred_mfg_requests_complete was called for {self.field_type.name} in {__class__.__name__} but no deferred concept extraction exists."
            )

        # Check if all search requests are complete
        llm_search_req_ids_to_lookup: set[GPTBatchRequestCustomID] = set()
        for (
            _chunk_bounds,
            extraction_bundle,
        ) in deferred_concept_extraction.chunk_request_bundle_map.items():
            llm_search_req_ids_to_lookup.add(extraction_bundle.llm_search_request_id)

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

        deferred_concept_extraction: Optional[DeferredConceptExtraction] = getattr(
            deferred_mfg, self.field_type.name
        )

        # get_missing_concept_mapping_requests only creates batch requests fresh or only missing ones,
        # for e.g., new mfg or some batch requests failed earlier and were deleted to allow re-processing
        updated_deferred_concept_extraction, batch_requests = (
            await get_missing_concept_search_requests(
                deferred_at=timestamp,
                concept_type=self.field_type,
                deferred_concept_extraction=deferred_concept_extraction,
                mfg_etld1=mfg.etld1,
                mfg_text=scraped_text_file.text,
            )
        )

        # Update deferred_mfg in memory only with new concept extraction
        setattr(deferred_mfg, self.field_type.name, updated_deferred_concept_extraction)

        return batch_requests
