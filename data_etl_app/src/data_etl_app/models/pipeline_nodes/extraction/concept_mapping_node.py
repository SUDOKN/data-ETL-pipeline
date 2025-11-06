import logging
from datetime import datetime
from typing import Optional

from core.models.db.manufacturer import Manufacturer
from data_etl_app.models.pipeline_nodes.reconcile.concept_reconcile_node import (
    ConceptReconcileNode,
)
from core.models.db.deferred_manufacturer import DeferredManufacturer
from core.models.db.gpt_batch_request import GPTBatchRequest
from data_etl_app.models.types_and_enums import ConceptTypeEnum
from core.models.deferred_concept_extraction import (
    DeferredConceptExtraction,
)
from data_etl_app.models.pipeline_nodes.extraction.extraction_node import (
    ExtractionNode,
)
from data_etl_app.services.llm_powered.extraction.extract_concept_deferred_service import (
    get_missing_concept_mapping_request,
)
from core.services.gpt_batch_request_service import (
    find_gpt_batch_request_by_custom_id,
    find_completed_gpt_batch_request_by_custom_id,
)
from scraper_app.models.scraped_text_file import ScrapedTextFile

logger = logging.getLogger(__name__)


class ConceptMappingNode(ExtractionNode):
    """Phase 2: Map unknown terms to known ontology"""

    field_type: ConceptTypeEnum
    next_node: Optional[ConceptReconcileNode]

    def __init__(
        self,
        concept_type: ConceptTypeEnum,
        next_node: Optional[ConceptReconcileNode],
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

        # else check if llm mapping GPT request exists
        if deferred_concept_extraction.llm_mapping_request_id is None:
            return True

        return not bool(
            await find_gpt_batch_request_by_custom_id(
                deferred_concept_extraction.llm_mapping_request_id
            )
        )

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

        # Check if the mapping request is complete
        if deferred_concept_extraction.llm_mapping_request_id is None:
            return True

        return bool(
            await find_completed_gpt_batch_request_by_custom_id(
                deferred_concept_extraction.llm_mapping_request_id
            )
        )

    async def create_batch_requests(
        self,
        mfg: Manufacturer,
        deferred_mfg: DeferredManufacturer,
        scraped_text_file: ScrapedTextFile,
        timestamp: datetime,
    ) -> list[GPTBatchRequest]:
        """Create batch requests for concept mapping phase."""

        deferred_concept_extraction: Optional[DeferredConceptExtraction] = getattr(
            deferred_mfg, self.field_type.name
        )
        if not deferred_concept_extraction:
            raise ValueError(
                f"create_batch_requests was called for {self.field_type.name} but no deferred concept extraction exists."
            )

        updated_deferred_concept_extraction, batch_request = (
            await get_missing_concept_mapping_request(
                deferred_at=timestamp,
                deferred_concept_extraction=deferred_concept_extraction,
                mfg_etld1=mfg.etld1,
                concept_type=self.field_type,
            )
        )

        # Update deferred_mfg in memory only with new concept extraction
        setattr(deferred_mfg, self.field_type.name, updated_deferred_concept_extraction)

        return [batch_request]
