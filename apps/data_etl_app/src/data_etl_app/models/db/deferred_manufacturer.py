from typing import Optional
from beanie import Document
from pydantic import Field
from datetime import datetime
import logging

from packages.core.src.core.models.field_types import MfgETLDType, S3FileVersionIDType
from packages.core.src.core.models.base.extraction_subject import (
    AbstractDeferredExtractionSubject,
)
from packages.core.src.core.models.deferred_extraction.deferred_single_stage_extraction_requests import (
    DeferredSingleStageExtractionRequests,
)
from packages.core.src.core.models.deferred_extraction.deferred_keyword_extraction import (
    DeferredKeywordExtractionRequests,
)
from packages.core.src.core.models.deferred_extraction.deferred_concept_extraction import (
    DeferredConceptExtractionRequests,
)

from packages.core.src.core.utils.time_util import get_current_time

logger = logging.getLogger(__name__)


class DeferredManufacturer(Document, AbstractDeferredExtractionSubject):
    etld1: MfgETLDType
    created_at: datetime = Field(default_factory=lambda: get_current_time())
    updated_at: datetime = Field(default_factory=lambda: get_current_time())
    scraped_text_file_num_tokens: int
    scraped_text_file_version_id: S3FileVersionIDType

    is_manufacturer: Optional[DeferredSingleStageExtractionRequests]
    is_contract_manufacturer: Optional[DeferredSingleStageExtractionRequests]
    is_product_manufacturer: Optional[DeferredSingleStageExtractionRequests]
    addresses: Optional[DeferredSingleStageExtractionRequests]
    business_desc: Optional[DeferredSingleStageExtractionRequests]

    products: Optional[DeferredKeywordExtractionRequests]
    contract_products: Optional[DeferredKeywordExtractionRequests]
    equipments: Optional[DeferredKeywordExtractionRequests]

    certificates: Optional[DeferredConceptExtractionRequests]
    industries: Optional[DeferredConceptExtractionRequests]
    process_caps: Optional[DeferredConceptExtractionRequests]
    material_caps: Optional[DeferredConceptExtractionRequests]

    @property
    def subject_unique_id(self) -> str:
        return self.etld1

    class Settings:
        name = "deferred_manufacturers"


"""
Indices for Manufacturers

db.deferred_manufacturers.createIndex(
  {
    etld1: 1,
  },
  {
    name: "deferred_mfg_etld1_unique_idx",
    unique: true
  }
);
db.deferred_manufacturers.createIndex(
  {
    etld1: 1,
    scraped_text_file_version_id: 1,
  },
  {
    name: "deferred_mfg_compound_idx"
  }
);
"""
