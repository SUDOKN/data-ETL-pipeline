from typing import Optional
from beanie import Document
from pydantic import Field
from datetime import datetime
import logging

from core.models.field_types import MfgETLDType, S3FileVersionIDType
from core.utils.time_util import get_current_time


from core.models.deferred_single_stage_extraction_requests import (
    DeferredSingleStageExtractionRequests,
)
from core.models.deferred_search_requests import (
    DeferredSearchRequests,
)
from core.models.deferred_concept_extraction import (
    DeferredConceptExtractionRequests,
)

logger = logging.getLogger(__name__)


class DeferredManufacturer(Document):
    mfg_etld1: MfgETLDType
    created_at: datetime = Field(default_factory=lambda: get_current_time())
    updated_at: datetime = Field(default_factory=lambda: get_current_time())
    scraped_text_file_num_tokens: int
    scraped_text_file_version_id: S3FileVersionIDType

    is_manufacturer: Optional[DeferredSingleStageExtractionRequests]
    is_contract_manufacturer: Optional[DeferredSingleStageExtractionRequests]
    is_product_manufacturer: Optional[DeferredSingleStageExtractionRequests]
    addresses: Optional[DeferredSingleStageExtractionRequests]
    business_desc: Optional[DeferredSingleStageExtractionRequests]

    products: Optional[DeferredSearchRequests]

    certificates: Optional[DeferredConceptExtractionRequests]
    industries: Optional[DeferredConceptExtractionRequests]
    process_caps: Optional[DeferredConceptExtractionRequests]
    material_caps: Optional[DeferredConceptExtractionRequests]

    class Settings:
        name = "deferred_manufacturers"


"""
Indices for Manufacturers

db.deferred_manufacturers.createIndex(
  {
    mfg_etld1: 1,
  },
  {
    name: "mfg_etld1_unique_idx",
    unique: true
  }
);
"""
