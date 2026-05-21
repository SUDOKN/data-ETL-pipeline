import logging
from beanie import Document
from datetime import datetime
from pydantic import BaseModel, Field
from typing import List, Optional

from core.models.field_types import MfgURLType, MfgETLDType, S3FileVersionIDType
from core.models.concept_extraction_results import ConceptExtractionResults
from core.models.keyword_extraction_results import KeywordExtractionResults
from core.models.binary_classification_result import (
    BinaryClassificationResult,
)
from core.models.address_extraction_result import (
    AddressExtractionResult,
)
from core.models.business_description_extraction_result import (
    BusinessDescriptionExtractionResult,
)

from core.utils.time_util import get_current_time

logger = logging.getLogger(__name__)


class Batch(BaseModel):
    title: str
    timestamp: datetime


class Manufacturer(Document):
    etld1: MfgETLDType  # effective top-level domain plus one, e.g. "example.com"; ".com" is top level domain, "example" is the second-level domain
    url_accessible_at: MfgURLType

    created_at: datetime = Field(default_factory=lambda: get_current_time())
    updated_at: datetime = Field(default_factory=lambda: get_current_time())
    scraped_text_file_num_tokens: int
    scraped_text_file_version_id: S3FileVersionIDType
    batches: list[Batch]

    name: Optional[str]
    founded_in: Optional[int]
    email_addresses: Optional[List[str]]
    num_employees: Optional[int]
    business_statuses: Optional[List[str]]
    primary_naics: Optional[str]
    secondary_naics: Optional[List[str]]

    # LLM extracted fields
    is_manufacturer: Optional[BinaryClassificationResult]
    is_contract_manufacturer: Optional[BinaryClassificationResult]
    is_product_manufacturer: Optional[BinaryClassificationResult]
    addresses: Optional[AddressExtractionResult]
    business_desc: Optional[BusinessDescriptionExtractionResult]

    products: Optional[KeywordExtractionResults]

    certificates: Optional[ConceptExtractionResults]
    industries: Optional[ConceptExtractionResults]
    process_caps: Optional[ConceptExtractionResults]
    material_caps: Optional[ConceptExtractionResults]

    class Settings:
        name = "manufacturers"


"""
Indexes for Manufacturers

db.manufacturers.createIndex(
  {
    etld1: 1,
  },
  {
    name: "mfg_etld1_unique_idx",
    unique: true
  }
);

db.manufacturers.createIndex(
  {
    url_accessible_at: 1,
  },
  {
    name: "mfg_url_accessible_at_idx",
    unique: true
  }
);
"""
