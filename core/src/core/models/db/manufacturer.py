from beanie import Document
from pydantic import BaseModel, Field
from datetime import datetime
import logging
from typing import List, Optional

from core.models.field_types import MfgURLType, MfgETLDType, S3FileVersionIDType
from core.models.concept_extraction_results import ConceptExtractionResults
from core.models.keyword_extraction_results import KeywordExtractionResults
from core.models.binary_classification_result import (
    BinaryClassificationResult,
)
from core.utils.time_util import get_current_time

logger = logging.getLogger(__name__)


class Address(BaseModel):
    city: str
    state: str
    country: str = "US"
    name: Optional[str] = None
    address_lines: Optional[list[str]] = None
    county: Optional[str] = None
    postal_code: Optional[str] = None

    # Geolocation fields
    latitude: Optional[float] = None
    longitude: Optional[float] = None

    # Contact fields
    phone_numbers: Optional[list[str]] = None
    fax_numbers: Optional[list[str]] = None

    def base_hash(self) -> str:
        return f"{self.city}-{self.state}-{self.country}"


class Batch(BaseModel):
    title: str
    timestamp: datetime

    # @field_validator("title")
    # @classmethod
    # def validate_title_not_empty(cls, v: str) -> str:
    #     if not v.strip():
    #         raise ValueError("Batch title cannot be empty")
    #     return v.strip()

    # @field_validator("timestamp")
    # @classmethod
    # def validate_timestamp(cls, v: datetime) -> datetime:
    #     if not isinstance(v, datetime):
    #         raise ValueError("timestamp must be a valid datetime object")
    #     return v


class BusinessDescriptionResult(BaseModel):
    name: Optional[str]
    description: Optional[str]


class Manufacturer(Document):
    etld1: MfgETLDType  # effective top-level domain plus one, e.g. "example.com"; ".com" is top level domain, "example" is the second-level domain
    url_accessible_at: MfgURLType

    created_at: datetime = Field(default_factory=lambda: get_current_time())
    updated_at: datetime = Field(default_factory=lambda: get_current_time())
    scraped_text_file_num_tokens: int
    scraped_text_file_version_id: S3FileVersionIDType
    batches: list[Batch]

    name: Optional[str]

    is_manufacturer: Optional[BinaryClassificationResult]
    is_contract_manufacturer: Optional[BinaryClassificationResult]
    is_product_manufacturer: Optional[BinaryClassificationResult]

    founded_in: Optional[int]
    email_addresses: Optional[List[str]]
    num_employees: Optional[int]
    business_statuses: Optional[List[str]]
    primary_naics: Optional[str]
    secondary_naics: Optional[List[str]]

    # LLM extracted fields
    addresses: Optional[List[Address]]
    business_desc: Optional[BusinessDescriptionResult]

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
