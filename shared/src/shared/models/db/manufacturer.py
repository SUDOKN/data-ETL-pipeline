from beanie import Document
from pydantic import BaseModel, Field, field_validator, model_validator
from datetime import datetime
import logging
from typing import List, Optional
from urllib.parse import urlparse

from shared.models.types import MfgURLType, MfgETLDType
from shared.models.extraction_results import ExtractionResults
from shared.models.binary_classification import (
    BinaryClassificationResult,
)
from shared.utils.time_util import get_current_time
from shared.utils.url_util import get_normalized_url, get_etld1_from_host

logger = logging.getLogger(__name__)


class Address(BaseModel):
    line_1: str
    city: str
    county: str
    state: str
    zip: str
    latitude: float
    longitude: float
    phone_num: Optional[str]
    fax_num: Optional[str]


class Product(BaseModel):
    name: str
    description: Optional[str]


class Batch(BaseModel):
    title: str
    timestamp: datetime

    @field_validator("title")
    @classmethod
    def validate_title_not_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("Batch title cannot be empty")
        return v.strip()

    @field_validator("timestamp")
    @classmethod
    def validate_timestamp(cls, v: datetime) -> datetime:
        if not isinstance(v, datetime):
            raise ValueError("timestamp must be a valid datetime object")
        return v


class IsManufacturerResult(BinaryClassificationResult):
    name: str


class Manufacturer(Document):
    etld1: MfgETLDType  # effective top-level domain plus one, e.g. "example.com"; ".com" is top level domain, "example" is the second-level domain
    url_accessible_at: MfgURLType

    created_at: datetime = Field(default_factory=lambda: get_current_time())
    updated_at: datetime = Field(default_factory=lambda: get_current_time())
    scraped_text_file_num_tokens: int
    scraped_text_file_version_id: str
    batches: list[Batch]

    name: Optional[str]

    is_manufacturer: Optional[IsManufacturerResult]
    is_contract_manufacturer: Optional[BinaryClassificationResult]
    is_product_manufacturer: Optional[BinaryClassificationResult]

    founded_in: Optional[int]
    num_employees: Optional[int]
    business_desc: Optional[str]
    business_statuses: Optional[List[str]]
    primary_naics: Optional[str]
    secondary_naics: Optional[List[str]]
    addresses: Optional[List[Address]]

    products: Optional[List[Product]]

    certificates: Optional[ExtractionResults]
    industries: Optional[ExtractionResults]
    process_caps: Optional[ExtractionResults]
    material_caps: Optional[ExtractionResults]

    @field_validator("url_accessible_at", mode="before")
    @classmethod
    def ensure_valid_url_accessible_at(cls, v: str) -> str:
        """
        This ensures that every instance of Manufacturer has a valid normalized url_accessible_at but doesn't check for accessibility.
        The user must ensure that the URL is accessible.
        """
        try:
            _, url = get_normalized_url(v)
            return url
        except ValueError as e:
            logger.error(f"Failed to normalize URL '{v}': {e}")
            raise

    @field_validator("batches")
    @classmethod
    def validate_batches(cls, value):
        """
        Validates that batches is a list of Batch instances and optionally checks
        that it is sorted by timestamp in descending order (latest first).
        """
        logger.debug(f"validate_batches value: {value}")
        if not isinstance(value, list):
            raise ValueError("batches must be a list")
        if len(value) < 1:
            raise ValueError("batches must contain at least one batch")

        for item in value:
            if not isinstance(item, Batch):
                raise ValueError("All items in batches must be Batch instances")
        # Optionally, ensure sorted by timestamp descending
        timestamps = [b.timestamp for b in value]
        if timestamps != sorted(timestamps):
            raise ValueError(
                "batches must be sorted by timestamp, ascending (oldest first)"
            )
        return value

    @model_validator(mode="after")
    def validate_etld1_url_consistency(self):
        """
        Validates that the etld1 field matches the etld1 derived from url_accessible_at.
        This ensures consistency between the two fields.
        """
        try:
            parsed_url = urlparse(self.url_accessible_at)
            url_etld1 = get_etld1_from_host(parsed_url.netloc)

            if self.etld1 != url_etld1:
                raise ValueError(
                    f"etld1 mismatch: provided '{self.etld1}' does not match "
                    f"etld1 derived from url_accessible_at '{url_etld1}'"
                )
        except Exception as e:
            logger.error(f"Failed to validate etld1 consistency: {e}")
            raise

        return self

    class Settings:
        name = "manufacturers"
