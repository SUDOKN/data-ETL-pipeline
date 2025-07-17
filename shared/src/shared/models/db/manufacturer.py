from beanie import Document
from pydantic import BaseModel, Field, field_validator
from datetime import datetime
import logging
from typing import List, Optional

from shared.models.types import MfgURLType
from shared.models.db.extraction_results import ExtractionResults
from shared.utils.time_util import get_current_time
from shared.utils.url_util import canonical_host

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


class BinaryClassifierResult(BaseModel):
    evaluated_at: datetime
    answer: bool
    confidence: int
    reason: str


class IsManufacturerResult(BinaryClassifierResult):
    name: str


class Manufacturer(Document):
    url: MfgURLType
    created_at: datetime = Field(default_factory=lambda: get_current_time())
    updated_at: datetime = Field(default_factory=lambda: get_current_time())
    scraped_text_file_num_tokens: int
    scraped_text_file_version_id: str
    batches: list[Batch]

    name: Optional[str]

    is_manufacturer: Optional[IsManufacturerResult]
    is_contract_manufacturer: Optional[BinaryClassifierResult]
    is_product_manufacturer: Optional[BinaryClassifierResult]

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

    @field_validator("url", mode="before")
    @classmethod
    def normalize_url(cls, v: str) -> str:
        """
        This ensures that every instance of Manufacturer has a canonicalized url,
        whether from .find_one(), .insert(), or deserialization.
        """
        retval = canonical_host(v)
        if not retval:
            raise ValueError(f"Invalid URL: '{v}' has no valid hostname.")
        return retval

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

    class Settings:
        name = "manufacturers"
