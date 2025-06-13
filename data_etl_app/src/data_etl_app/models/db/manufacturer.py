from beanie import Document
from datetime import datetime, timezone
from pydantic import BaseModel, Field
from typing import List, Optional

from data_etl_app.models.db.binary_classifier_result import (
    BinaryClassifierResult_DBModel,
)
from data_etl_app.models.db.extraction_results import ExtractionResults_DBModel


class Address(BaseModel):
    line_1: str
    city: Optional[str]
    county: Optional[str]
    state: Optional[str]
    zip: Optional[str]
    latitude: Optional[float]
    longitude: Optional[float]
    phone_num: Optional[str]
    fax_num: Optional[str]


class NAICSEntry(BaseModel):
    code: str
    desc: Optional[str]


class Manufacturer(Document):
    url: str
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    global_id: str  # not unique
    name: str
    num_employees: Optional[int]
    business_desc: Optional[str]
    data_src: str

    is_manufacturer: Optional[BinaryClassifierResult_DBModel]
    is_contract_manufacturer: Optional[BinaryClassifierResult_DBModel]
    is_product_manufacturer: Optional[BinaryClassifierResult_DBModel]

    business_statuses: List[str]
    products_old: List[str]
    addresses: List[Address]
    naics: Optional[List[NAICSEntry]]

    certificates: Optional[ExtractionResults_DBModel]
    industries: Optional[ExtractionResults_DBModel]
    process_caps: Optional[ExtractionResults_DBModel]
    material_caps: Optional[ExtractionResults_DBModel]

    class Settings:
        name = "manufacturers"
