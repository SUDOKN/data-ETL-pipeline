from beanie import Document, init_beanie # type: ignore
from motor.motor_asyncio import AsyncIOMotorClient
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings
from typing import List, Optional
from datetime import datetime, timezone

from services.binary_classifier import BinaryClassifierResult
from models.extraction import (
    ExtractedResults,
)


# 1. Define Pydantic Settings for DB
class Settings(BaseSettings):
    # MONGO_URI: str = "mongodb://52.73.155.98:27017/sudokn"
    MONGO_URI: str = (
        # "mongodb://root:example@localhost:27017/sudokn?authSource=admin&directConnection=true"
        "mongodb://root:M!o2N%23g4O%25@18.207.184.14:27017/sudokn?authSource=admin&directConnection=true"
    )


settings = Settings()





class MBinaryClassifierResult(BaseModel):
    name: Optional[str] = Field(default=None)
    answer: bool
    explanation: str

    @classmethod
    def from_dict(cls, binary_classifier_result: BinaryClassifierResult):
        return cls(
            name=binary_classifier_result.get("name"),
            answer=binary_classifier_result.get("answer"),
            explanation=binary_classifier_result.get("explanation"),
        )


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

    is_manufacturer: Optional[MBinaryClassifierResult]
    is_contract_manufacturer: Optional[MBinaryClassifierResult]
    is_product_manufacturer: Optional[MBinaryClassifierResult]

    business_statuses: List[str]
    products_old: List[str]
    addresses: List[Address]
    naics: Optional[List[NAICSEntry]]

    certificates: Optional[ExtractedResults]
    industries: Optional[ExtractedResults]
    process_caps: Optional[ExtractedResults]
    material_caps: Optional[ExtractedResults]

    class Settings:
        name = "manufacturers"


class ExtractionError(Document):
    error: str
    url: str
    name: str
    field: str
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    class Settings:
        name = "extraction_errors"


# 3. Initialize Beanie (Call once at startup)
async def init_db():
    client = AsyncIOMotorClient(settings.MONGO_URI) # type: ignore
    return await init_beanie(
        database=client.get_default_database(),
        document_models=[Manufacturer, ExtractionError],
    )
