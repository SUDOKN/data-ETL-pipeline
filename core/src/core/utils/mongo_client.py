import os
from beanie import init_beanie
from pymongo import AsyncMongoClient
from pydantic_settings import BaseSettings
from bson.codec_options import CodecOptions
from datetime import timezone

from core.models.db.manufacturer import Manufacturer
from core.models.db.extraction_error import ExtractionError
from core.models.db.scraping_error import ScrapingError
from core.models.db.user import User

from data_etl_app.models.db.manufacturer_user_form import ManufacturerUserForm
from data_etl_app.models.db.binary_ground_truth import BinaryGroundTruth
from data_etl_app.models.db.concept_ground_truth import ConceptGroundTruth
from data_etl_app.models.db.keyword_ground_truth import KeywordGroundTruth


MONGO_DB_URI = os.getenv("MONGO_DB_URI")
if not MONGO_DB_URI:
    raise ValueError("MONGO_DB_URI environment variable is not set.")


class Settings(BaseSettings):
    MONGO_URI: str = MONGO_DB_URI


settings = Settings()


async def init_db():
    # Configure codec options for timezone awareness
    codec_options = CodecOptions(tz_aware=True, tzinfo=timezone.utc)

    client = AsyncMongoClient(settings.MONGO_URI)  # <-- switched here

    # Get database with timezone-aware codec options
    database = client.get_default_database().with_options(codec_options=codec_options)

    print("Initializing Beanie connection to MongoDB...")
    if "example" in settings.MONGO_URI:
        print("WARNING: Using local MongoDB credentials.")
    return await init_beanie(
        database=database,
        document_models=[
            Manufacturer,
            ManufacturerUserForm,
            ExtractionError,
            ScrapingError,
            BinaryGroundTruth,
            ConceptGroundTruth,
            KeywordGroundTruth,
            User,
        ],
    )
