import os
from beanie import init_beanie  # type: ignore
from motor.motor_asyncio import AsyncIOMotorClient
from pydantic_settings import BaseSettings

from shared.models.db.manufacturer import Manufacturer
from shared.models.db.extraction_error import ExtractionError
from shared.models.db.scraping_error import ScrapingError
from shared.models.db.user import User

from data_etl_app.models.keyword_ground_truth import KeywordGroundTruth
from data_etl_app.models.binary_ground_truth import BinaryGroundTruth

MONGO_DB_URI = os.getenv("MONGO_DB_URI")
if not MONGO_DB_URI:
    raise ValueError("MONGO_DB_URI environment variable is not set.")


# 1. Define Pydantic Settings for DB
class Settings(BaseSettings):
    MONGO_URI: str = (
        # "mongodb://root:example@localhost:27017/sudokn?authSource=admin&directConnection=true"
        # "mongodb://root:M!o2N%23g4O%25@18.207.184.14:27017/sudokn?authSource=admin&directConnection=true"
        MONGO_DB_URI
    )


settings = Settings()


async def init_db():
    client = AsyncIOMotorClient(settings.MONGO_URI)  # type: ignore
    return await init_beanie(
        database=client.get_default_database(),
        document_models=[
            Manufacturer,
            ExtractionError,
            ScrapingError,
            KeywordGroundTruth,
            BinaryGroundTruth,
            User,
        ],
    )
