import os
from beanie import init_beanie
from pymongo import AsyncMongoClient  # <-- switched from Motor
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


class Settings(BaseSettings):
    MONGO_URI: str = MONGO_DB_URI


settings = Settings()


async def init_db():
    client = AsyncMongoClient(settings.MONGO_URI)  # <-- switched here
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
