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

from core.models.db.deferred_manufacturer import DeferredManufacturer
from core.models.db.gpt_batch_request import GPTBatchRequest
from core.models.db.gpt_batch import GPTBatch

from core.models.db.manufacturer_user_form import ManufacturerUserForm
from core.models.db.binary_ground_truth import BinaryGroundTruth
from core.models.db.concept_ground_truth import ConceptGroundTruth
from core.models.db.keyword_ground_truth import KeywordGroundTruth


MONGO_DB_URI = os.getenv("MONGO_DB_URI")
if not MONGO_DB_URI:
    raise ValueError("MONGO_DB_URI environment variable is not set.")


class Settings(BaseSettings):
    MONGO_URI: str = MONGO_DB_URI


settings = Settings()


async def init_db(
    max_pool_size: int = 20,
    min_pool_size: int = 5,
    max_idle_time_ms: int = 45000,
    server_selection_timeout_ms: int = 20000,
    connect_timeout_ms: int = 20000,
    socket_timeout_ms: int = 45000,
):
    # Configure codec options for timezone awareness
    codec_options = CodecOptions(tz_aware=True, tzinfo=timezone.utc)

    # Create client with optimized connection pool settings
    client = AsyncMongoClient(
        settings.MONGO_URI,
        maxPoolSize=max_pool_size,
        minPoolSize=min_pool_size,
        maxIdleTimeMS=max_idle_time_ms,
        serverSelectionTimeoutMS=server_selection_timeout_ms,
        connectTimeoutMS=connect_timeout_ms,
        socketTimeoutMS=socket_timeout_ms,
        # Additional performance optimizations
        retryWrites=True,
        retryReads=True,
        w="majority",  # Write concern for durability
        readPreference="primary",  # Only read from primary
        # Connection pool monitoring
        waitQueueTimeoutMS=30000,  # Wait up to 30s for connection from pool
    )

    # Get database with timezone-aware codec options
    database = client.get_default_database().with_options(codec_options=codec_options)

    print("Initializing Beanie connection to MongoDB...")
    if "example" in settings.MONGO_URI:
        print("WARNING: Using local MongoDB credentials.")

    print(
        f"MongoDB connection pool: maxPoolSize={max_pool_size}, minPoolSize={min_pool_size}"
    )

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
            DeferredManufacturer,
            GPTBatchRequest,
            GPTBatch,
            User,
        ],
    )


async def get_mongo_database():
    """Get direct access to MongoDB database for raw collection operations."""
    codec_options = CodecOptions(tz_aware=True, tzinfo=timezone.utc)
    client = AsyncMongoClient(settings.MONGO_URI)
    return client.get_default_database().with_options(codec_options=codec_options)
