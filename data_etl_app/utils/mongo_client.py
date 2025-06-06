from beanie import init_beanie  # type: ignore
from motor.motor_asyncio import AsyncIOMotorClient
from pydantic_settings import BaseSettings

from models.db.manufacturer import Manufacturer
from models.db.extraction_error import ExtractionError


# 1. Define Pydantic Settings for DB
class Settings(BaseSettings):
    # MONGO_URI: str = "mongodb://52.73.155.98:27017/sudokn"
    MONGO_URI: str = (
        # "mongodb://root:example@localhost:27017/sudokn?authSource=admin&directConnection=true"
        "mongodb://root:M!o2N%23g4O%25@18.207.184.14:27017/sudokn?authSource=admin&directConnection=true"
    )


settings = Settings()


async def init_db():
    client = AsyncIOMotorClient(settings.MONGO_URI)  # type: ignore
    return await init_beanie(
        database=client.get_default_database(),
        document_models=[Manufacturer, ExtractionError],
    )
