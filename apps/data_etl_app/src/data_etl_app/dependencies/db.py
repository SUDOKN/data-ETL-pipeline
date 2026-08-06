from pymongo import AsyncMongoClient

from infra.utils.db_clients.mongo_client import init_db

from data_etl_app.db_models import DOCUMENT_MODELS


async def init_app_db(**kwargs) -> AsyncMongoClient:
    """Initialize Beanie with every document model this app uses."""
    return await init_db(DOCUMENT_MODELS, **kwargs)
