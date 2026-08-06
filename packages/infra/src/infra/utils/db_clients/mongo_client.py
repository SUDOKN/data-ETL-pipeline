import hashlib
import logging
from collections.abc import Sequence
from datetime import timezone

from beanie import Document, init_beanie
from bson.codec_options import CodecOptions
from pymongo import AsyncMongoClient

from pure_utils.env_util import require_env

logger = logging.getLogger(__name__)

# Beanie binds collection state onto the Document classes themselves, so a second
# init against a different database would silently rebind every model process-wide.
_initialized_target: str | None = None


def _build_client(
    uri: str,
    max_pool_size: int,
    min_pool_size: int,
    max_idle_time_ms: int,
    server_selection_timeout_ms: int,
    connect_timeout_ms: int,
    socket_timeout_ms: int,
) -> AsyncMongoClient:
    return AsyncMongoClient(
        uri,
        maxPoolSize=max_pool_size,
        minPoolSize=min_pool_size,
        maxIdleTimeMS=max_idle_time_ms,
        serverSelectionTimeoutMS=server_selection_timeout_ms,
        connectTimeoutMS=connect_timeout_ms,
        socketTimeoutMS=socket_timeout_ms,
        retryWrites=True,
        retryReads=True,
        w="majority",  # Write concern for durability
        readPreference="primary",  # Only read from primary
        waitQueueTimeoutMS=30000,  # Wait up to 30s for connection from pool
    )


def _target_key(uri: str, database_name: str) -> str:
    """Identify a connection target without exposing credentials from the URI."""
    return f"{database_name}@{hashlib.sha256(uri.encode()).hexdigest()[:12]}"


async def init_db(
    document_models: Sequence[type[Document]],
    uri: str | None = None,
    max_pool_size: int = 20,
    min_pool_size: int = 5,
    max_idle_time_ms: int = 45000,
    server_selection_timeout_ms: int = 20000,
    connect_timeout_ms: int = 20000,
    socket_timeout_ms: int = 45000,
) -> AsyncMongoClient:
    """
    Initialize Beanie with the given document models and return the underlying client.

    Callers own the returned client's lifecycle and should `await client.close()` on shutdown.
    """
    global _initialized_target

    if not document_models:
        raise ValueError("init_db requires at least one Beanie document model.")

    resolved_uri = uri or require_env("MONGO_DB_URI")

    client = _build_client(
        resolved_uri,
        max_pool_size,
        min_pool_size,
        max_idle_time_ms,
        server_selection_timeout_ms,
        connect_timeout_ms,
        socket_timeout_ms,
    )
    codec_options = CodecOptions(tz_aware=True, tzinfo=timezone.utc)
    database = client.get_default_database().with_options(codec_options=codec_options)

    target = _target_key(resolved_uri, database.name)
    if _initialized_target is not None and _initialized_target != target:
        await client.close()
        raise RuntimeError(
            "Beanie is already initialized against a different database. "
            "init_beanie rebinds Document class state process-wide, so a second "
            f"target ({database.name}) would break every model bound to the first."
        )

    deduped_models = list(dict.fromkeys(document_models))

    logger.info(
        "Initializing Beanie connection to MongoDB database '%s'", database.name
    )
    if "example" in resolved_uri:
        logger.warning("Using local MongoDB credentials.")
    logger.info(
        "MongoDB connection pool: maxPoolSize=%s, minPoolSize=%s",
        max_pool_size,
        min_pool_size,
    )

    await init_beanie(database=database, document_models=deduped_models)
    _initialized_target = target
    return client


async def get_mongo_database(
    CUSTOM_MONGO_URI: str,
    max_pool_size: int = 20,
    min_pool_size: int = 5,
    max_idle_time_ms: int = 45000,
    server_selection_timeout_ms: int = 20000,
    connect_timeout_ms: int = 20000,
    socket_timeout_ms: int = 45000,
):
    """Get direct access to MongoDB database for raw collection operations."""
    codec_options = CodecOptions(tz_aware=True, tzinfo=timezone.utc)
    client = _build_client(
        CUSTOM_MONGO_URI,
        max_pool_size,
        min_pool_size,
        max_idle_time_ms,
        server_selection_timeout_ms,
        connect_timeout_ms,
        socket_timeout_ms,
    )
    return client.get_default_database().with_options(codec_options=codec_options)
