import asyncio
import os
import logging
from aiobotocore.session import get_session, AioSession
from types_aiobotocore_s3.client import S3Client
from types_aiobotocore_sqs.client import SQSClient
from typing import AsyncContextManager, Optional, cast


logger = logging.getLogger(__name__)


class DataETLAWSClients:
    """Manages AWS clients at the data ETL application level."""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._initialized = False
        self.session: Optional[AioSession] = None

        # Async context managers (for cleanup)
        self.prompt_rdf_s3_client_ctx: Optional[AsyncContextManager[S3Client]] = None

        # Active clients
        self.prompt_rdf_s3_client: Optional[S3Client] = None

    async def initialize(self) -> None:
        if self._initialized:
            return

        async with self._lock:
            if self._initialized:
                return
            logger.info("Initializing data ETL AWS clients...")
            self.session = get_session()

            await self.initialize_prompt_rdf_s3_client(self.session)

            self._initialized = True
            logger.info("Data ETL AWS clients initialized")

    async def initialize_prompt_rdf_s3_client(self, session: AioSession) -> None:
        if self.prompt_rdf_s3_client:
            return

        AWS_REGION = os.getenv("AWS_REGION")
        USER_ACCESS_KEY_ID = os.getenv("AWS_RDF_AND_PROMPT_USER_ACCESS_KEY_ID")
        USER_SECRET_ACCESS_KEY = os.getenv("AWS_RDF_AND_PROMPT_USER_SECRET_ACCESS_KEY")

        if not (AWS_REGION and USER_ACCESS_KEY_ID and USER_SECRET_ACCESS_KEY):
            raise ValueError(
                "AWS RDF and Prompt S3 credentials or region are not set. Please set them in your .env file."
            )

        self.prompt_rdf_s3_client_ctx = cast(
            AsyncContextManager[S3Client],
            session.create_client(
                "s3",
                region_name=AWS_REGION,
                aws_access_key_id=USER_ACCESS_KEY_ID,
                aws_secret_access_key=USER_SECRET_ACCESS_KEY,
            ),
        )
        self.prompt_rdf_s3_client = await self.prompt_rdf_s3_client_ctx.__aenter__()
        logger.info("Prompt and RDF S3 client initialized successfully")

    async def cleanup(self) -> None:
        if not self._initialized:
            return
        async with self._lock:
            logger.info("Cleaning up data ETL AWS clients...")
            if self.prompt_rdf_s3_client_ctx and self.prompt_rdf_s3_client:
                await self.prompt_rdf_s3_client_ctx.__aexit__(None, None, None)
            self._initialized = False
            logger.info("Data ETL AWS clients cleaned up")


# Keep the instance private; expose only functions.
_data_etl_aws_clients = DataETLAWSClients()


async def initialize_data_etl_aws_clients() -> None:
    await _data_etl_aws_clients.initialize()


async def cleanup_data_etl_aws_clients() -> None:
    await _data_etl_aws_clients.cleanup()


def get_prompt_rdf_s3_client() -> S3Client:
    if _data_etl_aws_clients.prompt_rdf_s3_client is None:
        raise RuntimeError(
            "S3 client not initialized. Call initialize_data_etl_aws_clients() at app startup."
        )
    return _data_etl_aws_clients.prompt_rdf_s3_client


__all__ = [
    "initialize_data_etl_aws_clients",
    "cleanup_data_etl_aws_clients",
    "get_prompt_rdf_s3_client",
]
