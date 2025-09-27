import asyncio
import os
import logging
from aiobotocore.session import get_session, AioSession
from types_aiobotocore_s3.client import S3Client
from types_aiobotocore_sqs.client import SQSClient
from typing import AsyncContextManager, Optional, cast


logger = logging.getLogger(__name__)


class SharedAWSClients:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._initialized = False
        self.session: Optional[AioSession] = None

        # Async context managers (for cleanup)
        self.scraped_bucket_s3_client_ctx: Optional[AsyncContextManager[S3Client]] = (
            None
        )
        self.scrape_queue_client_ctx: Optional[AsyncContextManager[SQSClient]] = None
        self.extract_queue_client_ctx: Optional[AsyncContextManager[SQSClient]] = None

        # Active clients
        self.scraped_bucket_s3_client: Optional[S3Client] = None
        self.scrape_queue_client: Optional[SQSClient] = None
        self.extract_queue_client: Optional[SQSClient] = None

    async def initialize(self) -> None:
        if self._initialized:
            return

        async with self._lock:
            if self._initialized:
                return
            logger.info("Initializing core AWS clients...")
            self.session = get_session()

            await self.initialize_scraped_bucket_s3_client(self.session)
            await self.initialize_scrape_queue_client(self.session)
            await self.initialize_extract_queue_client(self.session)

            self._initialized = True
            logger.info("Shared AWS clients initialized")

    async def initialize_scraped_bucket_s3_client(self, session: AioSession) -> None:
        if self.scraped_bucket_s3_client:
            return

        AWS_REGION = os.getenv("AWS_REGION")
        AWS_SCRAPED_BUCKET_USER_ACCESS_KEY_ID = os.getenv(
            "AWS_SCRAPED_BUCKET_USER_ACCESS_KEY_ID"
        )
        AWS_SCRAPED_BUCKET_USER_SECRET_ACCESS_KEY = os.getenv(
            "AWS_SCRAPED_BUCKET_USER_SECRET_ACCESS_KEY"
        )

        if (
            not AWS_REGION
            or not AWS_SCRAPED_BUCKET_USER_ACCESS_KEY_ID
            or not AWS_SCRAPED_BUCKET_USER_SECRET_ACCESS_KEY
        ):
            raise ValueError(
                "AWS S3 scrape bucket credentials or region are not set. Please set them in your .env file."
            )

        self.scraped_bucket_s3_client_ctx = cast(
            AsyncContextManager[S3Client],
            session.create_client(
                "s3",
                region_name=AWS_REGION,
                aws_access_key_id=AWS_SCRAPED_BUCKET_USER_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SCRAPED_BUCKET_USER_SECRET_ACCESS_KEY,
            ),
        )
        self.scraped_bucket_s3_client = (
            await self.scraped_bucket_s3_client_ctx.__aenter__()
        )
        logger.info("Scraped bucket S3 client initialized successfully")

    async def initialize_scrape_queue_client(self, session: AioSession) -> None:
        if self.scrape_queue_client:
            return

        AWS_REGION = os.getenv("AWS_REGION")
        AWS_SCRAPE_QUEUE_USER_ACCESS_KEY_ID = os.getenv(
            "AWS_SCRAPE_QUEUE_USER_ACCESS_KEY_ID"
        )
        AWS_SCRAPE_QUEUE_USER_SECRET_ACCESS_KEY = os.getenv(
            "AWS_SCRAPE_QUEUE_USER_SECRET_ACCESS_KEY"
        )

        if (
            not AWS_REGION
            or not AWS_SCRAPE_QUEUE_USER_ACCESS_KEY_ID
            or not AWS_SCRAPE_QUEUE_USER_SECRET_ACCESS_KEY
        ):
            raise ValueError(
                "AWS Scrape Queue credentials or region are not set. Please set them in your .env file."
            )

        self.scrape_queue_client_ctx = cast(
            AsyncContextManager[SQSClient],
            session.create_client(
                "sqs",
                region_name=AWS_REGION,
                aws_access_key_id=AWS_SCRAPE_QUEUE_USER_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SCRAPE_QUEUE_USER_SECRET_ACCESS_KEY,
            ),
        )
        self.scrape_queue_client = await self.scrape_queue_client_ctx.__aenter__()
        logger.info("Scrape queue client initialized successfully")

    async def initialize_extract_queue_client(self, session: AioSession) -> None:
        if self.extract_queue_client:
            return

        AWS_REGION = os.getenv("AWS_REGION")
        AWS_EXTRACT_QUEUE_USER_ACCESS_KEY_ID = os.getenv(
            "AWS_EXTRACT_QUEUE_USER_ACCESS_KEY_ID"
        )
        AWS_EXTRACT_QUEUE_USER_SECRET_ACCESS_KEY = os.getenv(
            "AWS_EXTRACT_QUEUE_USER_SECRET_ACCESS_KEY"
        )

        if (
            not AWS_REGION
            or not AWS_EXTRACT_QUEUE_USER_ACCESS_KEY_ID
            or not AWS_EXTRACT_QUEUE_USER_SECRET_ACCESS_KEY
        ):
            raise ValueError(
                "AWS Extract Queue credentials or region are not set. Please set them in your .env file."
            )

        self.extract_queue_client_ctx = cast(
            AsyncContextManager[SQSClient],
            session.create_client(
                "sqs",
                region_name=AWS_REGION,
                aws_access_key_id=AWS_EXTRACT_QUEUE_USER_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_EXTRACT_QUEUE_USER_SECRET_ACCESS_KEY,
            ),
        )
        self.extract_queue_client = await self.extract_queue_client_ctx.__aenter__()
        logger.info("Extract queue client initialized successfully")

    async def cleanup(self) -> None:
        if not self._initialized:
            return
        async with self._lock:
            logger.info("Cleaning up core AWS clients...")
            if self.extract_queue_client_ctx and self.extract_queue_client:
                await self.extract_queue_client_ctx.__aexit__(None, None, None)
            if self.scrape_queue_client_ctx and self.scrape_queue_client:
                await self.scrape_queue_client_ctx.__aexit__(None, None, None)
            if self.scraped_bucket_s3_client_ctx and self.scraped_bucket_s3_client:
                await self.scraped_bucket_s3_client_ctx.__aexit__(None, None, None)
            self._initialized = False
            logger.info("Shared AWS clients cleaned up")


# Keep the instance private; expose only functions.
_aws_clients = SharedAWSClients()


async def initialize_core_aws_clients() -> None:
    await _aws_clients.initialize()


async def cleanup_core_aws_clients() -> None:
    await _aws_clients.cleanup()


def get_scraped_bucket_s3_client() -> S3Client:
    if _aws_clients.scraped_bucket_s3_client is None:
        raise RuntimeError(
            "S3 client not initialized. Call initialize_aws_clients() at app startup."
        )
    return _aws_clients.scraped_bucket_s3_client


def get_scrape_queue_client() -> SQSClient:
    if _aws_clients.scrape_queue_client is None:
        raise RuntimeError(
            "SQS scrape client not initialized. Call initialize_aws_clients() at app startup."
        )
    return _aws_clients.scrape_queue_client


def get_extract_queue_client() -> SQSClient:
    if _aws_clients.extract_queue_client is None:
        raise RuntimeError(
            "SQS extract client not initialized. Call initialize_aws_clients() at app startup."
        )
    return _aws_clients.extract_queue_client


__all__ = [
    "initialize_core_aws_clients",
    "cleanup_core_aws_clients",
    "get_scraped_bucket_s3_client",
    "get_scrape_queue_client",
    "get_extract_queue_client",
]
