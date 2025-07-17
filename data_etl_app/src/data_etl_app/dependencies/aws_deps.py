from aiobotocore.session import get_session
from typing import Any
import logging

from shared.utils.aws.queue.sqs_scraper_client_util import make_sqs_scraper_client
from shared.utils.aws.s3.s3_client_util import make_s3_client

logger = logging.getLogger(__name__)


class AWSClients:
    """Manages AWS clients at the application level."""

    def __init__(self):
        self.session = None
        self.sqs_scraper_client_ctx = None
        self.s3_client_ctx = None
        self.sqs_scraper_client = None
        self.s3_client = None

    async def initialize(self):
        """Initialize AWS clients at app startup."""
        logger.info("Initializing AWS clients...")
        self.session = get_session()

        # Create context managers
        self.sqs_scraper_client_ctx = make_sqs_scraper_client(self.session)
        self.s3_client_ctx = make_s3_client(self.session)

        # Enter the context managers to get actual clients
        self.sqs_scraper_client = await self.sqs_scraper_client_ctx.__aenter__()
        self.s3_client = await self.s3_client_ctx.__aenter__()

        logger.info("AWS clients initialized successfully")

    async def cleanup(self):
        """Cleanup clients at app shutdown."""
        logger.info("Cleaning up AWS clients...")

        if self.sqs_scraper_client_ctx and self.sqs_scraper_client:
            await self.sqs_scraper_client_ctx.__aexit__(None, None, None)

        if self.s3_client_ctx and self.s3_client:
            await self.s3_client_ctx.__aexit__(None, None, None)

        logger.info("AWS clients cleaned up successfully")


# Global instance
aws_clients = AWSClients()


def get_sqs_scraper_client() -> Any:
    """Dependency to get the SQS scraper client."""
    if aws_clients.sqs_scraper_client is None:
        raise RuntimeError(
            "SQS scraper client not initialized. Make sure the app lifespan is properly configured."
        )
    return aws_clients.sqs_scraper_client


def get_s3_client() -> Any:
    """Dependency to get the S3 client."""
    if aws_clients.s3_client is None:
        raise RuntimeError(
            "S3 client not initialized. Make sure the app lifespan is properly configured."
        )
    return aws_clients.s3_client
