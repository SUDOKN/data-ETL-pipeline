"""Registry of the AWS clients used across the pipeline.

Clients are keyed by credential set, mirroring the groups in `infra.required_env`,
so an entrypoint only pays for the credentials it actually declares.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, AsyncContextManager, cast

from aiobotocore.session import AioSession, get_session
from types_aiobotocore_s3.client import S3Client
from types_aiobotocore_sqs.client import SQSClient

from pure_utils.env_util import require_env

logger = logging.getLogger(__name__)


class AWSClientName(StrEnum):
    """One entry per credential set, not per consuming application."""

    PROMPT_RDF_S3 = "prompt_rdf_s3"
    SCRAPED_TEXT_S3 = "scraped_text_s3"
    SCRAPE_QUEUE = "scrape_queue"
    EXTRACT_QUEUE = "extract_queue"


@dataclass(frozen=True)
class _ClientSpec:
    service: str
    access_key_var: str
    secret_key_var: str


_SPECS: dict[AWSClientName, _ClientSpec] = {
    AWSClientName.PROMPT_RDF_S3: _ClientSpec(
        service="s3",
        access_key_var="AWS_RDF_AND_PROMPT_USER_ACCESS_KEY_ID",
        secret_key_var="AWS_RDF_AND_PROMPT_USER_SECRET_ACCESS_KEY",
    ),
    AWSClientName.SCRAPED_TEXT_S3: _ClientSpec(
        service="s3",
        access_key_var="AWS_SCRAPED_BUCKET_USER_ACCESS_KEY_ID",
        secret_key_var="AWS_SCRAPED_BUCKET_USER_SECRET_ACCESS_KEY",
    ),
    AWSClientName.SCRAPE_QUEUE: _ClientSpec(
        service="sqs",
        access_key_var="AWS_SCRAPE_QUEUE_USER_ACCESS_KEY_ID",
        secret_key_var="AWS_SCRAPE_QUEUE_USER_SECRET_ACCESS_KEY",
    ),
    AWSClientName.EXTRACT_QUEUE: _ClientSpec(
        service="sqs",
        access_key_var="AWS_EXTRACT_QUEUE_USER_ACCESS_KEY_ID",
        secret_key_var="AWS_EXTRACT_QUEUE_USER_SECRET_ACCESS_KEY",
    ),
}

_lock = asyncio.Lock()
_session: AioSession | None = None
_clients: dict[AWSClientName, Any] = {}
_client_ctxs: dict[AWSClientName, AsyncContextManager[Any]] = {}


async def initialize_aws_clients(*names: AWSClientName) -> None:
    """Create the named clients. Already-initialized names are left untouched."""
    if not names:
        raise ValueError("initialize_aws_clients requires at least one client name.")

    global _session
    async with _lock:
        for name in names:
            if name in _clients:
                continue
            spec = _SPECS[name]
            if _session is None:
                _session = get_session()
            ctx = cast(
                AsyncContextManager[Any],
                _session.create_client(
                    spec.service,
                    region_name=require_env("AWS_REGION"),
                    aws_access_key_id=require_env(spec.access_key_var),
                    aws_secret_access_key=require_env(spec.secret_key_var),
                ),
            )
            _client_ctxs[name] = ctx
            _clients[name] = await ctx.__aenter__()
            logger.info("Initialized AWS %s client '%s'", spec.service, name)


async def cleanup_aws_clients(*names: AWSClientName) -> None:
    """Close the named clients, or every initialized client when none are given."""
    async with _lock:
        targets = list(names) if names else list(_clients)
        for name in targets:
            ctx = _client_ctxs.pop(name, None)
            # Dropped before closing so a later get_*_client() raises instead of
            # handing back a closed client.
            _clients.pop(name, None)
            if ctx is None:
                continue
            await ctx.__aexit__(None, None, None)
            logger.info("Cleaned up AWS client '%s'", name)


@asynccontextmanager
async def aws_clients(*names: AWSClientName) -> AsyncIterator[None]:
    """Scope the named clients to a block, closing them on exit."""
    await initialize_aws_clients(*names)
    try:
        yield
    finally:
        await cleanup_aws_clients(*names)


def _get(name: AWSClientName) -> Any:
    client = _clients.get(name)
    if client is None:
        raise RuntimeError(
            f"AWS client '{name}' is not initialized. Call "
            f"initialize_aws_clients(AWSClientName.{name.name}) at entrypoint startup."
        )
    return client


def get_prompt_rdf_s3_client() -> S3Client:
    return cast(S3Client, _get(AWSClientName.PROMPT_RDF_S3))


def get_scraped_bucket_s3_client() -> S3Client:
    return cast(S3Client, _get(AWSClientName.SCRAPED_TEXT_S3))


def get_scrape_queue_client() -> SQSClient:
    return cast(SQSClient, _get(AWSClientName.SCRAPE_QUEUE))


def get_extract_queue_client() -> SQSClient:
    return cast(SQSClient, _get(AWSClientName.EXTRACT_QUEUE))


__all__ = [
    "AWSClientName",
    "aws_clients",
    "initialize_aws_clients",
    "cleanup_aws_clients",
    "get_prompt_rdf_s3_client",
    "get_scraped_bucket_s3_client",
    "get_scrape_queue_client",
    "get_extract_queue_client",
]
