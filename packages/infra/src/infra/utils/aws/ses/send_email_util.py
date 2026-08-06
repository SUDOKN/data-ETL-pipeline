from __future__ import (
    annotations,
)  # This allows you to write self-referential types without quotes, because type annotations are no longer evaluated at function/class definition time

import logging
import asyncio
from typing import Optional

import aiobotocore.session

from pure_utils.env_util import optional_env, require_env

logger = logging.getLogger(__name__)


def _ses_from_email() -> str:
    return require_env("SES_FROM_EMAIL")


def _ses_region() -> str:
    return optional_env("SES_REGION", "us-east-1")


class SESEmailer:
    _instance: Optional[SESEmailer] = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._client = None
            cls._instance._client_ctx = None
            cls._instance._client_lock = asyncio.Lock()
        return cls._instance

    async def _get_client(self):
        async with self._client_lock:
            if self._client is None:
                session = aiobotocore.session.get_session()
                self._client_ctx = session.create_client(
                    "ses", region_name=_ses_region()
                )
                self._client = await self._client_ctx.__aenter__()
        return self._client

    async def close(self):
        if self._client_ctx is not None:
            await self._client_ctx.__aexit__(None, None, None)
            self._client = None
            self._client_ctx = None

    async def send_email(self, to_emails: list[str], subject: str, html_content: str):
        try:
            client = await self._get_client()
            logger.info(f"Sending email to {to_emails} with subject '{subject}'")
            response = await client.send_email(
                Source=_ses_from_email(),
                Destination={"ToAddresses": to_emails},
                Message={
                    "Subject": {"Data": subject, "Charset": "UTF-8"},
                    "Body": {"Html": {"Data": html_content, "Charset": "UTF-8"}},
                },
            )
            logger.info(f"Email sent. MessageId: {response['MessageId']}")
            logger.info(f"Email response metadata: {response['ResponseMetadata']}")
            return response
        except Exception as e:
            logger.error(f"Error sending email: {str(e)}")
            return None


# Usage:
emailer = SESEmailer()
