from __future__ import (
    annotations,
)  # This allows you to write self-referential types without quotes, because type annotations are no longer evaluated at function/class definition time

import os
import logging
import asyncio
from typing import Optional
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

logger = logging.getLogger(__name__)

SENDGRID_API_KEY = os.environ.get("SENDGRID_API_KEY")
if not SENDGRID_API_KEY:
    raise ValueError("SENDGRID_API_KEY is not set. Please check your .env file.")

SENDGRID_FROM_EMAIL = os.environ.get("SENDGRID_FROM_EMAIL")
if not SENDGRID_FROM_EMAIL:
    raise ValueError("SENDGRID_FROM_EMAIL is not set. Please check your .env file.")


class SendGridEmailer:
    _instance: Optional[SendGridEmailer] = None

    def __init__(self):
        # This will only be called once due to __new__
        self._client: SendGridAPIClient = SendGridAPIClient(SENDGRID_API_KEY)

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    async def send_email(self, to_emails: list[str], subject: str, html_content: str):
        try:
            message = Mail(
                from_email=SENDGRID_FROM_EMAIL,
                to_emails=to_emails,
                subject=subject,
                html_content=html_content,
            )
            # Run the synchronous send operation in a thread pool
            logger.info(f"Sending email to {to_emails} with subject '{subject}'")
            response = await asyncio.get_event_loop().run_in_executor(
                None, self._client.send, message
            )
            logger.info(f"Email sent with status code {response.status_code}")
            logger.info(f"Email response body: {response.body}")
            logger.info(f"Email response headers: {response.headers}")
            logger.info(f"Email response code: {response.status_code}")
            return response
        except Exception as e:
            logger.error(f"Error sending email: {str(e)}")
            return None


# Usage:
emailer = SendGridEmailer()
