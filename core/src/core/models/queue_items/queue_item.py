import logging
from typing import Optional
from pydantic import BaseModel

logger = logging.getLogger(__name__)

from core.utils.send_email_util import emailer


class EmailUserErrand(BaseModel):
    user_email: str

    async def run_errand(self, subject: str, html_content: str) -> None:
        logger.info(f"Running email errand for {self.user_email}")
        await emailer.send_email(
            to_emails=[self.user_email], subject=subject, html_content=html_content
        )
        logger.info(f"Email errand completed for {self.user_email}")


class QueueItem(BaseModel):
    redo_extraction: bool = False
    email_errand: Optional[EmailUserErrand] = None
