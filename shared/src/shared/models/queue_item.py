from typing import Optional
from pydantic import BaseModel

from shared.utils.send_email_util import emailer


class EmailUserErrand(BaseModel):
    user_email: str

    async def run_errand(self, subject: str, html_content: str) -> None:
        await emailer.send_email(
            to_emails=[self.user_email], subject=subject, html_content=html_content
        )


class QueueItem(BaseModel):
    redo_extraction: bool = False
    email_errand: Optional[EmailUserErrand] = None
