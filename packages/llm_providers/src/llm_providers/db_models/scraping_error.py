from beanie import Document
from datetime import datetime
from pydantic import Field

from packages.pure_utils.src.pure_utils.time_util import get_current_time


class ScrapingError(Document):
    created_at: datetime = Field(default_factory=lambda: get_current_time())
    error: str
    subject_unique_id: str

    class Settings:
        name = "scraping_errors"
