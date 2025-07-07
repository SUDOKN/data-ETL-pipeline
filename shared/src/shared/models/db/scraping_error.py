from beanie import Document
from datetime import datetime
from pydantic import Field

from shared.models.types import MfgURLType
from shared.utils.time_util import get_current_time


class ScrapingError(Document):
    error: str
    url: MfgURLType  # Manufacturer URL for which the error occurred
    created_at: datetime = Field(default_factory=lambda: get_current_time())

    class Settings:
        name = "scraping_errors"
