from beanie import Document
from datetime import datetime
from pydantic import Field

from core.models.field_types import MfgURLType
from core.models.db.manufacturer import Batch
from core.utils.time_util import get_current_time


class ScrapingError(Document):
    created_at: datetime = Field(default_factory=lambda: get_current_time())
    error: str
    url: MfgURLType  # Manufacturer URL for which the error occurred
    batch: Batch

    class Settings:
        name = "scraping_errors"
