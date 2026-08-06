from beanie import Document
from datetime import datetime
from pydantic import Field

from pure_utils.time_util import get_current_time


class ExtractionError(Document):
    created_at: datetime = Field(default_factory=lambda: get_current_time())
    error: str
    field: str
    subject_unique_id: str

    class Settings:
        name = "extraction_errors"
