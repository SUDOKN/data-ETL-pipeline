from beanie import Document
from datetime import datetime, timezone
from pydantic import Field


class ExtractionError(Document):
    error: str
    url: str
    name: str
    field: str
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    class Settings:
        name = "extraction_errors"
