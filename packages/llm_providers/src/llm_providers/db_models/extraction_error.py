from beanie import Document
from datetime import datetime
from pydantic import Field

from packages.core.src.core.field_types import SubjectUniqueIDType
from packages.pure_utils.src.pure_utils.time_util import get_current_time


class ExtractionError(Document):
    created_at: datetime = Field(default_factory=lambda: get_current_time())
    error: str
    field: str
    subject_unique_id: SubjectUniqueIDType

    class Settings:
        name = "extraction_errors"
