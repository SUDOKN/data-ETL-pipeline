from beanie import Document
from datetime import datetime
from pydantic import Field

from packages.core.src.core.models.field_types import MfgETLDType
from packages.core.src.core.utils.time_util import get_current_time


class ExtractionError(Document):
    created_at: datetime = Field(default_factory=lambda: get_current_time())
    error: str
    field: str
    mfg_etld1: MfgETLDType  # Manufacturer ETLD for which the error occurred

    class Settings:
        name = "extraction_errors"
