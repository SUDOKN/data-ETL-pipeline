from pydantic import BaseModel, field_validator

from shared.models.db.manufacturer import Batch
from shared.utils.url_util import (
    get_normalized_url,
    get_complete_url_with_compatible_protocol,
)


"""
Sample:
{
    "accessible_normalized_url": "3pindustries.com",
    "batch": {
        "title": "testing",
        "timestamp": "2025-07-08T03:29:34.165905+00:00"
    }
}
"""


class ToScrapeItem(BaseModel):
    accessible_normalized_url: str
    batch: Batch

    @field_validator("accessible_normalized_url")
    @classmethod
    def validate_and_normalize_url(cls, v: str) -> str:
        if not isinstance(v, str) or not v:
            raise ValueError("accessible_normalized_url must be a non-empty string")

        _, url = get_normalized_url(get_complete_url_with_compatible_protocol(v))
        return url
