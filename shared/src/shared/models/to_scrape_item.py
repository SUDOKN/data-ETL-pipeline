from pydantic import BaseModel, field_validator

from shared.models.db.manufacturer import Batch
from shared.utils.url_util import canonical_host


"""
Sample:
{
    "manufacturer_url": "www.3pindustries.com",
    "batch": {
        "title": "testing",
        "timestamp": "2025-07-08T03:29:34.165905+00:00"
    }
}
"""


class ToScrapeItem(BaseModel):
    manufacturer_url: str
    batch: Batch

    @field_validator("manufacturer_url")
    @classmethod
    def validate_and_canonicalize_url(cls, v: str) -> str:
        if not isinstance(v, str) or not v:
            raise ValueError("manufacturer_url must be a non-empty string")

        canonical = canonical_host(v)
        if not canonical:
            raise ValueError(f"Invalid URL: '{v}' has no valid hostname.")
        return canonical
