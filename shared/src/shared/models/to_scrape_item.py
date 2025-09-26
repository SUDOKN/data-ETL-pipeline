from __future__ import (
    annotations,
)  # This allows you to write self-referential types without quotes, because type annotations are no longer evaluated at function/class definition time
from pydantic import ConfigDict, Field, field_validator

from shared.models.db.manufacturer import Batch
from shared.models.queue_item import QueueItem
from shared.utils.url_util import (
    get_etld1_from_host,
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


class ToScrapeItem(QueueItem):
    model_config = ConfigDict(frozen=True, extra="forbid")

    accessible_normalized_url: str
    batch: Batch
    mfg_etld1: str = Field(
        init=False
    )  # effective top-level domain plus one, e.g. "example.com"; ".com" is top level domain, "example" is the second-level domain

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.mfg_etld1 = get_etld1_from_host(self.accessible_normalized_url)

    @field_validator("accessible_normalized_url")
    @classmethod
    def validate_and_normalize_url(cls, v: str) -> str:
        if not isinstance(v, str) or not v:
            raise ValueError("accessible_normalized_url must be a non-empty string")

        _, url = get_normalized_url(get_complete_url_with_compatible_protocol(v))
        return url
