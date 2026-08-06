from __future__ import (
    annotations,
)  # This allows you to write self-referential types without quotes, because type annotations are no longer evaluated at function/class definition time
from datetime import datetime
from pydantic import ConfigDict, computed_field, field_validator, BaseModel


from infra.models.queue_items.queue_item import QueueItem
from pure_utils.url_util import (
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


class Batch(BaseModel):
    title: str
    timestamp: datetime


class ToScrapeItem(QueueItem):
    model_config = ConfigDict(frozen=True)

    start_url: str
    batch: Batch

    @computed_field
    @property
    def subject_unique_id(self) -> str:
        return get_etld1_from_host(self.start_url)

    @field_validator("start_url")
    @classmethod
    def validate_and_normalize_url(cls, v: str) -> str:
        if not isinstance(v, str) or not v:
            raise ValueError("start_url must be a non-empty string")

        _, url = get_normalized_url(get_complete_url_with_compatible_protocol(v))
        return url
