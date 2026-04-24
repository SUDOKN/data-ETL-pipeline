from datetime import datetime
from beanie import Document, Indexed
from pydantic import Field

from core.utils.time_util import get_current_time


class Place(Document):
    place_id: Indexed(str, unique=True)  # type: ignore[valid-type]
    geocoded_at: datetime = Field(default_factory=lambda: get_current_time())
    geocode_query: str
    raw_result: dict

    class Settings:
        name = "places"
