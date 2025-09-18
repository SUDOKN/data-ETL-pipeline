from datetime import datetime
from pydantic import BaseModel
from shared.models.field_types import PromptVersionIDType


class FreeRangeSearchStats(BaseModel):
    extract_prompt_version_id: PromptVersionIDType
    search: dict[str, set[str]]


class FreeRangeSearchResults(BaseModel):
    extracted_at: datetime
    results: list[str]
    stats: FreeRangeSearchStats  # chunk map
