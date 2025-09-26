from datetime import datetime
from pydantic import BaseModel
from typing import Optional

from shared.models.field_types import (
    S3FileVersionIDType,
)

# Note that this is full text level, so there are no chunk level stats or results involved
# only final results matter here


# union of both keyword and concept extraction stats
class GenericExtractionStats(BaseModel):
    extract_prompt_version_id: S3FileVersionIDType
    map_prompt_version_id: Optional[S3FileVersionIDType] = None


# union of both keyword and concept extraction results
class GenericExtractionResults(BaseModel):
    extracted_at: datetime
    results: list[str]
    stats: GenericExtractionStats  # chunk map
