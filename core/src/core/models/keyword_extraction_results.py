from datetime import datetime
from pydantic import BaseModel

from core.models.field_types import (
    S3FileVersionIDType,
)


class KeywordExtractionChunkStats(BaseModel):
    results: set[str]


# "0:1000" -> {results: ('keyword1', 'keyword2', ...)}
KeywordSearchChunkMap = dict[str, KeywordExtractionChunkStats]


class KeywordExtractionStats(BaseModel):
    extract_prompt_version_id: S3FileVersionIDType
    chunked_stats: KeywordSearchChunkMap


class KeywordExtractionResults(BaseModel):
    extracted_at: datetime
    results: set[str]
    stats: KeywordExtractionStats  # chunk map
