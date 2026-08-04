from pydantic import BaseModel

from packages.core.src.core.models.field_types import (
    S3FileVersionIDType,
)
from packages.core.src.core.models.field_types import BatchRequestIDType


class DeferredBinaryClassification(BaseModel):
    prompt_version_id: S3FileVersionIDType
    final_chunk_key: str
    chunk_request_id_map: dict[str, BatchRequestIDType]
