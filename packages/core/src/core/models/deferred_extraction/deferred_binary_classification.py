from pydantic import BaseModel

from packages.infra.src.infra.field_types import (
    S3FileVersionIDType,
)
from packages.llm_providers.src.llm_providers.field_types import BatchRequestIDType


class DeferredBinaryClassification(BaseModel):
    prompt_version_id: S3FileVersionIDType
    final_chunk_key: str
    chunk_request_id_map: dict[str, BatchRequestIDType]
