from pydantic import BaseModel

from core.models.field_types import (
    S3FileVersionIDType,
)
from open_ai_key_app.models.field_types import GPTBatchRequestCustomID


class DeferredBasicExtraction(BaseModel):
    prompt_version_id: S3FileVersionIDType
    gpt_request_id: GPTBatchRequestCustomID
