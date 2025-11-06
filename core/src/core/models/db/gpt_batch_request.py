from beanie import Document
from datetime import datetime

from core.models.gpt_batch_request_blob import GPTBatchRequestBlob
from core.models.gpt_batch_response_blob import GPTBatchResponseBlob


class GPTBatchRequest(Document):
    created_at: datetime
    request: GPTBatchRequestBlob
    batch_id: str | None  # known after batch is uploaded
    response_blob: GPTBatchResponseBlob | None = (
        None  # known after batch response is received,
    )

    class Settings:
        name = "gpt_batch_requests"


"""
Indices for GPTBatchRequest

db.gpt_batch_requests.createIndex(
  {
    request.custom_id: 1,
  },
  {
    name: "gpt_batch_requests_custom_id_idx",
    unique: true
  }
);
"""
