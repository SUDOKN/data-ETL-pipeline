from beanie import Document
from datetime import datetime
from pydantic import Field

from core.models.gpt_batch_request_blob import GPTBatchRequestBlob
from core.models.gpt_batch_response_blob import GPTBatchResponse


class GPTBatchRequest(Document):
    created_at: datetime
    updated_at: datetime

    etld1: str  # because relying on the request.custom_id to parse this doesn't scale
    batch_id: str | None  # known after batch is uploaded
    num_batches_paired_with: int

    request: GPTBatchRequestBlob
    response: GPTBatchResponse | None = (
        None  # usually set after batch response is received,
    )
    response_parse_errors: list[dict] = Field(default_factory=list)

    def is_batch_request_pending(self) -> bool:
        return (
            self.batch_id
            is None
            # and gpt_batch_request.response is None
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
db.gpt_batch_requests.createIndex(
  { batch_id: 1 },
  { name: "gpt_batch_id_sparse_idx", sparse: true }
)
"""
