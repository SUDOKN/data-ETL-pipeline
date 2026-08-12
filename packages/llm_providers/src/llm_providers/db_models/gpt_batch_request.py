from beanie import Document
from datetime import datetime
from pydantic import Field

from llm_providers.models.open_ai.gpt_batch_request_blob import (
    GPTBatchRequestBlob,
)
from llm_providers.models.open_ai.gpt_batch_response_blob import (
    GPTBatchResponse,
)


class GPTBatchRequest(Document):
    created_at: datetime
    updated_at: datetime

    subject_unique_id: (
        str  # because relying on the request.custom_id to parse this doesn't scale
    )
    batch_id: str | None  # known after batch is uploaded
    num_batches_paired_with: int

    request: GPTBatchRequestBlob
    response: GPTBatchResponse | None = (
        None  # usually set after batch response is received,
    )
    response_parse_errors: list[dict] = Field(default_factory=list)

    # The phrases and relationship summaries a request was built from are NOT
    # stored here. They live in `request.body`'s user message, which is where they
    # were sent from — see core.services.phrase_blocks_contract. A field here would
    # be a copy of that, free to disagree with it. Parsing read them back off the
    # user message until 2026-08-11, to check the response's quoted evidence
    # against; nothing needs them at parse time now that the field is gone.

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
