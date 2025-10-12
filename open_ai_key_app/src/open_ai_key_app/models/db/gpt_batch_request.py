from beanie import Document
from datetime import datetime

from open_ai_key_app.models.gpt_batch_request_blob import GPTBatchRequestBlob
from open_ai_key_app.models.gpt_batch_response_blob import GPTBatchResponseBlob


class GPTBatchRequest(Document):
    request: GPTBatchRequestBlob
    batch_id: str | None  # known after batch is uploaded
    request_sent_at: datetime | None = None  # known after batch is created on gpt
    response_blob: GPTBatchResponseBlob | None = (
        None  # known after batch response is received
    )
    response_received_at: datetime | None = (
        None  # known after batch response is received
    )

    class Settings:
        name = "gpt_batch_requests"
