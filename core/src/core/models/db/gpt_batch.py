from datetime import datetime
from beanie import Document
from enum import Enum
from pydantic import BaseModel


class GPTBatchStatus(
    str, Enum
):  # may not be exhaustive, see: https://platform.openai.com/docs/guides/batch#batch-expiration
    VALIDATING = (
        "validating"  # the input file is being validated before the batch can begin
    )
    FAILED = "failed"  # the input file has failed the validation process
    IN_PROGRESS = "in_progress"  # the input file was successfully validated and the batch is currently being run
    FINALIZING = (
        "finalizing"  # the batch has completed and the results are being prepared
    )
    COMPLETED = "completed"  # the batch has been completed and the results are ready
    EXPIRED = "expired"  # the batch was not able to be completed within the 24-hour time window
    CANCELLING = (
        "cancelling"  # the batch is being cancelled (may take up to 10 minutes)
    )
    CANCELLED = "cancelled"  # the batch was cancelled


"""
Further Notes:
Batches that do not complete in time eventually move to an expired state; 
unfinished requests within that batch are cancelled, 
and any responses to completed requests are made available via the batch's output file. 
You will be charged for tokens consumed from any completed requests.

Expired requests will be written to your error file with the message as shown below. 
You can use the custom_id to retrieve the request data for expired requests.
"""


class GPTBatchMetadata(BaseModel):
    original_filename: str
    num_manufacturers: int
    total_requests: int
    total_tokens: int
    api_key_label: str


class GPTBatch(Document):
    # Read/Sync only
    external_batch_id: str  # e.g. "batch_abc123"
    endpoint: str  # e.g. "/v1/chat/completions"
    input_file_id: str  # e.g. "file-abc123"
    completion_window: str  # e.g. "24h"
    status: str  # e.g. "validating", "in_progress", "processing", "completed", "failed", "expired"
    output_file_id: str | None = None  # e.g. "file-xyz789"
    error_file_id: str | None = None  # e.g. "file-error456"
    created_at: datetime
    in_progress_at: datetime | None = None
    expires_at: datetime
    completed_at: datetime | None = None
    failed_at: datetime | None = None
    expired_at: datetime | None = None
    request_counts: dict  # e.g. {"total": 100, "completed": 80, "failed": 20}
    metadata: GPTBatchMetadata
    api_key_label: str  # e.g. "sudokn.tool", "sudokn.tool+1"

    # Updatables
    processing_completed_at: (
        datetime | None
    )  # batch status was recognized and batch was processed

    def is_processed_by_openai(self) -> bool:
        return self.status in [
            GPTBatchStatus.COMPLETED,
            GPTBatchStatus.FAILED,
            GPTBatchStatus.EXPIRED,
        ]

    def is_our_processing_complete(self) -> bool:
        return self.processing_completed_at != None

    async def mark_our_processing_complete(self, processing_completed_at: datetime):
        self.processing_completed_at = processing_completed_at
        await self.save()

    def __str__(self) -> str:
        return (
            f"GPTBatch(\n"
            f"  external_batch_id={self.external_batch_id}\n"
            f"  status={self.status}\n"
            f"  endpoint={self.endpoint}\n"
            f"  input_file_id={self.input_file_id}\n"
            f"  output_file_id={self.output_file_id}\n"
            f"  error_file_id={self.error_file_id}\n"
            f"  completion_window={self.completion_window}\n"
            f"  created_at={self.created_at}\n"
            f"  in_progress_at={self.in_progress_at}\n"
            f"  expires_at={self.expires_at}\n"
            f"  completed_at={self.completed_at}\n"
            f"  failed_at={self.failed_at}\n"
            f"  expired_at={self.expired_at}\n"
            f"  processing_completed_at={self.processing_completed_at}\n"
            f"  request_counts={self.request_counts}\n"
            f"  api_key_label={self.api_key_label}\n"
            f"  metadata={self.metadata}\n"
            f")"
        )

    class Settings:
        name = "gpt_batches"

    class Config:
        json_schema_extra = {
            "example": {
                "batch_id": "batch_abc123",
                "endpoint": "/v1/chat/completions",
                "input_file_id": "file-abc123",
                "completion_window": "24h",
                "status": "validating",
                "output_file_id": None,
                "error_file_id": None,
                "created_at": datetime.utcnow(),
                "in_progress_at": None,
                "expires_at": datetime.utcnow(),
                "completed_at": None,
                "failed_at": None,
                "expired_at": None,
                "request_counts": {"total": 0, "completed": 0, "failed": 0},
                "metadata": None,
                "api_key_label": "sudokn.tool",
            }
        }


"""
Indices for GPTBatchRequest

db.gpt_batch_requests.createIndex(
  {
    external_batch_id: 1,
  },
  {
    name: "gpt_batch_external_batch_id_idx",
    unique: true
  }
);
"""
