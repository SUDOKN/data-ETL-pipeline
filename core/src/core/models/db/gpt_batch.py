from datetime import datetime
from beanie import Document
from enum import Enum


class GPTBatchStatus(str, Enum):
    VALIDATING = "validating"
    IN_PROGRESS = "in_progress"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    EXPIRED = "expired"


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
    metadata: dict | None = None
    api_key_label: str  # e.g. "sudokn.tool", "sudokn.tool+1"

    # Updatables
    processing_completed_at: (
        datetime | None
    )  # batch status was recognized and batch was processed

    def is_processing_complete(self) -> bool:
        return self.processing_completed_at != None

    async def mark_processing_complete(self, processing_completed_at: datetime):
        self.processing_completed_at = processing_completed_at
        await self.save()

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
