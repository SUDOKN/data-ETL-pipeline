from beanie import Document
from datetime import datetime, timedelta
from pydantic import Field

from core.models.db.gpt_batch import GPTBatch
from core.utils.time_util import get_current_time

MAX_COOLDOWN_MINS = 30


class APIKeyBundle(Document):
    # Constants
    created_at: datetime = Field(default_factory=lambda: get_current_time())
    label: str  # API key name
    key: str
    batch_queue_limit: int  # max number of batch requests that can be enqueued per day
    available_at: datetime

    # Updatable
    updated_at: datetime = Field(default_factory=lambda: get_current_time())
    latest_external_batch_id: str | None

    async def update_latest_external_batch_id(
        self, updated_at: datetime, external_batch_id: str
    ):
        self.updated_at = updated_at
        self.latest_external_batch_id = external_batch_id
        await self.save()

    async def mark_batch_inactive(self, updated_at: datetime):
        self.updated_at = updated_at
        self.latest_external_batch_id = None
        await self.save()

    def has_active_batch(self):
        return self.latest_external_batch_id != None

    async def apply_cooldown(self, cooldown_for_seconds: int):
        now = get_current_time()
        if (self.available_at - now).total_seconds() > MAX_COOLDOWN_MINS * 60:
            return

        self.available_at = now + timedelta(seconds=cooldown_for_seconds)
        await self.save()

    def is_available_now(self, now: datetime):
        return now > self.available_at

    class Settings:
        name = "api_keys"


"""
Indices for APIKeys

db.api_keys.createIndex(
  {
    label: 1,
  },
  {
    name: "apikey_label_unique_idx",
    unique: true
  }
);
db.api_keys.createIndex(
  {
    key: 1,
  },
  {
    name: "apikey_key_unique_idx",
    unique: true
  }
);
"""
