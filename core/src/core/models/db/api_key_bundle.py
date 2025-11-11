from beanie import Document
from datetime import datetime, timedelta
from pydantic import Field
import logging

from core.utils.time_util import get_current_time


logger = logging.getLogger(__name__)


MAX_COOLDOWN_MINS = 30


class APIKeyBundle(Document):
    # Constants
    created_at: datetime = Field(default_factory=lambda: get_current_time())
    label: str  # API key name
    key: str
    batch_queue_limit: int  # max number of batch requests that can be enqueued per day
    available_at: datetime

    # Updatable
    tokens_in_use: int
    updated_at: datetime

    async def add_tokens_in_use(self, tokens: int):
        self.tokens_in_use += tokens
        await self.save()

    async def remove_tokens_in_use(self, tokens: int):
        self.tokens_in_use -= tokens
        if self.tokens_in_use < 0:
            self.tokens_in_use = 0
        await self.save()

    # latest_external_batch_id: str | None

    # async def update_latest_external_batch_id(
    #     self, updated_at: datetime, external_batch_id: str
    # ):
    #     self.updated_at = updated_at
    #     self.latest_external_batch_id = external_batch_id
    #     await self.save()

    # async def mark_batch_inactive(self, updated_at: datetime):
    #     self.updated_at = updated_at
    #     self.latest_external_batch_id = None
    #     await self.save()

    # def has_active_batch(self):
    #     return self.latest_external_batch_id != None

    async def apply_cooldown(self, cooldown_for_seconds: int):
        now = get_current_time()
        logger.info(
            f"Applying cooldown: now={now}, available_at={self.available_at}, cooldown_for_seconds={cooldown_for_seconds}"
        )

        if (self.available_at - now).total_seconds() > MAX_COOLDOWN_MINS * 60:
            logger.info(
                f"Cooldown not applied: cooldown would exceed MAX_COOLDOWN_MINS ({MAX_COOLDOWN_MINS}), "
                f"remaining cooldown time is {(self.available_at - now).total_seconds()}"
            )
            return

        self.available_at = now + timedelta(seconds=cooldown_for_seconds)
        logger.info(f"Cooldown applied: new available_at={self.available_at}")
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
