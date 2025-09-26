from pydantic import ConfigDict

from shared.models.queue_item import QueueItem


class ToExtractItem(QueueItem):
    model_config = ConfigDict(frozen=True, extra="forbid")
    mfg_etld1: str
