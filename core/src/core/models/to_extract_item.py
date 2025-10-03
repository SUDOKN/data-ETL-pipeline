from __future__ import annotations
from pydantic import ConfigDict

from core.models.to_scrape_item import ToScrapeItem
from core.models.queue_item import QueueItem


class ToExtractItem(QueueItem):
    model_config = ConfigDict(frozen=True, extra="forbid")
    mfg_etld1: str

    @classmethod
    def from_to_scrape_item(cls, to_scrape_item: ToScrapeItem) -> ToExtractItem:
        """
        Create a ToExtractItem from a ToScrapeItem.

        Args:
            to_scrape_item: The ToScrapeItem to convert

        Returns:
            A new ToExtractItem instance with the mfg_etld1 from the ToScrapeItem
        """
        return cls(
            mfg_etld1=to_scrape_item.mfg_etld1,
            redo_extraction=to_scrape_item.redo_extraction,
            email_errand=to_scrape_item.email_errand,
        )
