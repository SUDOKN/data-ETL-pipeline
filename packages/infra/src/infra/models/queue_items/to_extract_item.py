from __future__ import annotations
from pydantic import ConfigDict

from infra.models.queue_items.to_scrape_item import ToScrapeItem
from infra.models.queue_items.queue_item import QueueItem


class ToExtractItem(QueueItem):
    model_config = ConfigDict(frozen=True, extra="forbid")
    subject_unique_id: str

    @classmethod
    def from_to_scrape_item(cls, to_scrape_item: ToScrapeItem) -> ToExtractItem:
        """
        Create a ToExtractItem from a ToScrapeItem.

        Args:
            to_scrape_item: The ToScrapeItem to convert

        Returns:
            A new ToExtractItem instance with the subject_unique_id from the ToScrapeItem
        """
        return cls(
            subject_unique_id=to_scrape_item.subject_unique_id,
            redo_extraction=to_scrape_item.redo_extraction,
            email_errand=to_scrape_item.email_errand,
        )
