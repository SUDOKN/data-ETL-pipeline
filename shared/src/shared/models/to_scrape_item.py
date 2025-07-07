from dataclasses import dataclass
from shared.models.db.manufacturer import Batch
from shared.utils.url_util import canonical_host


@dataclass
class ToScrapeItem:
    manufacturer_url: str
    batch: Batch

    def __post_init__(self):
        if not isinstance(self.manufacturer_url, str) or not self.manufacturer_url:
            raise ValueError("manufacturer_url must be a non-empty string")
        if not isinstance(self.batch, Batch):
            raise ValueError("batch must be a Batch instance")

        canonical = canonical_host(self.manufacturer_url)
        if not canonical:
            raise ValueError(
                f"Invalid URL: '{self.manufacturer_url}' has no valid hostname."
            )
        self.manufacturer_url = canonical

    @classmethod
    def from_dict(cls, d: dict):
        return cls(manufacturer_url=d["manufacturer_url"], batch=Batch(**d["batch"]))

    def to_dict(self):
        return {
            "manufacturer_url": self.manufacturer_url,
            "batch": self.batch.model_dump_json(),
        }
