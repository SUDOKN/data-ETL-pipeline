from shared.models.db.manufacturer import Batch


class ToExtractItem:
    def __init__(self, manufacturer_url: str):

        if not isinstance(manufacturer_url, str) or not manufacturer_url:
            raise ValueError("manufacturer_url must be a non-empty string")

        self.manufacturer_url = manufacturer_url

    @classmethod
    def from_dict(cls, d: dict):
        return cls(
            manufacturer_url=d["manufacturer_url"],
        )

    def to_dict(self):
        return {
            "manufacturer_url": self.manufacturer_url,
        }
