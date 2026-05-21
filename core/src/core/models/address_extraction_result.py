from pydantic import BaseModel
from typing import Optional
from core.models.single_stage_extraction_results import (
    SingleStageExtractionResults,
    SingleStageStats,
)


class Address(BaseModel):
    city: str
    state: str
    country: str = "US"
    name: Optional[str] = None
    address_lines: Optional[list[str]] = None
    county: Optional[str] = None
    postal_code: Optional[str] = None

    # Geolocation fields
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    place_id: Optional[str] = None

    # Contact fields
    phone_numbers: Optional[list[str]] = None
    fax_numbers: Optional[list[str]] = None

    def base_hash(self) -> str:
        return f"{self.city}-{self.state}-{self.country}"


class AddressExtractionStats(SingleStageStats[list[Address]]):
    result: list[Address]


AddressExtractionStatsMap = dict[
    str, AddressExtractionStats
]  # "0:1000" -> {results: [{street, city, state, zip}, ...]}


class AddressExtractionResult(
    SingleStageExtractionResults[list[Address], list[Address]]
):
    result: list[Address]
    chunk_stats: AddressExtractionStatsMap
