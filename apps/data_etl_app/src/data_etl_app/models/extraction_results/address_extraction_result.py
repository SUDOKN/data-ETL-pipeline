from pydantic import BaseModel, ConfigDict
from typing import Optional

from core.models.extraction_results.single_stage_extraction_results import (
    SingleStageExtractionResults,
    SingleStageStats,
)


class AddressWire(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: Optional[str]
    address_lines: list[str]
    city: str
    state: str
    postal_code: str
    country: Optional[str]
    phone_numbers: list[str]
    fax_numbers: list[str]


class AddressExtractionResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    addresses: list[AddressWire]


class Address(AddressWire):
    country: str = "US"
    county: Optional[str] = None

    # Geolocation fields
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    place_id: Optional[str] = None

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
