from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, ConfigDict

# Wire schemas for the single-stage nodes (address / business_desc / binary
# classification). These mirror the GPT response shape exactly and are distinct
# from the richer domain models (core.models.extraction_results.*) that carry
# extra derived fields (geolocation, defaults, etc.) not part of the LLM contract.


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


class BusinessDescResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: Optional[str]
    description: Optional[str]


class BinaryClassificationResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    answer: bool
    confidence: int
    reason: str
