from pydantic import BaseModel, field_validator

from shared.utils.url_util import normalize_host


class ToExtractItem(BaseModel):
    manufacturer_url: str

    @field_validator("manufacturer_url")
    @classmethod
    def validate_and_canonicalize_url(cls, v: str) -> str:
        if not isinstance(v, str) or not v:
            raise ValueError("manufacturer_url must be a non-empty string")

        canonical = normalize_host(v)
        if not canonical:
            raise ValueError(f"Invalid URL: '{v}' has no valid hostname.")
        return canonical
