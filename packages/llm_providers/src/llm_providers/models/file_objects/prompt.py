from typing import Optional

from pydantic import BaseModel


class Prompt(BaseModel):
    s3_version_id: str
    name: str
    text: str
    num_tokens: int
    # The rule catalog these bytes were rendered from, read off the S3 object's
    # provenance stamp rather than off a local file, so it describes the prompt
    # actually in hand. None for prompts with no catalog (search, relationship,
    # single-stage).
    catalog_version: Optional[str] = None
