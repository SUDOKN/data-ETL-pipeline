from pydantic import BaseModel


class Prompt(BaseModel):
    s3_version_id: str
    name: str
    text: str
    num_tokens: int
