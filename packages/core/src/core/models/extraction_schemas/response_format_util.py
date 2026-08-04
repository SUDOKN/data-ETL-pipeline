from __future__ import annotations

from pydantic import BaseModel


def build_gpt_response_format(model: type[BaseModel], name: str) -> dict:
    """Wrap a Pydantic model's JSON schema into an OpenAI strict `json_schema`
    response_format dict. The model (and any nested models) must set
    `model_config = ConfigDict(extra="forbid")` so `additionalProperties: false`
    holds at every level, and required-but-nullable fields must be declared
    `Optional[...]` with no default so they still appear in `required`."""
    return {
        "type": "json_schema",
        "json_schema": {
            "name": name,
            "strict": True,
            "schema": model.model_json_schema(),
        },
    }
