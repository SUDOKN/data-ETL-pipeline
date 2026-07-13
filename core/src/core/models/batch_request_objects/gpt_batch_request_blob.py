from pydantic import BaseModel, ConfigDict

from open_ai_key_app.models.gpt_model_params import (
    GPTRequestBody,
)

# ─────────────────────────────────────────────────────────────────
# Batch JSONL envelope
# ─────────────────────────────────────────────────────────────────


class GPTBatchRequestBlob(BaseModel):
    """
    One line in the batch .jsonl file.
    body is GPTRequestBody — pure OpenAI params, no local accounting fields.
    input_tokens is a local accounting field stored in MongoDB but excluded
    from the serialized JSONL line sent to OpenAI.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    custom_id: str
    method: str = "POST"
    url: str = "/v1/chat/completions"
    body: GPTRequestBody
    input_tokens: int  # local accounting; exclude via model_dump(exclude={"input_tokens"}) when writing JSONL
