from datetime import datetime
from pydantic import BaseModel


"""
{
  "custom_id": "request-1",
  "method": "POST",
  "url": "/v1/chat/completions",
  "body": {
    "model": "gpt-3.5-turbo-0125",
    "messages": [
      {
        "role": "system",
        "content": "You are a helpful assistant."
      },
      {
        "role": "user",
        "content": "Hello world!"
      }
    ],
    "max_tokens": 1000
  }
}

{
  "custom_id": "request-2",
  "method": "POST",
  "url": "/v1/chat/completions",
  "body": {
    "model": "gpt-3.5-turbo-0125",
    "messages": [
      {
        "role": "system",
        "content": "You are an unhelpful assistant."
      },
      {
        "role": "user",
        "content": "Hello world!"
      }
    ],
    "max_tokens": 1000
  }
}
"""


class GPTBatchRequestBlobBody(BaseModel):
    model: str  # e.g. "gpt-4o-mini"
    messages: list[dict]  # e.g. [{"role": "user", "content": "Hello!"}]
    input_tokens: int  # exclude in deserialization
    max_tokens: int


class GPTBatchRequestBlob(BaseModel):
    created_at: datetime
    custom_id: str  # we provide this id
    method: str = "POST"
    url: str = "/v1/chat/completions"
    body: GPTBatchRequestBlobBody
