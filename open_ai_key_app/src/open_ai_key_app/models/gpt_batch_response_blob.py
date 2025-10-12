from datetime import datetime
from pydantic import BaseModel, computed_field

"""
{
    "id": "batch_req_123", 
    "custom_id": "request-2", 
    "response": {
        "status_code": 200, 
        "request_id": "req_123", 
        "body": {
            "id": "chatcmpl-123", 
            "object": "chat.completion", 
            "created": 1711652795, 
            "model": "gpt-3.5-turbo-0125", 
            "choices": [
                {
                    "index": 0, 
                    "message": {
                        "role": "assistant", 
                        "content": "Hello."
                    }, 
                    "logprobs": null, 
                    "finish_reason": "stop"
                }
            ], 
            "usage": {
                "prompt_tokens": 22, 
                "completion_tokens": 2, 
                "total_tokens": 24
            }, 
            "system_fingerprint": "fp_123"
        }
    }, 
    "error": null
}

{
  "id": "batch_req_456",
  "custom_id": "request-1",
  "response": {
    "status_code": 200,
    "request_id": "req_789",
    "body": {
      "id": "chatcmpl-abc",
      "object": "chat.completion",
      "created": 1711652789,
      "model": "gpt-3.5-turbo-0125",
      "choices": [
        {
          "index": 0,
          "message": {
            "role": "assistant",
            "content": "Hello! How can I assist you today?"
          },
          "logprobs": null,
          "finish_reason": "stop"
        }
      ],
      "usage": {
        "prompt_tokens": 20,
        "completion_tokens": 9,
        "total_tokens": 29
      },
      "system_fingerprint": "fp_3ba"
    }
  },
  "error": null
}
"""


class GPTBatchResponseBlobChoiceMessage(BaseModel):
    role: str  # e.g. "assistant"
    content: str  # e.g. "Hello."


class GPTBatchResponseBlobChoice(BaseModel):
    index: int  # e.g. 0
    message: GPTBatchResponseBlobChoiceMessage
    logprobs: dict | None  # e.g. null
    finish_reason: str  # e.g. "stop"


class GPTBatchResponseBlobUsage(BaseModel):
    prompt_tokens: int  # e.g. 22
    completion_tokens: int  # e.g. 2
    total_tokens: int  # e.g. 24


class GPTResponseBlobBody(BaseModel):
    completion_id: str  # e.g. "chatcmpl-123"
    object: str  # e.g. "chat.completion"
    created: datetime  # e.g. 1711652795 (epoch time) converted to datetime
    model: str  # e.g. "gpt-3.5-turbo-0125"
    choices: list[GPTBatchResponseBlobChoice]
    usage: GPTBatchResponseBlobUsage
    system_fingerprint: str  # e.g. "fp_123"


class GPTBatchResponseBody(BaseModel):
    status_code: int  # e.g. 200
    gpt_internal_request_id: str  # e.g. "req_123"
    body: GPTResponseBlobBody

    @computed_field
    @property
    def result(self) -> str:
        return self.body.choices[0].message.content


class GPTBatchResponseBlob(BaseModel):
    batch_id: str  # e.g. "batch_req_123"
    request_custom_id: str  # e.g. "request-2"
    response: GPTBatchResponseBody
    error: dict | None  # e.g. null
