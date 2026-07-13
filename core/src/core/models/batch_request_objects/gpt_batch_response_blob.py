from datetime import datetime
from pydantic import BaseModel

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


class ChatCompletionChoiceMessage(BaseModel):
    role: str  # e.g. "assistant"
    content: str | None  # e.g. "Hello."


class ChatCompletionChoice(BaseModel):
    index: int  # e.g. 0
    message: ChatCompletionChoiceMessage
    # logprobs: dict | None  # e.g. null
    # finish_reason: str  # e.g. "stop"


class ChatCompletionUsage(BaseModel):
    prompt_tokens: int  # e.g. 22
    completion_tokens: int  # e.g. 2
    total_tokens: int  # e.g. 24


class ChatCompletionResponse(BaseModel):
    id: str  # e.g. "chatcmpl-123"
    created: datetime  # e.g. 1711652795 (epoch time) converted to datetime
    model: str  # OpenAI silently upgrades model aliases (e.g. gpt-4o might point to a newer snapshot over time), so the response model field tells you the exact version that actually ran (e.g. gpt-4o-2024-08-06). Useful for debugging output quality regressions.
    choices: list[ChatCompletionChoice]
    usage: ChatCompletionUsage
    system_fingerprint: str | None  # e.g. "fp_123"


class GPTBatchResponse(BaseModel):
    # batch_id: str  # e.g. "batch_req_123" skipped because it's redundant with the parent GPTBatchRequest's batch_id
    request_custom_id: str  # e.g. "request-2" expected to match the GPTBatchRequest's request.custom_id
    chat_completion_result: ChatCompletionResponse
    error: dict | None = None

    @property
    def result(self) -> str | None:
        return self.chat_completion_result.choices[0].message.content
