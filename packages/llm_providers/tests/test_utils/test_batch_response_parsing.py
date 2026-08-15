"""Batch output rows: the completion timestamp must land as UTC.

Bare ``fromtimestamp()`` converts to the machine's LOCAL time; on any box west
of Greenwich that skewed every stored completion timestamp by hours, which
would corrupt any duration computed against the (UTC) request ``created_at`` —
exactly what the extraction dumps' turnaround and span numbers do.
"""

from llm_providers.utils.open_ai.gpt_batch_request_util import (
    parse_individual_batch_req_response_raw,
)


def test_completion_created_is_parsed_as_utc():
    raw = {
        "custom_id": "req-1",
        "response": {
            "status_code": 200,
            "request_id": "r-1",
            "body": {
                "id": "chatcmpl-1",
                "created": 1755200000,
                "model": "gpt-4o-mini-2024-07-18",
                "choices": [
                    {"index": 0, "message": {"role": "assistant", "content": "hi"}}
                ],
                "usage": {
                    "prompt_tokens": 5,
                    "completion_tokens": 2,
                    "total_tokens": 7,
                },
            },
        },
        "error": None,
    }

    blob = parse_individual_batch_req_response_raw(raw, batch_id="b-1")

    created = blob.chat_completion_result.created
    assert created.tzinfo is not None
    assert created.utcoffset().total_seconds() == 0
    # Round-trips to the exact instant OpenAI reported, regardless of the
    # machine's timezone.
    assert created.timestamp() == 1755200000
    # Latency fields exist only for eager dispatch; a parsed batch row leaves
    # them unset.
    assert blob.client_latency_ms is None
    assert blob.openai_processing_ms is None
