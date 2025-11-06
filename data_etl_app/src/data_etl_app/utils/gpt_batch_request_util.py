import logging
from datetime import datetime


from core.models.gpt_batch_response_blob import (
    GPTBatchResponseBlob,
    GPTBatchResponseBody,
    GPTResponseBlobBody,
    GPTBatchResponseBlobChoice,
    GPTBatchResponseBlobChoiceMessage,
    GPTBatchResponseBlobUsage,
)

logger = logging.getLogger(__name__)


def parse_batch_req_response(raw_result: dict, batch_id: str) -> dict:
    """
    Parse raw batch result into structured response blob.

    Args:
        raw_result: Raw result dictionary from JSONL file
        batch_id: Batch ID to associate with this result

    Returns:
        Parsed response blob dictionary
    """
    try:
        if raw_result.get("error"):
            raise ValueError(raw_result.get("error"))

        response_data = raw_result["response"]
        body_data = response_data["body"]

        # Parse choices
        choices = []
        for choice_data in body_data["choices"]:
            message_data = choice_data["message"]
            choice = GPTBatchResponseBlobChoice(
                index=choice_data["index"],
                message=GPTBatchResponseBlobChoiceMessage(
                    role=message_data["role"],
                    content=message_data["content"],
                ),
                # logprobs=choice_data.get("logprobs"),
                # finish_reason=choice_data["finish_reason"],
            )
            choices.append(choice)

        # Parse usage
        usage_data = body_data["usage"]
        usage = GPTBatchResponseBlobUsage(
            prompt_tokens=usage_data["prompt_tokens"],
            completion_tokens=usage_data["completion_tokens"],
            total_tokens=usage_data["total_tokens"],
        )

        # Parse response body
        response_body = GPTResponseBlobBody(
            # completion_id=body_data["id"],
            # object=body_data["object"],
            created=datetime.fromtimestamp(body_data["created"]),
            # model=body_data["model"],
            choices=choices,
            usage=usage,
            # system_fingerprint=body_data.get("system_fingerprint"),
        )

        # Parse full response
        response = GPTBatchResponseBody(
            status_code=response_data["status_code"],
            # gpt_internal_request_id=response_data["request_id"],
            body=response_body,
        )

        # Create complete blob
        blob = GPTBatchResponseBlob(
            batch_id=batch_id,
            request_custom_id=raw_result["custom_id"],
            response=response,
            error=raw_result.get("error"),
        )

        return blob.model_dump()

    except Exception as e:
        logger.error(
            f"Failed to parse response for custom_id {raw_result.get('custom_id')}: {e}"
        )
        raise
