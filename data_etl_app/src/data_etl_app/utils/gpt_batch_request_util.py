import logging
from datetime import datetime
from openai.types.chat import ChatCompletion

from core.models.gpt_batch_response_blob import (
    GPTBatchResponse,
    ChatCompletionResponse,
    ChatCompletionChoice,
    ChatCompletionChoiceMessage,
    ChatCompletionUsage,
)

logger = logging.getLogger(__name__)


def parse_individual_batch_req_response_raw(
    raw_result: dict, batch_id: str
) -> GPTBatchResponse:
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
            logger.error(
                f"parse_individual_batch_req_response_raw: error in raw result\n:{raw_result.get("error")}"
            )
            raise ValueError(raw_result.get("error"))

        response_data = raw_result["response"]
        body_data = response_data["body"]

        # Parse choices
        choices = []
        for choice_data in body_data["choices"]:
            message_data = choice_data["message"]
            choice = ChatCompletionChoice(
                index=choice_data["index"],
                message=ChatCompletionChoiceMessage(
                    role=message_data["role"],
                    content=message_data["content"],
                ),
                # logprobs=choice_data.get("logprobs"),
                # finish_reason=choice_data["finish_reason"],
            )
            choices.append(choice)

        # Parse usage
        usage_data = body_data["usage"]
        usage = ChatCompletionUsage(
            prompt_tokens=usage_data["prompt_tokens"],
            completion_tokens=usage_data["completion_tokens"],
            total_tokens=usage_data["total_tokens"],
        )

        # Parse response body
        chat_completion_response = ChatCompletionResponse(
            id=body_data["id"],
            # object=body_data["object"],
            created=datetime.fromtimestamp(body_data["created"]),
            model=body_data["model"],
            choices=choices,
            usage=usage,
            system_fingerprint=body_data.get("system_fingerprint"),
        )

        # Create complete blob
        blob = GPTBatchResponse(
            request_custom_id=raw_result["custom_id"],
            chat_completion_result=chat_completion_response,
            error=raw_result.get("error"),
        )

        return blob

    except Exception as e:
        logger.error(
            f"Failed to parse response for custom_id {raw_result.get('custom_id')}: {e}"
        )
        raise


def build_response_from_chat_completion(
    chat_completion_result: ChatCompletion,
    custom_id: str,
    batch_id: str,
) -> GPTBatchResponse:
    logger.info(f"received response for custom_id={custom_id}, batch_id={batch_id}")
    logger.info(f"response: {chat_completion_result}")
    return GPTBatchResponse(
        request_custom_id=custom_id,
        chat_completion_result=ChatCompletionResponse(
            **chat_completion_result.model_dump()
        ),
        error=None,
    )
