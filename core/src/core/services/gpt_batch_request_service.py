import logging
import uuid
from datetime import datetime

from core.models.prompt import Prompt
from core.models.db.gpt_batch_request import GPTBatchRequest
from core.models.gpt_batch_response_blob import (
    ChatCompletionChoice,
    ChatCompletionChoiceMessage,
    ChatCompletionResponse,
    ChatCompletionUsage,
    GPTBatchResponse,
)

from litellm_proxy_app.models.llm_model import LLM_Model
from litellm_proxy_app.models.llm_model import No_model
from open_ai_key_app.models.gpt_model_params import GPTModelParams
from open_ai_key_app.utils.ask_gpt_util import fetch_gpt_batch_response
from open_ai_key_app.utils.batch_gpt_util import (
    get_gpt_request_blob,
)

logger = logging.getLogger(__name__)


def create_base_gpt_batch_request(
    deferred_at: datetime,
    etld1: str,
    custom_id: str,
    context: str,
    prompt: Prompt,
    gpt_model: LLM_Model,
    batch_id: str | None,
    model_params: GPTModelParams,
) -> GPTBatchRequest:
    nonce = uuid.uuid4().hex

    request_blob = get_gpt_request_blob(
        custom_id=custom_id,
        context=f"{nonce}\n\n{context}",
        # context=f"{context}",
        prompt=f"{nonce}\n\n{prompt.text}",
        # prompt=f"{prompt.text}",
        gpt_model=gpt_model,
        model_params=model_params,
    )

    gpt_batch_request = GPTBatchRequest(
        created_at=deferred_at,
        updated_at=deferred_at,
        etld1=etld1,
        num_batches_paired_with=0,
        batch_id=batch_id,
        request=request_blob,
    )

    return gpt_batch_request


def get_dummy_gpt_batch_response(
    deferred_at: datetime,
    request_custom_id: str,
    dummy_chat_completion_id: str,
    chat_completion_choice_message: ChatCompletionChoiceMessage,
):
    return GPTBatchResponse(
        request_custom_id=request_custom_id,
        chat_completion_result=ChatCompletionResponse(
            id=dummy_chat_completion_id,
            created=deferred_at,
            model=No_model.model_name,
            choices=[
                ChatCompletionChoice(
                    index=0,
                    message=chat_completion_choice_message,
                )
            ],
            usage=ChatCompletionUsage(
                prompt_tokens=1,
                completion_tokens=1,
                total_tokens=2,
            ),
            system_fingerprint="dummy_system_fingerprint",
        ),
    )


async def dispatch_gpt_batch_request(
    gpt_batch_request: GPTBatchRequest,
    gpt_model: LLM_Model,
    model_params: GPTModelParams,
) -> GPTBatchResponse:
    if (
        gpt_batch_request.is_batch_request_pending
        and gpt_batch_request.batch_id != "Eager"
    ):
        raise ValueError(
            f"Cannot dispatch a pending GPT batch request with id {gpt_batch_request.request.custom_id} if batch_id is not 'Eager'."
        )

    # This is a placeholder for any additional logic needed to dispatch the request,
    # such as adding it to a queue or marking it as dispatched in the database.
    logger.info(
        f"Dispatching GPT batch request with id {gpt_batch_request.request.custom_id}"
    )
    prompt, context = (
        gpt_batch_request.request.body.messages[0]["content"],
        gpt_batch_request.request.body.messages[1]["content"],
    )
    gpt_response_blob = await fetch_gpt_batch_response(
        context=context,
        prompt=prompt,
        custom_id=gpt_batch_request.request.custom_id,
        batch_id=gpt_batch_request.batch_id,
        gpt_model=gpt_model,
        model_params=model_params,
    )
    logger.info(
        f"Received GPT batch response for request id {gpt_batch_request.request.custom_id}"
    )
    return gpt_response_blob
