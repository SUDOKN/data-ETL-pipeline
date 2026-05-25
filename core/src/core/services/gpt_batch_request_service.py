from datetime import datetime
import logging

from core.models.prompt import Prompt
from core.models.db.gpt_batch_request import GPTBatchRequest
from core.models.gpt_batch_response_blob import GPTBatchResponseBlob

from open_ai_key_app.models.llm_model import LLM_Model
from open_ai_key_app.models.gpt_model_params import GPTModelParams
from open_ai_key_app.utils.ask_gpt_util import fetch_gpt_batch_response
from open_ai_key_app.utils.batch_gpt_util import (
    get_gpt_request_blob,
)

logger = logging.getLogger(__name__)


def create_base_gpt_batch_request(
    deferred_at: datetime,
    custom_id: str,
    context: str,
    prompt: Prompt,
    gpt_model: LLM_Model,
    batch_id: str | None,
    model_params: GPTModelParams,
) -> GPTBatchRequest:
    request_blob = get_gpt_request_blob(
        custom_id=custom_id,
        context=context,
        prompt=prompt.text,
        gpt_model=gpt_model,
        model_params=model_params,
    )

    gpt_batch_request = GPTBatchRequest(
        created_at=deferred_at,
        updated_at=deferred_at,
        num_batches_paired_with=0,
        batch_id=batch_id,
        request=request_blob,
    )

    return gpt_batch_request


def is_batch_request_pending(
    gpt_batch_request: GPTBatchRequest,
) -> bool:
    return (
        gpt_batch_request.batch_id
        is None
        # and gpt_batch_request.response_blob is None
    )


async def dispatch_gpt_batch_request(
    gpt_batch_request: GPTBatchRequest,
    gpt_model: LLM_Model,
    model_params: GPTModelParams,
) -> GPTBatchResponseBlob:
    if is_batch_request_pending(gpt_batch_request):
        raise ValueError(
            f"Cannot dispatch GPT batch request with id {gpt_batch_request.request.custom_id} when it is pending."
        )
    elif gpt_batch_request.batch_id != "Eager":
        raise ValueError(
            f"Can only dispatch GPT batch request with id {gpt_batch_request.request.custom_id} if it has a batch_id of 'Eager'."
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
