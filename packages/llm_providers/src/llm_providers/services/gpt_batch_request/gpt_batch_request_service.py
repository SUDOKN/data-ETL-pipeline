import logging
import uuid
from datetime import datetime

from llm_providers.db_models.gpt_batch_request import (
    GPTBatchRequest,
)
from llm_providers.models.open_ai.gpt_batch_response_blob import (
    ChatCompletionChoice,
    ChatCompletionChoiceMessage,
    ChatCompletionResponse,
    ChatCompletionUsage,
    GPTBatchResponse,
)

from llm_providers.models.llm_model import LLM_Model
from llm_providers.models.llm_model import NO_MODEL
from llm_providers.models.open_ai.gpt_model_params import (
    GPTModelParams,
)
from llm_providers.utils.open_ai.ask_gpt_util import (
    fetch_gpt_batch_response,
)
from llm_providers.utils.open_ai.batch_gpt_util import (
    get_gpt_request_blob,
)

logger = logging.getLogger(__name__)


def create_base_gpt_batch_request(
    deferred_at: datetime,
    subject_unique_id: str,
    custom_id: str,
    context: str,
    prompt_text: str,
    gpt_model: LLM_Model,
    batch_id: str | None,
    model_params: GPTModelParams,
) -> GPTBatchRequest:
    nonce = uuid.uuid4().hex

    request_blob = get_gpt_request_blob(
        custom_id=custom_id,
        prompt_text=f"{prompt_text}",  # system role, helps to keep static for kv cache
        context=f"{nonce}\n\n{context}",  # user role
        gpt_model=gpt_model,
        model_params=model_params,
    )

    gpt_batch_request = GPTBatchRequest(
        created_at=deferred_at,
        updated_at=deferred_at,
        subject_unique_id=subject_unique_id,
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
            model=NO_MODEL.name,
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
) -> GPTBatchResponse:
    """Send a request eagerly, from the body that the batch path would have sent.

    The params come off ``request.body`` and NOT from an argument. They used to
    come from an argument, and on 2026-08-18 that cost a whole subject: nodes set
    their catalog-generated strict ``response_format`` on the body via
    ``with_response_format`` while every caller passed its own
    ``metadata.<stage>.model_params``, whose ``response_format`` was the
    pipeline-wide ``{"type": "json_object"}``. The strict schema was written to
    Mongo and never sent, so a freehand-grounding response answered a `condition`
    rule in a `quality` rule's vocabulary — a value strict decoding cannot emit —
    and failed parse after the tokens were spent.

    The body is the serialized request: it is what the JSONL batch path uploads,
    so sourcing from it is also what makes an eager run and a batch run send the
    same bytes. ``gpt_model`` stays an argument because the LLM_Model object
    carries the context window that the name alone does not; it is checked
    against the body rather than trusted.
    """
    if (
        gpt_batch_request.is_batch_request_pending()
        and gpt_batch_request.batch_id != "Eager"
    ):
        raise ValueError(
            f"Cannot dispatch a pending GPT batch request with id {gpt_batch_request.request.custom_id} if batch_id is not 'Eager'."
        )

    body = gpt_batch_request.request.body
    if body.model != gpt_model.name:
        raise ValueError(
            f"Cannot dispatch GPT batch request {gpt_batch_request.request.custom_id}: "
            f"it was built for model {body.model!r} but dispatch was handed "
            f"{gpt_model.name!r}. The stored body is what gets sent, so the two "
            f"must name the same model."
        )

    # This is a placeholder for any additional logic needed to dispatch the request,
    # such as adding it to a queue or marking it as dispatched in the database.
    logger.info(
        f"Dispatching GPT batch request with id {gpt_batch_request.request.custom_id}"
    )
    prompt, context = (
        body.system_message(),
        body.user_message(),
    )
    gpt_response_blob = await fetch_gpt_batch_response(
        context=context,
        prompt=prompt,
        custom_id=gpt_batch_request.request.custom_id,
        batch_id=gpt_batch_request.batch_id,
        gpt_model=gpt_model,
        # GPTRequestBody IS a GPTModelParams (plus `model` and `messages`), so the
        # stored body serves directly as the params of the call it describes.
        model_params=body,
    )
    logger.info(
        f"Received GPT batch response for request id {gpt_batch_request.request.custom_id}"
    )
    return gpt_response_blob
