import os
import logging
import time
import uuid
import litellm
from dataclasses import dataclass
from typing import Optional
from openai import AsyncOpenAI
from openai.types.chat import ChatCompletion

from llm_providers.models.llm_model import LLM_Model
from llm_providers.models.llm_model_params import (
    LLMModelParams,
    LLMSamplingParams,
)

logger = logging.getLogger(__name__)

# Module-level singleton AsyncOpenAI client pointed at the LiteLLM proxy.
# Initialised lazily on first call to ask_llm_async() so that
# LITELLM_PROXY_URL / LITELLM_VIRTUAL_KEY are already in the environment
# (loaded by load_litellm_env() at bot startup).
_client: Optional[AsyncOpenAI] = None


def _get_client() -> AsyncOpenAI:
    global _client
    if _client is None:
        _client = AsyncOpenAI(
            base_url=os.environ["LITELLM_PROXY_URL"],
            api_key=os.environ["LITELLM_VIRTUAL_KEY"],
        )
    return _client


@dataclass
class LLMCallTiming:
    """What a synchronous call can measure that a batch never can.

    ``client_latency_ms`` is our wall clock around the API call (network
    included). ``openai_processing_ms`` is the provider's own server-side
    number, read off the response headers — None when the LiteLLM proxy does
    not forward it.
    """

    client_latency_ms: int
    openai_processing_ms: Optional[int]


def _processing_ms_from_headers(headers) -> Optional[int]:
    # Direct OpenAI name first; LiteLLM forwards provider headers under an
    # llm_provider- prefix (version/config dependent, hence both spellings).
    for name in ("openai-processing-ms", "llm_provider-openai-processing-ms"):
        value = headers.get(name)
        if value is not None:
            try:
                return int(float(value))
            except (TypeError, ValueError):
                return None
    return None


async def ask_gpt(
    context: str,
    prompt: str,
    gpt_model: LLM_Model,
    model_params: LLMModelParams,
) -> Optional[str]:
    response, _timing = await fetch_llm_chat_completion_result(
        context=context,
        prompt=prompt,
        gpt_model=gpt_model,
        model_params=model_params,
    )
    return response.choices[0].message.content


async def fetch_llm_chat_completion_result(
    context: str,
    prompt: str,
    gpt_model: LLM_Model,
    model_params: LLMModelParams,
) -> tuple[ChatCompletion, LLMCallTiming]:
    """
    Drop-in replacement for open_ai_key_app.utils.ask_gpt_util.ask_gpt_async.

    Sends a chat-completion request through the LiteLLM proxy.  No keypool
    borrow/return cycle — rate-limit management is handled by the proxy.

    Returns the assistant message content string, or None if the model returns
    an empty response.
    """
    request_id = str(uuid.uuid4())[:8]
    start_time = time.time()

    logger.info(f"[Request {request_id}] Starting ask_llm_async request")

    # Pre-call context-window guard (same logic as ask_gpt_async).
    # litellm.token_counter is provider-aware: uses tiktoken for OpenAI models,
    # the Anthropic/Google tokeniser for other providers.
    tokens_prompt = litellm.token_counter(model=gpt_model.name, text=prompt)
    tokens_context = litellm.token_counter(model=gpt_model.name, text=context)
    tokens_needed = tokens_prompt + tokens_context + model_params.max_completion_tokens

    if tokens_needed > gpt_model.max_context_tokens:
        elapsed = time.time() - start_time
        logger.error(
            f"[Request {request_id}] Failed after {elapsed:.2f}s: "
            f"Total tokens needed:{tokens_needed} exceed max context tokens:{gpt_model.max_context_tokens}."
        )
        raise ValueError(
            f"Total tokens needed:{tokens_needed} exceed max context tokens:{gpt_model.max_context_tokens}."
        )

    logger.debug(
        f"[Request {request_id}] context tokens: {tokens_context}, prompt tokens: {tokens_prompt}, "
        f"max response tokens: {model_params.max_completion_tokens}, total tokens needed: {tokens_needed}."
    )

    client = _get_client()
    api_call_start = time.time()

    # response_format (and any other OpenAI-only fields such as tools,
    # logprobs, etc.) only exist on GPTModelParams, not the base
    # LLMModelParams used by non-OpenAI litellm proxy callers. Forward it
    # only when present and set, so it isn't silently dropped for GPT
    # requests, without breaking callers that pass a plain LLMModelParams.
    optional_kwargs = {}
    response_format = getattr(model_params, "response_format", None)
    if response_format is not None:
        optional_kwargs["response_format"] = response_format

    # with_raw_response, not create(): the parsed object is identical, but only
    # the raw response exposes the headers carrying the provider's own
    # processing time — the one latency number a client clock cannot give.
    raw_response = await client.chat.completions.with_raw_response.create(
        model=gpt_model.name,
        messages=[
            {"role": "system", "content": prompt},
            {"role": "user", "content": context},
        ],
        max_completion_tokens=model_params.max_completion_tokens,
        temperature=model_params.temperature,
        top_p=model_params.top_p,
        presence_penalty=model_params.presence_penalty,
        frequency_penalty=model_params.frequency_penalty,
        seed=model_params.seed,
        **optional_kwargs,
    )
    response = raw_response.parse()

    api_call_duration = time.time() - api_call_start
    total_duration = time.time() - start_time
    timing = LLMCallTiming(
        client_latency_ms=int(api_call_duration * 1000),
        openai_processing_ms=_processing_ms_from_headers(raw_response.headers),
    )

    logger.info(
        f"[Request {request_id}] Success! API call took {api_call_duration:.2f}s, "
        f"total request time: {total_duration:.2f}s. "
        f"Received {len(response.choices)} choices."
    )

    return response, timing
