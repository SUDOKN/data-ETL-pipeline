import openai
import re
import asyncio
import tiktoken

from open_ai_key_app.src.models.gpt_model import GPTModel, GPT_4o_mini, ModelParameters
from open_ai_key_app.src.services.openai_keypool import keypool


# --- Token Estimation ---
def num_tokens_from_string(string: str, gpt_model: GPTModel = GPT_4o_mini) -> int:
    encoding = tiktoken.encoding_for_model(gpt_model.model_name)
    return len(encoding.encode(string))


# --- ask_gpt_async Function ---
# should ideally acquire lock before calling this function
async def ask_gpt_async(
    context: str,
    prompt: str,
    gpt_model: GPTModel,
    model_params: ModelParameters,
):
    tokens_prompt = num_tokens_from_string(prompt)
    tokens_context = num_tokens_from_string(context)
    max_response_tokens = (
        model_params.max_tokens
        if model_params.max_tokens
        else gpt_model.safe_completion_tokens
    )
    tokens_needed = tokens_prompt + tokens_context + max_response_tokens

    if tokens_needed > gpt_model.max_context_tokens:
        raise ValueError("Total tokens needed exceed max context tokens.")

    key_name, api_key, lock_token = await keypool.borrow_key(tokens_needed)
    try:
        openai.api_key = api_key
        response = await asyncio.to_thread(
            openai.chat.completions.create,
            model=gpt_model.model_name,
            messages=[
                {"role": "system", "content": prompt},
                {"role": "user", "content": context},
            ],
            max_completion_tokens=max_response_tokens,
            temperature=model_params.temperature,
            top_p=model_params.top_p,
            presence_penalty=model_params.presence_penalty,
            frequency_penalty=model_params.frequency_penalty,
        )
        await keypool.record_key_usage(api_key, tokens_needed)
        return response.choices[0].message.content
    except Exception as e:
        # some errors look like
        # Error code: 429 - {'error': {'message': 'Rate limit reached for gpt-4o-mini in organization org-M5dkpWKwz4bw95SV04FgKdYV on tokens per min (TPM): Limit 200000, Used 130491, Requested 75418. Please try again in 1.772s. Visit https://platform.openai.com/account/rate-limits to learn more.', 'type': 'tokens', 'param': None, 'code': 'rate_limit_exceeded'}}
        # Error code: 429 - {'error': {'message': 'You exceeded your current quota, please check your plan and billing details. For more information on this error, read the docs: https://platform.openai.com/docs/guides/error-codes/api-errors.', 'type': 'insufficient_quota', 'param': None, 'code': 'insufficient_quota'}}
        # check if the error is due to quota exceeded
        error_msg = str(e)
        print(f"ask_gpt_async exception occurred: {error_msg}")
        if "You exceeded your current quota" in error_msg:
            print(f"Quota exceeded for API key_name: {key_name}. Removing from pool.")
            await keypool.mark_key_exhausted(api_key, error_msg)
            raise ValueError(f"Quota exceeded for API key: {key_name}.")

        # Handle rate limiting with suggested retry delay
        elif "rate limit reached" in error_msg or "Rate limit" in error_msg:
            match = re.search(r"Please try again in ([\d.]+)s", error_msg)
            if match:
                delay = float(match.group(1))
                print(
                    f"Rate limit hit for key {key_name}. Marking as unavailable for {delay}s."
                )
                await keypool.set_key_cooldown(api_key, delay)
            else:
                print(
                    f"Rate limit hit for key {key_name}. Marking as unavailable for 5s by default."
                )
                await keypool.set_key_cooldown(api_key, 5.0)  # Fallback

            # Do not retry, just fail and allow next request to pick a different key
            raise ValueError(f"Rate limit hit for API key: {key_name}.")
        else:
            raise e
    finally:
        await keypool.return_key(api_key, lock_token)
