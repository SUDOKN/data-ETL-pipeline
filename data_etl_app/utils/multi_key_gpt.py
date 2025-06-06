import openai
import re
import os
import sys
import subprocess
import asyncio
import random
import time
import tiktoken
from typing import Optional


# --- GPT Model Settings ---
class GPTModel:
    def __init__(
        self,
        model_name: str,
        rate_limit_window: int,
        max_context_tokens: int,
        token_limit_per_minute: int,
        safe_completion_tokens: int,
    ):
        self.model_name = model_name
        self.rate_limit_window = rate_limit_window
        self.max_context_tokens = max_context_tokens
        self.token_limit_per_minute = token_limit_per_minute
        self.safe_completion_tokens = safe_completion_tokens


GPT_4o_mini = GPTModel(
    model_name="gpt-4o-mini",
    rate_limit_window=60,
    max_context_tokens=128000,
    token_limit_per_minute=200000,
    safe_completion_tokens=2500,
)


# --- Token Estimation ---
def num_tokens_from_string(string: str, gpt_model: GPTModel = GPT_4o_mini) -> int:
    encoding = tiktoken.encoding_for_model(gpt_model.model_name)
    return len(encoding.encode(string))


# --- KeySlot Class ---
class KeySlot:
    def __init__(self, id: str, api_key: str, token_limit_per_min: int = 200000):
        self.id = id
        self.api_key = api_key
        self.token_limit = token_limit_per_min
        self.token_usage: list[tuple[float, int]] = []
        self.lock = asyncio.Lock()
        self.cooldown_until = 0.0

    def _prune_old_tokens(self):
        now = time.time()
        self.token_usage = [(t, tok) for (t, tok) in self.token_usage if now - t < 60]

    def can_accept(self, tokens_needed: int) -> bool:
        now = time.time()
        if now < self.cooldown_until:
            return False

        self._prune_old_tokens()
        return (
            sum(tok for _, tok in self.token_usage) + tokens_needed <= self.token_limit
        )

    async def wait_until_available(self, tokens_needed: int):
        while not self.can_accept(tokens_needed):
            await asyncio.sleep(0.5)

    def record_usage(self, tokens_used: int):
        self.token_usage.append((time.time(), tokens_used))

    def set_cooldown(self, seconds: float):
        self.cooldown_until = time.time() + seconds


# --- KeyPoolManager Class ---
class KeyPoolManager:
    def __init__(self, api_keys: dict[str, str]):
        self.slots: list[KeySlot] = [
            KeySlot(id, api_key) for id, api_key in api_keys.items()
        ]

    async def acquire_slot(self, tokens_needed: int) -> tuple[KeySlot, asyncio.Lock]:
        while True:
            random.shuffle(self.slots)  # Randomize slot order each check
            for slot in self.slots:
                if slot.can_accept(tokens_needed) and not slot.lock.locked():
                    await slot.wait_until_available(tokens_needed)
                    await slot.lock.acquire()
                    return slot, slot.lock
            await asyncio.sleep(0.25)

    def remove_slot(self, api_key: str):  # to be used when quota is exceeded
        for slot in self.slots:
            if slot.api_key == api_key:
                self.slots.remove(slot)
                break

        if self.slots == []:
            print("All API keys are exhausted. Stopping the process.")
            self._stop_pm2_self()

    def _stop_pm2_self(self):
        pm2_name = os.getenv("PM2_APP_NAME")
        if not pm2_name:
            print("Not running under PM2, exiting normally.")
            sys.exit(0)

        print(f"Stopping PM2 process: {pm2_name}")
        result = subprocess.run(["pm2", "stop", pm2_name])

        if result.returncode != 0:
            print("Warning: PM2 stop failed, exiting anyway.")

        sys.exit(0)

    def __str__(self):
        return "\n".join(
            [f"KeySlot(id={slot.id}, api_key={slot.api_key})" for slot in self.slots]
        )


class ModelParameters:
    def __init__(
        self,
        temperature: float = 1,
        top_p: float = 1,
        presence_penalty: float = 0,
        frequency_penalty: float = 0,
        max_tokens: Optional[int] = None,
    ):
        self.temperature: float = temperature
        self.top_p: float = top_p
        self.presence_penalty: float = presence_penalty
        self.frequency_penalty: float = frequency_penalty
        self.max_tokens: Optional[int] = max_tokens


DefaultModelParameters = ModelParameters()


# --- ask_gpt_async Function ---
async def ask_gpt_async(
    context: str,
    prompt: str,
    pool: KeyPoolManager,
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

    slot, lock = await pool.acquire_slot(tokens_needed)
    try:
        openai.api_key = slot.api_key
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
        slot.record_usage(tokens_needed)
        return response.choices[0].message.content
    except Exception as e:
        # some errors look like
        # Error code: 429 - {'error': {'message': 'Rate limit reached for gpt-4o-mini in organization org-M5dkpWKwz4bw95SV04FgKdYV on tokens per min (TPM): Limit 200000, Used 130491, Requested 75418. Please try again in 1.772s. Visit https://platform.openai.com/account/rate-limits to learn more.', 'type': 'tokens', 'param': None, 'code': 'rate_limit_exceeded'}}
        # Error code: 429 - {'error': {'message': 'You exceeded your current quota, please check your plan and billing details. For more information on this error, read the docs: https://platform.openai.com/docs/guides/error-codes/api-errors.', 'type': 'insufficient_quota', 'param': None, 'code': 'insufficient_quota'}}
        # check if the error is due to quota exceeded
        error_msg = str(e)
        print(f"ask_gpt_async exception occurred: {error_msg}")
        if "You exceeded your current quota" in error_msg:
            print(f"Quota exceeded for API id: {slot.id}. Removing from pool.")
            pool.remove_slot(slot.api_key)
            raise ValueError(f"Quota exceeded for API key: {slot.id}.")

        # Handle rate limiting with suggested retry delay
        elif "rate limit reached" in error_msg or "Rate limit" in error_msg:
            match = re.search(r"Please try again in ([\d.]+)s", error_msg)
            if match:
                delay = float(match.group(1))
                print(
                    f"Rate limit hit for key {slot.id}. Marking as unavailable for {delay}s."
                )
                slot.set_cooldown(delay)
            else:
                print(
                    f"Rate limit hit for key {slot.id}. Marking as unavailable for 5s by default."
                )
                slot.set_cooldown(5.0)  # Fallback

            # Do not retry, just fail and allow next request to pick a different key
            raise ValueError(f"Rate limit hit for API key: {slot.id}.")
        else:
            raise e
    finally:
        lock.release()
