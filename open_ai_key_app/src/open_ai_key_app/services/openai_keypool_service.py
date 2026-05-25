"""
KeyPoolManager is a singleton class that manages a pool of API keys.
Other apps/modules can import the singleton instance to use it if
they are one the same server as the parent wrapper redis app.
"""

import asyncio
import os
import logging
import random

from open_ai_key_app.models.gpt_model import MODEL_REGISTRY
from open_ai_key_app.models.keyslot import KeySlot
from open_ai_key_app.utils.redis_key_manager_util import get_all_openai_keys

logger = logging.getLogger(__name__)

LOCK_EXPIRY = os.getenv("LOCK_EXPIRY")
if not LOCK_EXPIRY:
    raise ValueError(
        "LOCK_EXPIRY environment variable is not set. Please set it in your .env file."
    )


class OpenAIKeyPool:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        # only run once
        if getattr(self, "_initialized", False):
            return
        self._initialized = True
        self.initialize_slots()

    def initialize_slots(self):
        key_map = get_all_openai_keys()
        self.slots: list[KeySlot] = [
            KeySlot(
                name=f"{key_name}:{model.model_name}",
                api_key=api_key,
                token_limit_per_min=model.token_limit_per_minute,
                model_name=model.model_name,
            )
            for key_name, api_key in key_map.items()
            for model in MODEL_REGISTRY
        ]

    def refresh(self):
        self.initialize_slots()

    def _find_slot(self, api_key: str, model_name: str) -> KeySlot:
        for slot in self.slots:
            if slot.api_key == api_key and slot.model_name == model_name:
                return slot
        raise ValueError(f"No slot found for api_key/model: {api_key}/{model_name}")

    async def borrow_key(
        self,
        tokens_needed: int,
        model_name: str,
        lock_expiry: int = int(LOCK_EXPIRY),  # when accessing using REDIS
        timeout_in_seconds: int = 0,  # when accessing using HTTP API
    ) -> tuple[str, str, str]:
        expiry = (
            asyncio.get_event_loop().time() + (timeout_in_seconds * 1000)
            if timeout_in_seconds > 0
            else None
        )
        while True:
            model_slots = [s for s in self.slots if s.model_name == model_name]
            random.shuffle(model_slots)
            for slot in model_slots:
                # for keys that are exhausted, can_accept will always return False
                if slot.can_accept(tokens_needed) and not slot.is_locked():
                    try:
                        lock_token = await slot.acquire_lock(lock_expiry)
                    except Exception as e:
                        logger.error(f"Error acquiring lock for slot {slot.name}: {e}")
                        continue
                    return slot.name, slot.api_key, lock_token
            await asyncio.sleep(0.25)
            if expiry and asyncio.get_event_loop().time() > expiry:
                raise TimeoutError("No available slot found within the timeout period.")

    def return_key(self, api_key: str, lock_token: str, model_name: str) -> None:
        slot = self._find_slot(api_key, model_name)
        try:
            slot.release_lock(lock_token)
        except Exception as e:
            raise ValueError(f"Failed to release lock for slot {slot.name}: {e}") from e

    def record_key_usage(self, api_key: str, tokens_used: int, model: str) -> None:
        """
        Records the usage of the API key by updating its token usage in REDIS.
        CAUTION: This effect will be global, affecting all users of the key pool in any app/module.
        """
        slot = self._find_slot(api_key, model)
        slot.record_usage(tokens_used)

    def set_key_cooldown(self, api_key: str, cooldown_seconds: float, model: str):
        """
        Sets a cooldown for the API key in REDIS and updates the slot in memory.
        CAUTION: This effect will be global, affecting all users of the key pool in any app/module.
        """
        slot = self._find_slot(api_key, model)
        slot.set_cooldown(cooldown_seconds)

    def mark_key_exhausted(self, api_key: str, exhausted_msg: str, model_name: str):
        """
        Marks the API key as exhausted in REDIS and removes its slot from the pool memory.
        CAUTION: This effect will be global, affecting all users of the key pool in any app/module.

        Only the (api_key, model) slot is removed — the same API key remains available
        for other models. While other apps/modules may still have the slot in their memory,
        can_accept ensures it is skipped each time during borrow_key.
        """
        slot = self._find_slot(api_key, model_name)
        slot.mark_exhausted(exhausted_msg)
        self.slots.remove(slot)

        remaining_model_slots = [s for s in self.slots if s.model_name == model_name]
        if not remaining_model_slots:
            logger.error(
                f"All API keys exhausted for model: {model_name}. Stopping the process."
            )
            raise RuntimeError(f"All API keys are exhausted for model: {model_name}.")

    def mark_key_available(self, api_key: str, model_name: str):
        """
        Marks the API key as available in REDIS and adds it back to the pool memory (local to user of this file).
        CAUTION: This effect will be global, affecting all users of the key pool in any app/module.
        """
        for slot in self.slots:
            if slot.api_key == api_key and slot.model_name == model_name:
                slot.mark_available()
                return

        # If not found, reinitialize the slots
        self.refresh()

    def __str__(self):
        return "\n".join(
            [
                f"KeySlot(id={slot.name}, api_key={slot.api_key}, model_name={slot.model_name}, token_limit={slot.token_limit}, token_usage={slot.token_usage}, is_locked={slot.is_locked()}, is_exhausted={slot.is_exhausted})"
                for slot in self.slots
            ]
        )


keypool = OpenAIKeyPool()
