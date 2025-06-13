"""
KeyPoolManager is a singleton class that manages a pool of API keys.
Other apps/modules can import the singleton instance to use it if
they are one the same server as the parent wrapper redis app.
"""

import asyncio
import os
import random
import sys
import subprocess

from open_ai_key_app.src.models.keyslot import KeySlot
from open_ai_key_app.src.utils.redis_key_manager import get_all_openai_keys

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

    async def __init__(self):
        # only run once
        if getattr(self, "_initialized", False):
            return
        self._initialized = True
        await self.initialize_slots()

    async def initialize_slots(self):
        key_map = await get_all_openai_keys()
        self.slots: list[KeySlot] = [
            KeySlot(name, api_key) for name, api_key in key_map.items()
        ]

    async def refresh(self):
        await self.initialize_slots()

    async def borrow_key(
        self,
        tokens_needed: int,
        lock_expiry: float = float(LOCK_EXPIRY),  # when accessing using REDIS
        timeout_in_seconds: int = 0,  # when accessing using HTTP API
    ) -> tuple[str, str, str]:
        expiry = (
            asyncio.get_event_loop().time() + (timeout_in_seconds * 1000)
            if timeout_in_seconds > 0
            else None
        )
        while True:
            random.shuffle(self.slots)
            for slot in self.slots:
                # for keys that are exhausted, can_accept will always return False
                if (await slot.can_accept(tokens_needed)) and not (
                    await slot.is_locked()
                ):
                    await slot.wait_until_available(tokens_needed)
                    try:
                        lock_token = await slot.acquire_lock(lock_expiry)
                    except Exception as e:
                        print(f"Error acquiring lock for slot {slot.name}: {e}")
                        continue
                    return slot.name, slot.api_key, lock_token
            await asyncio.sleep(0.25)
            if expiry and asyncio.get_event_loop().time() > expiry:
                raise TimeoutError("No available slot found within the timeout period.")

    async def return_key(self, api_key: str, lock_token: str) -> None:
        for slot in self.slots:
            if slot.api_key == api_key:
                try:
                    await slot.release_lock(lock_token)
                except Exception as e:
                    raise ValueError(
                        f"Failed to release lock for slot {slot.name}: {e}"
                    ) from e
                return

        raise ValueError(f"No slot found for API key: {api_key}")

    async def record_key_usage(self, api_key: str, tokens_used: int) -> None:
        """
        Records the usage of the API key by updating its token usage in REDIS.
        CAUTION: This effect will be global, affecting all users of the key pool in any app/module.
        """
        for slot in self.slots:
            if slot.api_key == api_key:
                await slot.record_usage(tokens_used)
                return

        raise ValueError(f"No slot found for API key: {api_key}")

    async def set_key_cooldown(self, api_key: str, cooldown_seconds: float):
        """
        Sets a cooldown for the API key in REDIS and updates the slot in memory.
        CAUTION: This effect will be global, affecting all users of the key pool in any app/module.
        """
        for slot in self.slots:
            if slot.api_key == api_key:
                await slot.set_cooldown(cooldown_seconds)
                return

        raise ValueError(f"No slot found for API key: {api_key}")

    async def mark_key_exhausted(self, api_key: str, exhausted_msg: str):
        """
        Marks the API key as exhausted in REDIS and removes it from the pool memory (local to user of this file).
        CAUTION: This effect will be global, affecting all users of the key pool in any app/module.

        While other apps/modules may still have the key in their memory, the use of can_accept ensures they
        the key is skipped each time during acquire_key_slot.
        """
        for slot in self.slots:
            if slot.api_key == api_key:
                await slot.mark_exhausted(exhausted_msg)
                self.slots.remove(slot)
                break

        if not self.slots:
            print("All API keys are exhausted. Stopping the process.")
            raise RuntimeError("All API keys are exhausted.")

    async def mark_key_available(self, api_key: str):
        """
        Marks the API key as available in REDIS and adds it back to the pool memory (local to user of this file).
        CAUTION: This effect will be global, affecting all users of the key pool in any app/module.
        """
        for slot in self.slots:
            if slot.api_key == api_key:
                await slot.mark_available()
                return

        # If not found, reinitialize the slots
        await self.refresh()

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
            [f"KeySlot(id={slot.name}, api_key={slot.api_key})" for slot in self.slots]
        )


keypool = OpenAIKeyPool()
