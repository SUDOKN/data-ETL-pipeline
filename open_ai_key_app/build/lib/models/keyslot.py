"""
Internal class used by OpenAIKeyPool to manage keys using instances of KeySlot.
DO NOT USE THIS CLASS DIRECTLY.
"""

import asyncio

import time
import uuid

from open_ai_key_app.src.utils.redis_client import redis
from open_ai_key_app.src.utils.openai_key_labels import (
    get_usage_key_label,
    get_cooldown_key_label,
    get_lock_key_label,
    get_exhausted_msg_key_label,
)


class KeySlot:
    def __init__(self, name: str, api_key: str, token_limit_per_min: int = 200000):
        self.name = name
        self.api_key = api_key
        self.token_limit = token_limit_per_min
        self._set_key_labels()

    def _set_key_labels(self):
        """
        Set the key labels for this KeySlot instance.
        This is called in the constructor to initialize the keys.
        """
        self.usage_key = get_usage_key_label(self.name)
        self.cooldown_key = get_cooldown_key_label(self.name)
        self.lock_key = get_lock_key_label(self.name)
        self.exhausted_msg_key = get_exhausted_msg_key_label(self.name)

    @property
    async def token_usage(self) -> int:
        """
        Sum up all unexpired usage records—each record is its own key
        with a 60 s TTL.
        """
        pattern = f"{self.usage_key}:*"
        # SCAN is preferable in prod, but KEYS is easiest to show here:
        keys = await redis.keys(pattern)
        if not keys:
            return 0
        # Fetch all values in parallel
        vals = await asyncio.gather(*(redis.get(k) for k in keys))
        return sum(int(v) for v in vals if v is not None)

    async def record_usage(self, tokens_used: int) -> None:
        """
        Create a unique key for this usage-report, set it with a 60 s TTL,
        and let Redis auto-expire it.
        """
        # e.g. "prefix:sudokn.tool:usage:1617975307123:550e8400-e29b-41d4-a716-446655440000"
        usage_recorded_at = int(time.time() * 1_000)
        unique_id = str(uuid.uuid4())
        key = f"{self.usage_key}:{usage_recorded_at}:{unique_id}"
        # SET with an expiration of 60 seconds
        await redis.set(key, tokens_used, ex=60)

    async def acquire_lock(self, lock_expiry: float, timeout: float = 2.0) -> str:
        """
        Acquire a distributed lock for this key slot. Returns lock token if acquired, else raises TimeoutError.
        """
        lock_token = str(uuid.uuid4())
        end = time.time() + timeout
        while time.time() < end:
            # nx=True This will set the key only if it does not exist (NX) and set an expiry time (EX=...)
            # LOCK_EXPIRY_IN_FLOAT: Setting an automatic expiry would probably mean the consumer has LOCK_EXPIRY seconds to finish
            # its work before the lock is released automatically, which may not sound ideal, but this is to prevent
            # deadlocks in case the consumer crashes or fails to release the lock.
            # This is especially important in a distributed system where multiple consumers(/concurrent processes) may be trying to acquire the same lock.
            # If there is only one consumer, the lock should be acquired instantly without sleep.
            result = await redis.set(self.lock_key, lock_token, nx=True, ex=lock_expiry)
            if result:
                return lock_token
            await asyncio.sleep(0.1)

        raise TimeoutError(f"Could not acquire lock for {self.name}")

    async def is_locked(self) -> bool:
        """
        Check if the distributed lock for this key slot is currently held.
        """
        return await redis.exists(self.lock_key) > 0

    async def release_lock(self, lock_token: str) -> None:
        """
        Release the distributed lock for this key slot, only if the token matches.
        """
        # Use Lua script for atomic check-and-delete, BUT WHY?
        lua = """
        if redis.call('get', KEYS[1]) == ARGV[1] then
            return redis.call('del', KEYS[1])
        else
            return 0
        end
        """
        await redis.eval(lua, 1, self.lock_key, lock_token)

    @property
    async def cooldown_until(self) -> float:
        """
        Get the cooldown timestamp from Redis, or return 0 if not set.
        """
        cooldown_value = await redis.get(self.cooldown_key)
        return float(cooldown_value) if cooldown_value else 0.0

    async def set_cooldown(self, seconds: float):
        await redis.set(self.cooldown_key, time.time() + seconds)
        await redis.expire(self.cooldown_key, int(seconds) + 1)

    async def mark_exhausted(self, exhausted_msg: str) -> None:
        """
        Marks the key as exhausted by setting a cooldown until the current time.
        """
        # NOTE: no further cleaning is needed because other state variables have their own TTLs.
        await self.set_cooldown(0)
        await redis.set(self.exhausted_msg_key, exhausted_msg)

    async def mark_available(self) -> None:
        """
        Mark a key as replenished by removing its cooldown and exhausted message.
        """
        await redis.delete(self.cooldown_key)
        await redis.delete(self.exhausted_msg_key)

    async def can_accept(self, tokens_needed: int) -> bool:
        # for keys that are exhausted, can_accept will always return False
        now = time.time()
        if now < await self.cooldown_until:
            return False
        current_usage = await self.token_usage
        return (current_usage + tokens_needed) <= self.token_limit

    async def wait_until_available(self, tokens_needed: int) -> None:
        while not self.can_accept(tokens_needed):
            await asyncio.sleep(0.5)
