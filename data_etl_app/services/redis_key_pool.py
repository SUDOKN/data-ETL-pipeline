import asyncio
import os
import random
import time
import uuid

from redis_service.redis_client import redis

KEYPOOL_PREFIX = os.getenv("KEYPOOL_PREFIX")
LOCK_EXPIRY = os.getenv("LOCK_EXPIRY")

if not KEYPOOL_PREFIX:
    raise ValueError(
        "KEYPOOL_PREFIX environment variable is not set. Please set it in your .env file."
    )


if not LOCK_EXPIRY:
    raise ValueError(
        "LOCK_EXPIRY environment variable is not set. Please set it in your .env file."
    )

LOCK_EXPIRY_IN_FLOAT = float(LOCK_EXPIRY)


async def acquire_lock(key_id: str, timeout: float = 10.0) -> str:
    """
    Acquire a distributed lock for a key. Returns lock token if acquired, else raises TimeoutError.
    """
    lock_key = f"{KEYPOOL_PREFIX}:{key_id}:lock"
    lock_token = str(uuid.uuid4())
    end = time.time() + timeout
    while time.time() < end:
        # This will set the key only if it does not exist (NX) and set an expiry time (EX=...)
        # Setting an automatic expiry would probably mean the consumer has LOCK_EXPIRY seconds to finish
        # its work before the lock is released automatically, which may not sound ideal, but this is to prevent
        # deadlocks in case the consumer crashes or fails to release the lock.
        # This is especially important in a distributed system where multiple consumers(/concurrent processes) may be trying to acquire the same lock.
        # If there is only one consumer, the lock should be acquired instantly without sleep.
        result = await redis.set(lock_key, lock_token, nx=True, ex=LOCK_EXPIRY_IN_FLOAT)
        if result:
            return lock_token
        await asyncio.sleep(0.1)
    raise TimeoutError(f"Could not acquire lock for {key_id}")


async def release_lock(key_id: str, lock_token: str):
    """
    Release the distributed lock for a key, only if the token matches.
    """
    lock_key = f"{KEYPOOL_PREFIX}:{key_id}:lock"
    # Use Lua script for atomic check-and-delete
    lua = """
    if redis.call('get', KEYS[1]) == ARGV[1] then
        return redis.call('del', KEYS[1])
    else
        return 0
    end
    """
    await redis.eval(lua, 1, lock_key, lock_token)  # type: ignore


async def get_token_usage(key_id: str) -> int:
    value = await redis.get(f"{KEYPOOL_PREFIX}:{key_id}:usage")
    return int(value) if value is not None else 0


async def add_token_usage(key_id: str, tokens: int):
    await redis.incrby(f"{KEYPOOL_PREFIX}:{key_id}:usage", tokens)
    await redis.expire(f"{KEYPOOL_PREFIX}:{key_id}:usage", 65)


async def reset_token_usage(key_id: str):
    await redis.delete(f"{KEYPOOL_PREFIX}:{key_id}:usage")


async def get_cooldown_until(key_id: str) -> float:
    value = await redis.get(f"{KEYPOOL_PREFIX}:{key_id}:cooldown_until")
    return float(value) if value is not None else 0.0


async def set_cooldown(key_id: str, seconds: float):
    await redis.set(f"{KEYPOOL_PREFIX}:{key_id}:cooldown_until", time.time() + seconds)
    await redis.expire(f"{KEYPOOL_PREFIX}:{key_id}:cooldown_until", int(seconds) + 1)


async def is_in_cooldown(key_id: str) -> bool:
    now = time.time()
    cooldown_until = await get_cooldown_until(key_id)
    return now < cooldown_until


async def get_all_keys() -> list[str]:
    keys: set[str] = await redis.smembers(f"{KEYPOOL_PREFIX}:all")
    return list(keys)


async def add_key(key_id: str, key_name: str) -> None:
    # SADD expects key_id as str or bytes
    await redis.sadd(f"{KEYPOOL_PREFIX}:all", str(key_id))
    await redis.set(f"{KEYPOOL_PREFIX}:{key_id}:name", key_name)


async def remove_key(key_id: str) -> None:
    # SREM expects key_id as str or bytes
    await redis.srem(f"{KEYPOOL_PREFIX}:all", str(key_id))
    await reset_token_usage(key_id)
    # redis.delete is sync in some redis-py versions, so do not await if not awaitable
    result = redis.delete(f"{KEYPOOL_PREFIX}:{key_id}:cooldown_until")
    if hasattr(result, "__await__"):
        await result


# --- Key Acquisition Logic ---
async def acquire_available_key(
    tokens_needed: int, token_limit: int = 200000, timeout: float = 10.0
):
    """
    Atomically acquire a key that is not in cooldown, not locked, and has enough quota.
    Returns (key_id, lock_token) or raises TimeoutError.
    """
    keys = await get_all_keys()
    random.shuffle(keys)
    end = time.time() + timeout
    while time.time() < end:
        for key_id in keys:
            if await is_in_cooldown(key_id):
                continue
            usage = await get_token_usage(key_id)
            if usage + tokens_needed > token_limit:
                continue
            try:
                lock_token = await acquire_lock(key_id, timeout=0.5)
                # Double check after lock
                if await is_in_cooldown(key_id):
                    await release_lock(key_id, lock_token)
                    continue
                usage = await get_token_usage(key_id)
                if usage + tokens_needed > token_limit:
                    await release_lock(key_id, lock_token)
                    continue
                return key_id, lock_token
            except TimeoutError:
                continue
        await asyncio.sleep(0.2)
    raise TimeoutError("No available key could be acquired in time.")
