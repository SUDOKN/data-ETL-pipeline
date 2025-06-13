"""
DO NOT USE THIS UTIL DIRECTLY.
It is designed to load API keys from the local .env file or seed file
and allow KeyPoolManager to add/remove them in Redis.
"""

import os
from open_ai_key_app.utils.redis_client_util import redis
from open_ai_key_app.utils.openai_key_labels_util import (
    get_all_keys_label,
    get_openai_key_name_label,
    get_cooldown_key_label,
    get_exhausted_msg_key_prefix,
)

# OPEN AI KEY MANAGEMENT ------------------------------------------------------ #
KEYPOOL_PREFIX = os.getenv("KEYPOOL_PREFIX")
if not KEYPOOL_PREFIX:
    raise ValueError(
        "KEYPOOL_PREFIX environment variable is not set. Please set it in your .env file."
    )

LOCK_EXPIRY = os.getenv("LOCK_EXPIRY")
if not LOCK_EXPIRY:
    raise ValueError(
        "LOCK_EXPIRY environment variable is not set. Please set it in your .env file."
    )

LOCK_EXPIRY_IN_FLOAT = float(LOCK_EXPIRY)


def get_all_openai_keys() -> dict[str, str]:
    keys: set[str] = redis.smembers(get_all_keys_label())
    key_map = {get_openai_key_name(k): k for k in keys}
    return key_map


def get_openai_key_name(api_key: str) -> str:
    key_name = redis.get(get_openai_key_name_label(api_key))
    if key_name is None:
        raise ValueError(f"Key with ID {api_key} does not exist.")
    return key_name


def add_openai_key(api_key: str, key_name: str) -> None:
    # SADD expects key_id as str or bytes
    redis.sadd(get_all_keys_label(), str(api_key))
    redis.set(get_openai_key_name_label(api_key), key_name)


def remove_openai_key(api_key: str, key_name: str) -> None:
    redis.srem(get_all_keys_label(), api_key)
    redis.delete(get_cooldown_key_label(key_name))


def remove_all_openai_keys() -> None:
    """
    Remove all OpenAI keys from the Redis database.
    """
    keys = redis.smembers(get_all_keys_label())
    for key in keys:
        redis.delete(get_openai_key_name_label(key))
    redis.delete(get_all_keys_label())


def get_exhausted_keys() -> dict[str, str]:
    """
    Get all keys that are currently exhausted (i.e., in cooldown).
    """
    keys = redis.keys(f"{get_exhausted_msg_key_prefix()}:*")
    exhausted_key_map = {k.split(":")[1]: redis.get(k) or "" for k in keys}
    return exhausted_key_map


# ----------------------------------------------------------------------- #
