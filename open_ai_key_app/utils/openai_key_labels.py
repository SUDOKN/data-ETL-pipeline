import os

KEYPOOL_PREFIX = os.getenv("KEYPOOL_PREFIX")
if not KEYPOOL_PREFIX:
    raise ValueError(
        "KEYPOOL_PREFIX environment variable is not set. Please set it in your .env file."
    )


def get_openai_key_name_label(api_key: str) -> str:
    return f"{KEYPOOL_PREFIX}:{api_key}:name"


def get_cooldown_key_label(key_name: str) -> str:
    return f"{KEYPOOL_PREFIX}:{key_name}:cooldown_until"


def get_exhausted_msg_key_prefix() -> str:
    return f"{KEYPOOL_PREFIX}:exhausted_msg"


def get_exhausted_msg_key_label(key_name: str) -> str:
    return f"{get_exhausted_msg_key_prefix()}:{key_name}"


def get_usage_key_label(key_name: str) -> str:
    return f"{KEYPOOL_PREFIX}:{key_name}:usage"


def get_lock_key_label(key_name: str) -> str:
    return f"{KEYPOOL_PREFIX}:{key_name}:lock"


def get_all_keys_label() -> str:
    return f"{KEYPOOL_PREFIX}:all"
