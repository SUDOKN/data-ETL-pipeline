import logging
import threading
from pathlib import Path
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

# Resolves to litellm_proxy_app/src/.env
# parents[0] = .../dependencies/
# parents[1] = .../litellm_proxy_app/
# parents[2] = .../src/
DOT_ENV_PATH = Path(__file__).resolve().parents[2] / ".env"

_loaded = False
_lock = threading.Lock()


def load_litellm_env(
    *, required: bool = True, override: bool = False, env_path: Path | None = None
) -> None:
    """
    Load client-side LiteLLM environment variables (LITELLM_PROXY_URL, LITELLM_VIRTUAL_KEY).
    - Idempotent by default.
    - required=True: raise FileNotFoundError if .env is missing.
    - override=True: .env values overwrite existing environment variables.
    """
    global _loaded
    with _lock:
        if _loaded:
            logger.info("LiteLLM proxy environment variables already loaded; skipping.")
            return

        path = env_path or DOT_ENV_PATH
        if not path.exists():
            msg = f"LiteLLM proxy .env file not found at: {path}"
            if required:
                raise FileNotFoundError(msg)
            logger.info(f"{msg}. Skipping load.")
            _loaded = True
            return

        load_dotenv(dotenv_path=path, override=override)
        logger.info(f"Loaded LiteLLM proxy environment variables from: {path}")
        _loaded = True
