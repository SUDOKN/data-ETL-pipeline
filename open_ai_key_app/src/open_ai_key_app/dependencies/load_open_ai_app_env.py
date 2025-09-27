import logging
import threading
from pathlib import Path
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

# Resolve core/.env relative to this file
DOT_ENV_PATH = Path(__file__).resolve().parents[2] / ".env"

_loaded = False
_lock = threading.Lock()


def load_open_ai_app_env(
    *, required: bool = True, override: bool = False, env_path: Path | None = None
) -> None:
    """
    Load environment variables from the core .env file.
    - Idempotent by default.
    - required=True: raise FileNotFoundError if .env is missing.
    - override=True: .env values overwrite existing environment variables.
    """
    global _loaded
    with _lock:
        if _loaded:
            logger.info("Shared environment variables already loaded; skipping.")
            return

        path = env_path or DOT_ENV_PATH
        if not path.exists():
            msg = f".env file not found at: {path}"
            if required:
                raise FileNotFoundError(msg)
            logger.info(f"{msg}. Skipping load.")
            _loaded = True
            return

        load_dotenv(dotenv_path=path, override=override)
        logger.info(f"Loaded core environment variables from: {path}")
        _loaded = True
