import logging
import os
import threading
from collections.abc import Iterable
from pathlib import Path

from dotenv import load_dotenv

logger = logging.getLogger(__name__)

ENV_FILE_OVERRIDE_VAR = "SUDOKN_ENV_FILE"
REPO_ROOT_MARKER = ".git"

_loaded_paths: set[Path] = set()
_lock = threading.Lock()


class MissingEnvironmentVariables(RuntimeError):
    """Raised when required environment variables are absent or blank."""

    def __init__(self, missing: list[str], env_path: Path | None) -> None:
        self.missing = missing
        self.env_path = env_path
        source = f" (.env resolved to: {env_path})" if env_path else ""
        super().__init__(
            f"Missing required environment variables{source}: {', '.join(missing)}"
        )


def find_repo_root(start: Path | None = None) -> Path:
    """Walk upwards from `start` (default: cwd) until a directory containing `.git` is found."""
    current = (start or Path.cwd()).resolve()
    for candidate in (current, *current.parents):
        if (candidate / REPO_ROOT_MARKER).exists():
            return candidate
    raise FileNotFoundError(
        f"Could not locate repo root: no {REPO_ROOT_MARKER} found at or above {current}"
    )


def resolve_env_path(env_path: Path | None = None) -> Path:
    if env_path is not None:
        return env_path.resolve()

    override = os.environ.get(ENV_FILE_OVERRIDE_VAR)
    if override:
        return Path(override).resolve()

    return find_repo_root() / ".env"


def load_env(
    required_vars: Iterable[str],
    *,
    env_path: Path | None = None,
    override: bool = False,
) -> None:
    """
    Load the repo-root .env file and fail fast if any required variable is missing.

    Call this once from an entrypoint (app, bot, script, conftest) before doing any work.
    Library code should use `require_env` / `optional_env` instead of calling this.
    """
    path = resolve_env_path(env_path)

    with _lock:
        if path in _loaded_paths:
            logger.debug(
                "Environment already loaded from %s; skipping file read.", path
            )
        elif path.exists():
            load_dotenv(dotenv_path=path, override=override)
            _loaded_paths.add(path)
            logger.info("Loaded environment variables from %s", path)
        else:
            # Deployments may inject variables directly; the required check below is the real gate.
            _loaded_paths.add(path)
            logger.info("No .env file at %s; relying on the process environment.", path)

    missing = [
        name
        for name in dict.fromkeys(required_vars)
        if not os.environ.get(name, "").strip()
    ]
    if missing:
        raise MissingEnvironmentVariables(sorted(missing), path)


def require_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise MissingEnvironmentVariables([name], None)
    return value


def optional_env(name: str, default: str) -> str:
    value = os.environ.get(name, "").strip()
    return value or default
