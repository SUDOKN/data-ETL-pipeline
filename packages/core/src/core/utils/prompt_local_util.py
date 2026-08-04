import os
from pathlib import Path

# Relative (or absolute) path to the local prompts directory, set via the env.
LOCAL_PROMPTS_DIR = os.getenv("LOCAL_PROMPTS_DIR")


def get_local_prompts_dir() -> Path:
    """Resolve the local prompts directory from the LOCAL_PROMPTS_DIR env var.

    A relative path is resolved against the current working directory.
    """
    if not LOCAL_PROMPTS_DIR:
        raise ValueError("LOCAL_PROMPTS_DIR is not set. Please check your .env file.")

    prompts_dir = Path(LOCAL_PROMPTS_DIR).expanduser()
    if not prompts_dir.is_absolute():
        prompts_dir = (Path.cwd() / prompts_dir).resolve()

    if not prompts_dir.is_dir():
        raise FileNotFoundError(
            f"Local prompts directory does not exist: {prompts_dir}"
        )

    return prompts_dir


def read_local_prompt(prompt_filename: str) -> str:
    """Read a prompt file from the local prompts directory.

    :param prompt_filename: The name of the prompt file to read (e.g. "is_manufacturer.txt").
    :return: The prompt file content as a string.
    """
    if not prompt_filename:
        raise ValueError("Prompt filename must be provided")

    prompt_path = get_local_prompts_dir() / prompt_filename
    if not prompt_path.is_file():
        raise FileNotFoundError(f"Local prompt file not found: {prompt_path}")

    return prompt_path.read_text(encoding="utf-8")
