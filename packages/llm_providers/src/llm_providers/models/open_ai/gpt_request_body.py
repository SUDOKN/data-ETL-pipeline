from __future__ import annotations
from pydantic import BaseModel, ConfigDict, Field
from typing import Literal, Optional

from llm_providers.models.open_ai.gpt_model_params import (
    GPTModelParams,
)

# ─────────────────────────────────────────────────────────────────
# Call-level parameters (used when constructing requests at runtime)
# ─────────────────────────────────────────────────────────────────


class GPTRequestBody(GPTModelParams):
    """
    All params accepted by /v1/chat/completions that are also valid
    inside a batch JSONL body. This is the shared base for both paths.
    """

    model_config = ConfigDict(extra="forbid")

    model: str
    messages: list[dict]

    def system_message(self) -> str:
        """The prompt. Built by the prompt pipeline and pinned to an S3 version —
        read it for provenance, never parse it for data."""
        return self._message_content(0, "system")

    def user_message(self) -> str:
        """The context: the per-subject payload this request was built around.
        Assembled in code, so what goes into it can be read back out of it."""
        return self._message_content(1, "user")

    def _message_content(self, index: int, expected_role: str) -> str:
        try:
            message = self.messages[index]
        except IndexError as e:
            raise ValueError(
                f"expected a {expected_role} message at index {index}, but this "
                f"request has {len(self.messages)} message(s)"
            ) from e

        role = message.get("role")
        if role != expected_role:
            raise ValueError(
                f"expected role {expected_role!r} at message index {index}, got {role!r}"
            )
        return message["content"]


# ─────────────────────────────────────────────────────────────────
# Sync-only extension
# ─────────────────────────────────────────────────────────────────


class GPTSyncRequestBody(GPTRequestBody):
    """
    Adds params that are ONLY valid for synchronous calls.
    Do not use this to build batch JSONL lines.

    stream / stream_options: meaningless in async batch
    service_tier:            batch already IS a distinct service tier
    store / metadata:        only useful attached to a stored completion
    """

    stream: bool = False
    stream_options: Optional[dict] = None  # {"include_usage": True}
    service_tier: Optional[Literal["auto", "default", "flex", "priority"]] = None
    store: Optional[bool] = None
    metadata: Optional[dict] = None  # arbitrary k/v for stored completions
