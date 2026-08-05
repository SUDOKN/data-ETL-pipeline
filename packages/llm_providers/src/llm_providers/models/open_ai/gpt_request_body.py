from __future__ import annotations
from pydantic import BaseModel, ConfigDict, Field
from typing import Literal, Optional

from packages.llm_providers.src.llm_providers.models.open_ai.gpt_model_params import (
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
