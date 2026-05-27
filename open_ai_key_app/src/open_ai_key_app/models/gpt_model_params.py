from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict, Field

# ─────────────────────────────────────────────────────────────────
# OpenAI API parameter sub-groups
# ─────────────────────────────────────────────────────────────────


class GPTSamplingParams(BaseModel):
    """
    Core sampling controls. Set one of temperature or top_p, not both.
    Unsupported on o-series reasoning models — do not set alongside
    reasoning_effort or the request will be rejected.
    """

    temperature: float = Field(default=1.0, ge=0.0, le=2.0)
    top_p: float = Field(default=1.0, ge=0.0, le=1.0)
    presence_penalty: float = Field(default=0.0, ge=-2.0, le=2.0)
    frequency_penalty: float = Field(default=0.0, ge=-2.0, le=2.0)
    seed: Optional[int] = None

    @classmethod
    def with_defaults(cls) -> "GPTSamplingParams":
        return cls(
            temperature=1.0,
            top_p=1.0,
            presence_penalty=0.0,
            frequency_penalty=0.0,
            seed=None,
        )


class GPTOutputParams(BaseModel):
    """
    Controls the shape and length of the output.
    """

    max_completion_tokens: int = 7500
    stop: Optional[str | list[str]] = None  # up to 4 sequences
    n: Optional[int] = Field(default=None, ge=1)  # keep at 1 for batch
    response_format: Optional[dict] = None
    # {"type": "text"}
    # {"type": "json_object"}
    # {"type": "json_schema", "json_schema": {"name": "...", "schema": {...}, "strict": True}}

    @classmethod
    def with_defaults(cls) -> "GPTOutputParams":
        # max_completion_tokens=0 is a sentinel: real values are always >= 1,
        # so this field always appears in the custom_id segment.
        return cls(stop=None, max_completion_tokens=7500, response_format=None, n=None)


class GPTLogprobParams(BaseModel):
    """
    Token probability introspection. Useful for experiment calibration.
    logprobs must be True to use top_logprobs.
    """

    logprobs: Optional[bool] = None
    top_logprobs: Optional[int] = Field(default=None, ge=0, le=20)

    @classmethod
    def with_defaults(cls) -> "GPTLogprobParams":
        return cls(logprobs=None, top_logprobs=None)


class GPTToolParams(BaseModel):
    """
    Function / tool calling configuration.
    """

    tools: Optional[list[dict]] = None
    tool_choice: Optional[str | dict] = None
    # "none" | "auto" | "required" | {"type": "function", "function": {"name": "..."}}
    parallel_tool_calls: Optional[bool] = None  # default True; False = sequential

    @classmethod
    def with_defaults(cls) -> "GPTToolParams":
        return cls(tools=None, tool_choice=None, parallel_tool_calls=None)


class GPTMiscParams(BaseModel):
    """
    Miscellaneous params valid in both batch and sync contexts.
    """

    logit_bias: Optional[dict[str, int]] = None  # token_id (str) → -100..100
    user: Optional[str] = None  # end-user ID for abuse detection

    @classmethod
    def with_defaults(cls) -> "GPTMiscParams":
        return cls(logit_bias=None, user=None)


class GPTReasoningParams(BaseModel):
    """
    o-series reasoning model controls.
    When set, do NOT also set temperature / top_p / presence_penalty /
    frequency_penalty — OpenAI will reject the request.
    """

    reasoning_effort: Optional[Literal["low", "medium", "high"]] = None

    @classmethod
    def with_defaults(cls) -> "GPTReasoningParams":
        return cls(reasoning_effort=None)


# ─────────────────────────────────────────────────────────────────
# Shared body — valid in BOTH batch and sync
# ─────────────────────────────────────────────────────────────────


class GPTModelParams(
    GPTSamplingParams,
    GPTOutputParams,
    GPTLogprobParams,
    GPTToolParams,
    GPTMiscParams,
    GPTReasoningParams,
):
    @classmethod
    def with_defaults(cls) -> "GPTModelParams":
        merged: dict = {}
        for sub in [
            GPTSamplingParams,
            GPTOutputParams,
            GPTLogprobParams,
            GPTToolParams,
            GPTMiscParams,
            GPTReasoningParams,
        ]:
            merged.update(sub.with_defaults().model_dump())
        return cls(**merged)

    def to_custom_id_segment(self, model_name: str) -> str:
        defaults = GPTModelParams.with_defaults()
        non_default = [
            f"{field}={getattr(self, field)}"
            for field in self.__class__.model_fields
            if getattr(self, field) != getattr(defaults, field)
        ]
        if non_default:
            return model_name + "|" + "|".join(non_default)
        return model_name


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
