from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, ConfigDict, Field


class LLMSamplingParams(BaseModel):
    """
    Core sampling controls. Set one of temperature or top_p, not both.
    Unsupported on o-series reasoning models — do not set alongside
    reasoning_effort or the request will be rejected.
    """

    temperature: float = Field(ge=0.0, le=2.0)
    top_p: float = Field(ge=0.0, le=1.0)
    presence_penalty: float = Field(ge=-2.0, le=2.0)
    frequency_penalty: float = Field(ge=-2.0, le=2.0)
    seed: Optional[int] = None


class LLMOutputParams(BaseModel):
    """
    Controls the shape and length of the output.
    """

    max_completion_tokens: int = Field(ge=1)


class LLMModelParams(
    LLMSamplingParams,
    LLMOutputParams,
):
    """
    Comprehensive set of model parameters for the LiteLLM proxy path.  Combines
    sampling and output controls.

    Note that rate-limit management parameters (token_limit_per_minute,
    rate_limit_window) are intentionally omitted, as rate-limit management is
    now delegated to the LiteLLM proxy.
    """

    pass
