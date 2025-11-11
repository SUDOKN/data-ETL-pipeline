from typing import Optional


# --- GPT Model Settings ---
class GPTModel:
    def __init__(
        self,
        model_name: str,
        rate_limit_window: int,
        max_context_tokens: int,
        token_limit_per_minute: int,
        safe_completion_tokens: int,
    ):
        self.model_name = model_name
        self.rate_limit_window = rate_limit_window
        self.max_context_tokens = max_context_tokens
        self.token_limit_per_minute = token_limit_per_minute
        self.safe_completion_tokens = safe_completion_tokens


class ModelParameters:
    def __init__(
        self,
        temperature: float = 1,
        top_p: float = 1,
        presence_penalty: float = 0,
        frequency_penalty: float = 0,
        max_tokens: Optional[int] = None,
    ):
        self.temperature: float = temperature
        self.top_p: float = top_p
        self.presence_penalty: float = presence_penalty
        self.frequency_penalty: float = frequency_penalty
        self.max_tokens: Optional[int] = max_tokens


DefaultModelParameters = ModelParameters()

GPT_4o_mini = GPTModel(
    model_name="gpt-4o-mini",
    rate_limit_window=60,
    max_context_tokens=128000,
    token_limit_per_minute=200000,
    safe_completion_tokens=7500,
)
