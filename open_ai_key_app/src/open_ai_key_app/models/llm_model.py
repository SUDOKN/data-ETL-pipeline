class LLM_Model:
    def __init__(
        self,
        model_name: str,
        rate_limit_window: int,
        max_context_tokens: int,
        token_limit_per_minute: int,
    ):
        self.model_name = model_name
        self.rate_limit_window = rate_limit_window
        self.max_context_tokens = max_context_tokens
        self.token_limit_per_minute = token_limit_per_minute
