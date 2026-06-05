# Provider-agnostic model config for the LiteLLM proxy path.
# Unlike open_ai_key_app.models.gpt_model.GPTModel, this class omits
# rate-limit bookkeeping (token_limit_per_minute, rate_limit_window) because
# rate-limit management is now delegated to the LiteLLM proxy.
class LLM_Model:
    def __init__(
        self,
        model_name: str,
        max_context_tokens: int,
    ):
        self.model_name = model_name
        self.max_context_tokens = max_context_tokens


# ── OpenAI ──────────────────────────────────────────────────────────────────
GPT_4o_mini = LLM_Model(
    model_name="gpt-4o-mini",
    max_context_tokens=128_000,
)

GPT_4o = LLM_Model(
    model_name="gpt-4o",
    max_context_tokens=128_000,
)

GPT_4_1 = LLM_Model(
    model_name="gpt-4.1",
    # max_context_tokens=1_047_576,
    max_context_tokens=128_000,
)

GPT_4_1_mini = LLM_Model(
    model_name="gpt-4.1-mini",
    # max_context_tokens=1_047_576,
    max_context_tokens=128_000,
)

GPT_4_1_nano = LLM_Model(
    model_name="gpt-4.1-nano",
    # max_context_tokens=1_047_576,
    max_context_tokens=128_000,
)


GPT_5_nano = LLM_Model(
    model_name="gpt-5-nano",
    max_context_tokens=128_000,
)

GPT_5_mini = LLM_Model(
    model_name="gpt-5-mini",
    max_context_tokens=128_000,
)

GPT_5 = LLM_Model(
    model_name="gpt-5",
    max_context_tokens=128_000,
)

GPT_5_1 = LLM_Model(
    model_name="gpt-5.1",
    max_context_tokens=272_000,
)

GPT_5_2 = LLM_Model(
    model_name="gpt-5.2",
    max_context_tokens=272_000,
)

GPT_o3_mini = LLM_Model(
    model_name="o3-mini",
    max_context_tokens=272_000,
)

No_model = LLM_Model(
    model_name="no_model",
    max_context_tokens=100_000_000,
)

# ── Anthropic ────────────────────────────────────────────────────────────────
Claude_3_5_Haiku = LLM_Model(
    model_name="claude-3-5-haiku-20241022",
    max_context_tokens=200_000,
)

Claude_3_5_Sonnet = LLM_Model(
    model_name="claude-3-5-sonnet-20241022",
    max_context_tokens=200_000,
)

Claude_3_7_Sonnet = LLM_Model(
    model_name="claude-3-7-sonnet-20250219",
    max_context_tokens=200_000,
)

# ── Google Gemini ─────────────────────────────────────────────────────────────
Gemini_2_0_Flash = LLM_Model(
    model_name="gemini/gemini-2.0-flash",
    max_context_tokens=1_048_576,
)

Gemini_2_5_Flash = LLM_Model(
    model_name="gemini/gemini-2.5-flash-preview-04-17",
    max_context_tokens=1_048_576,
)

Gemini_2_5_Pro = LLM_Model(
    model_name="gemini/gemini-2.5-pro-preview-05-06",
    max_context_tokens=1_048_576,
)

# Default model used when no model is specified
DefaultLLMModel: LLM_Model = GPT_4o_mini


MODEL_REGISTRY: list[LLM_Model] = [
    GPT_4o_mini,
    GPT_4_1,
    GPT_4_1_mini,
    GPT_o3_mini,
    GPT_5_nano,
    GPT_5_mini,
    GPT_5,
    GPT_5_1,
    GPT_5_2,
]
