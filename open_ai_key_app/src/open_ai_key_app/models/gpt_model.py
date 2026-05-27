from open_ai_key_app.models.llm_model import LLM_Model

# DefaultModelParameters = GPTModelParams(
#     temperature=0,  # Greedy decoding — always picks highest probability token
#     top_p=1,  # No nucleus sampling restriction needed when temp=0
#     presence_penalty=0,  # No penalty adjustments that could shift token selection
#     frequency_penalty=0,  # Same — keep it neutral
#     seed=12345,  # NEW: explicitly request deterministic sampling
#     max_completion_tokens=7500,  # NEW: explicitly set max tokens
# )

GPT_4o_mini = LLM_Model(
    model_name="gpt-4o-mini",
    rate_limit_window=60,
    max_context_tokens=128_000,
    token_limit_per_minute=10_000_000,
)

GPT_4_1 = LLM_Model(
    model_name="gpt-4.1",
    rate_limit_window=60,
    max_context_tokens=128_000,
    token_limit_per_minute=10_000_000,
)

GPT_4_1_mini = LLM_Model(
    model_name="gpt-4.1-mini",
    rate_limit_window=60,
    max_context_tokens=128_000,
    token_limit_per_minute=10_000_000,
)

GPT_5_nano = LLM_Model(
    model_name="gpt-5-nano",
    rate_limit_window=60,
    max_context_tokens=128_000,
    token_limit_per_minute=10_000_000,
)

GPT_5_mini = LLM_Model(
    model_name="gpt-5-mini",
    rate_limit_window=60,
    max_context_tokens=128_000,
    token_limit_per_minute=10_000_000,
)

GPT_5 = LLM_Model(
    model_name="gpt-5",
    rate_limit_window=60,
    max_context_tokens=128_000,
    token_limit_per_minute=10_000_000,
)

GPT_5_1 = LLM_Model(
    model_name="gpt-5.1",
    rate_limit_window=60,
    max_context_tokens=272_000,
    token_limit_per_minute=4_000_000,
)

GPT_5_2 = LLM_Model(
    model_name="gpt-5.2",
    rate_limit_window=60,
    max_context_tokens=272_000,
    token_limit_per_minute=4_000_000,
)

GPT_o3_mini = LLM_Model(
    model_name="o3-mini",
    rate_limit_window=60,
    max_context_tokens=272_000,
    token_limit_per_minute=4_000_000,
)

No_model = LLM_Model(
    model_name="no_model",
    rate_limit_window=60,
    max_context_tokens=100_000_000,
    token_limit_per_minute=100_000_000,
)

# Maps model names to tiktoken encoding names
MODEL_ENCODING_MAP: dict[str, str] = {
    "gpt-4o-mini": "o200k_base",
    "gpt-5.2": "o200k_base",
}

# All models managed by the keypool. Each entry creates one slot per API key.
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
