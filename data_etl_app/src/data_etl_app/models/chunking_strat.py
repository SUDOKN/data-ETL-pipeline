from pydantic import BaseModel

from core.models.prompt import Prompt
from litellm_proxy_app.models.llm_model import LLM_Model


class ChunkingStrategy(BaseModel):
    overlap: float  # must be between [0, 1)
    max_chunks: int
    max_tokens_per_chunk: int

    def __post_init__(self):
        if self.overlap < 0 or self.overlap >= 1:
            raise ValueError("Overlap must be between >=0 and <1")
        if self.max_tokens_per_chunk >= 128000:
            raise ValueError("Max Tokens must be less than 128000")


def get_binary_classification_chunking_strat(prompt: Prompt) -> ChunkingStrategy:
    return get_single_shot_chunking_strat(
        max_context_tokens=BINARY_CLASSIFICATION_CHUNKING_STRAT_MAX_TOKENS,
        prompt=prompt,
    )


def get_single_shot_chunking_strat(
    max_context_tokens: int, prompt: Prompt
) -> ChunkingStrategy:
    # For binary classification, we want to be more conservative with chunking to ensure the model has enough context to make an accurate classification.
    # The exact parameters can be tuned based on experimentation, but as a starting point, we'll use a smaller max_tokens and only allow for 1 chunk to be generated.
    return ChunkingStrategy(
        overlap=0,
        max_tokens_per_chunk=max_context_tokens - prompt.num_tokens - 10_000,
        max_chunks=1,
    )


BINARY_CLASSIFICATION_CHUNKING_STRAT_MAX_TOKENS = 128_000

PRODUCT_CHUNKING_STRAT = ChunkingStrategy(
    overlap=0.15, max_tokens_per_chunk=5000, max_chunks=10
)
CERTIFICATE_CHUNKING_STRAT = ChunkingStrategy(
    overlap=0.0, max_tokens_per_chunk=5000, max_chunks=10
)
MATERIAL_CAP_CHUNKING_STRAT = ChunkingStrategy(
    overlap=0.15, max_tokens_per_chunk=5000, max_chunks=10
)
PROCESS_CAP_CHUNKING_STRAT = ChunkingStrategy(
    overlap=0.15, max_tokens_per_chunk=2500, max_chunks=10  # TODO: change back to 100
)
INDUSTRY_CHUNKING_STRAT = ChunkingStrategy(
    overlap=0.15, max_tokens_per_chunk=5000, max_chunks=10
)
