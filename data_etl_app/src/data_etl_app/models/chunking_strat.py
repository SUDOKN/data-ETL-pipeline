from dataclasses import dataclass

from core.models.prompt import Prompt
from open_ai_key_app.models.gpt_model import LLM_Model


@dataclass
class ChunkingStrat:
    overlap: float  # must be between [0, 1)
    max_chunks: int
    max_tokens: int = 10000

    def __post_init__(self):
        if self.overlap < 0 or self.overlap >= 1:
            raise ValueError("Overlap must be between >=0 and <1")
        if self.max_tokens > 25000:
            raise ValueError("Max Tokens must be less than 25000")


def get_single_shot_chunking_strat(
    gpt_model: LLM_Model, prompt: Prompt
) -> ChunkingStrat:
    # For binary classification, we want to be more conservative with chunking to ensure the model has enough context to make an accurate classification.
    # The exact parameters can be tuned based on experimentation, but as a starting point, we'll use a smaller max_tokens and only allow for 1 chunk to be generated.
    return ChunkingStrat(
        overlap=0,
        max_tokens=gpt_model.max_context_tokens - prompt.num_tokens - 10_000,
        max_chunks=1,
    )


PRODUCT_CHUNKING_STRAT = ChunkingStrat(overlap=0.15, max_tokens=5000, max_chunks=50)
CERTIFICATE_CHUNKING_STRAT = ChunkingStrat(overlap=0.0, max_tokens=7500, max_chunks=25)
MATERIAL_CAP_CHUNKING_STRAT = ChunkingStrat(
    overlap=0.15, max_tokens=5000, max_chunks=50
)
PROCESS_CAP_CHUNKING_STRAT = ChunkingStrat(
    overlap=0.15, max_tokens=2500, max_chunks=100
)
INDUSTRY_CHUNKING_STRAT = ChunkingStrat(overlap=0.15, max_tokens=5000, max_chunks=15)
