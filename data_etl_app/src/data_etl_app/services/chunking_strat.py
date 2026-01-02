from dataclasses import dataclass


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


PRODUCT_CHUNKING_STRAT = ChunkingStrat(overlap=0.15, max_tokens=5000, max_chunks=50)
CERTIFICATE_CHUNKING_STRAT = ChunkingStrat(overlap=0.0, max_tokens=7500, max_chunks=25)
MATERIAL_CAP_CHUNKING_STRAT = ChunkingStrat(
    overlap=0.15, max_tokens=5000, max_chunks=50
)
PROCESS_CAP_CHUNKING_STRAT = ChunkingStrat(
    overlap=0.15, max_tokens=2500, max_chunks=100
)
INDUSTRY_CHUNKING_STRAT = ChunkingStrat(overlap=0.15, max_tokens=5000, max_chunks=15)
