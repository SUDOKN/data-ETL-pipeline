from pydantic import BaseModel

from llm_providers.models.file_objects.prompt import Prompt
from llm_providers.models.llm_model import LLM_Model
from llm_providers.utils.chunk_util import get_chunks_respecting_line_boundaries


class ChunkingStrategy(BaseModel):
    overlap: float  # must be between [0, 1)
    max_chunks: int
    max_tokens_per_chunk: int
    # How many sub-windows the search + recursive-search stages split each chunk
    # into. The chunk itself (and every stage from phrase_relationship down) is
    # untouched: relationship reads the full chunk text, search reads
    # max_tokens_per_chunk // search_divisor sized windows of it. 1 = search on
    # whole chunks, exactly the pre-divisor behavior.
    search_divisor: int = 1

    def __init__(self, **data):
        super().__init__(**data)
        if self.overlap < 0 or self.overlap >= 1:
            raise ValueError("Overlap must be between >=0 and <1")
        if self.max_tokens_per_chunk >= 128000:
            raise ValueError("Max Tokens must be less than 128000")
        if self.search_divisor < 1:
            raise ValueError("search_divisor must be >= 1")


async def derive_search_sub_bounds(
    chunk_bounds: str,
    chunk_text: str,
    chunk_strategy: ChunkingStrategy,
    llm_model: LLM_Model,
) -> list[str]:
    """Absolute ``start:end`` bounds of the search sub-windows of one chunk.

    Bounds share the chunk's coordinate system (character offsets into the full
    subject text), so ``subject_text[start:end]`` yields a sub-window's text
    anywhere downstream without re-chunking. Derived once at prefill and stored
    on the bundle: embedding sub-window request ids happens in stages that don't
    hold the text, so the geometry has to already be there.
    """
    if chunk_strategy.search_divisor == 1:
        return [chunk_bounds]

    chunk_start = int(chunk_bounds.split(":")[0])
    sub_map = await get_chunks_respecting_line_boundaries(
        text=chunk_text,
        soft_limit_tokens=chunk_strategy.max_tokens_per_chunk
        // chunk_strategy.search_divisor,
        overlap_ratio=chunk_strategy.overlap,
        max_chunks=None,  # sub-windows must cover the whole chunk
        llm_model=llm_model,
    )
    return [
        f"{chunk_start + int(rel_bounds.split(':')[0])}:{chunk_start + int(rel_bounds.split(':')[1])}"
        for rel_bounds in sub_map.keys()
    ]


def get_binary_classification_chunking_strat(prompt: Prompt) -> ChunkingStrategy:
    return _get_single_shot_chunking_strat(
        max_context_tokens=BINARY_CLASSIFICATION_CHUNKING_STRAT_MAX_TOKENS,
        prompt=prompt,
    )


def get_basic_field_chunking_strat(prompt: Prompt) -> ChunkingStrategy:
    return _get_single_shot_chunking_strat(
        max_context_tokens=BASIC_FIELD_EXTRACTION_CHUNKING_STRAT_MAX_TOKENS,
        prompt=prompt,
    )


def _get_single_shot_chunking_strat(
    max_context_tokens: int, prompt: Prompt
) -> ChunkingStrategy:
    # For binary classification, we want to be more conservative with chunking to ensure the model has enough context to make an accurate classification.
    # The exact parameters can be tuned based on experimentation, but as a starting point, we'll use a smaller max_tokens and only allow for 1 chunk to be generated.
    return ChunkingStrategy(
        overlap=0,
        max_tokens_per_chunk=max_context_tokens - prompt.num_tokens - 10_000,
        max_chunks=1,
    )


DEFAULT_MAX_CHUNKS = 2


BINARY_CLASSIFICATION_CHUNKING_STRAT_MAX_TOKENS = 120_000
BASIC_FIELD_EXTRACTION_CHUNKING_STRAT_MAX_TOKENS = 120_000

shallow = ChunkingStrategy(
    overlap=0.15, max_tokens_per_chunk=5000, max_chunks=DEFAULT_MAX_CHUNKS
)

medium = ChunkingStrategy(
    overlap=0.15, max_tokens_per_chunk=10_000, max_chunks=DEFAULT_MAX_CHUNKS
)

wide = ChunkingStrategy(
    overlap=0.15,
    max_tokens_per_chunk=20_000,  # <- the 20k knob
    max_chunks=2,
    search_divisor=4,  # <- 20k / 4 = 5k search windows
)

PRODUCT_CHUNKING_STRAT = wide
EQUIPMENT_CHUNKING_STRAT = wide
CONFORMITY_ATTESTATION_CHUNKING_STRAT = wide
MATERIAL_CAP_CHUNKING_STRAT = wide
PROCESS_CAP_CHUNKING_STRAT = wide
INDUSTRY_CHUNKING_STRAT = wide
