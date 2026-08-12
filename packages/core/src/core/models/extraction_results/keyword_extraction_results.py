from pydantic import BaseModel

from core.models.extraction_results.llm_phrase_extraction_results import (
    ExtractionNodeMetadata,
    LLMPhraseExtractionMetadata,
    LLMPhraseExtractionStats,
)
from core.models.extraction_schemas.grounding import (
    PhraseToTagAndRulesMap,
)


class KeywordExtractionStats(LLMPhraseExtractionStats):
    results: set[str]
    # round → {phrase: {tag: [applied_rule, ...]}} — each phrase assigned to its earliest search round
    llm_phrase_freehand_grounding: dict[int, PhraseToTagAndRulesMap]


KeywordExtractionStatsMap = dict[str, KeywordExtractionStats]


class BatchedFreehandGroundingNodeMetadata(ExtractionNodeMetadata):
    # Hard cap on phrase pairs per freehand-grounding request. The screened
    # phrases for a chunk are split into ceil(num_pairs / max_pairs_per_request)
    # groups, each grounded independently and merged back into one flat result.
    max_pairs_per_request: int

    def to_custom_id_segment(self) -> str:
        return f"{super().to_custom_id_segment()}|gs={self.max_pairs_per_request}"


class KeywordExtractionMetadata(LLMPhraseExtractionMetadata):
    llm_phrase_freehand_grounding: BatchedFreehandGroundingNodeMetadata


class KeywordExtractionResults(BaseModel):
    metadata: KeywordExtractionMetadata
    results: set[str]
    chunk_stats: KeywordExtractionStatsMap  # chunk map
