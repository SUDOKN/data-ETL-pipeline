from pydantic import BaseModel

from core.models.extraction_results.llm_phrase_extraction_results import (
    ExtractionNodeMetadata,
    LLMPhraseExtractionMetadata,
    LLMPhraseExtractionStats,
)
from core.models.extraction_schemas.grounding import (
    PhraseToTagAndReasonMap,
)


class KeywordExtractionStats(LLMPhraseExtractionStats):
    results: set[str]
    # round → {phrase: {tag: reason}} — each phrase assigned to its earliest search round
    llm_phrase_freehand_grounding: dict[int, PhraseToTagAndReasonMap]


KeywordExtractionStatsMap = dict[str, KeywordExtractionStats]


class KeywordExtractionMetadata(LLMPhraseExtractionMetadata):
    llm_phrase_freehand_grounding: ExtractionNodeMetadata


class KeywordExtractionResults(BaseModel):
    metadata: KeywordExtractionMetadata
    results: set[str]
    chunk_stats: KeywordExtractionStatsMap  # chunk map
