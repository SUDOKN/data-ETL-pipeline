from pydantic import BaseModel

from core.models.llm_phrase_extraction_results import (
    LLMPhraseExtractionMetadata,
    LLMPhraseExtractionStats,
)


class KeywordExtractionStats(LLMPhraseExtractionStats):
    pass


KeywordExtractionStatsMap = dict[str, KeywordExtractionStats]


class KeywordExtractionMetadata(LLMPhraseExtractionMetadata):
    pass


class KeywordExtractionResults(BaseModel):
    metadata: KeywordExtractionMetadata
    chunk_stats: KeywordExtractionStatsMap  # chunk map
