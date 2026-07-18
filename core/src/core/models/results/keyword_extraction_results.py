from pydantic import BaseModel

from core.models.results.llm_phrase_extraction_results import (
    ExtractionNodeMetadata,
    LLMPhraseExtractionMetadata,
    LLMPhraseExtractionStats,
)
from core.models.field_types import PhraseToTagAndReasonMap


class KeywordExtractionStats(LLMPhraseExtractionStats):
    results: set[str]
    llm_phrase_freehand_grounding: PhraseToTagAndReasonMap


KeywordExtractionStatsMap = dict[str, KeywordExtractionStats]


class KeywordExtractionMetadata(LLMPhraseExtractionMetadata):
    llm_phrase_freehand_grounding: ExtractionNodeMetadata


class KeywordExtractionResults(BaseModel):
    metadata: KeywordExtractionMetadata
    results: set[str]
    chunk_stats: KeywordExtractionStatsMap  # chunk map
