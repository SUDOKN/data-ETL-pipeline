from core.models.search_stage_results import (
    SearchStageResults,
    SearchStageStats,
)


class KeywordExtractionStats(SearchStageStats):
    # results: set[str] # just a copy of llm_search
    llm_search: set[str]


KeywordExtractionStatsMap = dict[str, KeywordExtractionStats]
"""
{
    "0:1000" : {
        results: ('keyword1', 'keyword2', ...), 
        llm_search: ('keyword1', 'keyword2', ...)
    },
    "750:1500": {
        ...
    }
    
}
"""


class KeywordExtractionResults(SearchStageResults):
    chunk_stats: KeywordExtractionStatsMap  # chunk map
