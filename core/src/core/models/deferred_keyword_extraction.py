from typing import Optional

from core.models.deferred_search_requests import (
    DeferredSearchRequests,
    SearchRequestBundle,
)
from core.models.keyword_extraction_results import (
    KeywordExtractionMetadata,
)
from open_ai_key_app.models.field_types import GPTBatchRequestCustomID


class KeywordExtractionRequestBundle(SearchRequestBundle):
    llm_evidence_request_id: Optional[GPTBatchRequestCustomID]


KeywordExtractionRequestMap = dict[str, KeywordExtractionRequestBundle]
"""
{
    "0:1000" : {
        llm_search_request_id: "101machine.com>products>llm_search>chunk>0:1000",
        llm_evidence_request_id: "101machine.com>products>llm_evidence>chunk>0:1000",
    },
    "750:1500": {
        ...
    }

}
"""


class DeferredKeywordExtractionRequests(DeferredSearchRequests):
    metadata: KeywordExtractionMetadata
    request_map: KeywordExtractionRequestMap
