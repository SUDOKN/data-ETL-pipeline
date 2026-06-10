from typing import Optional

from core.models.deferred_search_requests import (
    DeferredSearchRequests,
    SearchRequestBundle,
)
from core.models.concept_extraction_results import ConceptExtractionMetadata
from open_ai_key_app.models.field_types import GPTBatchRequestCustomID


class ConceptExtractionRequestBundle(SearchRequestBundle):
    brute: set[str]
    llm_evidence_request_id: Optional[GPTBatchRequestCustomID]
    llm_mapping_request_id: Optional[GPTBatchRequestCustomID]


ConceptExtractionRequestMap = dict[str, ConceptExtractionRequestBundle]
"""
{
    "0:1000" : {
        llm_search_request_id: "101machine.com>products>llm_search>chunk>0:1000",
        llm_evidence_request_id: "101machine.com>products>llm_evidence>chunk>0:1000",
        llm_mapping_request_id: "101machine.com>products>llm_mapping>chunk>0:1000",
    },
    "750:1500": {
        ...
    }
    
}
"""


class DeferredConceptExtractionRequests(DeferredSearchRequests):
    metadata: ConceptExtractionMetadata
    request_map: ConceptExtractionRequestMap
