from core.models.search_stage_results import SearchStageMetadata
from pydantic import BaseModel

from open_ai_key_app.models.field_types import GPTBatchRequestCustomID


class SearchRequestBundle(BaseModel):
    llm_search_request_id: GPTBatchRequestCustomID


SearchRequestMap = dict[str, SearchRequestBundle]
"""
{
    "0:1000" : {
        llm_search_request_id: "101machine.com>products>llm_search>chunk>0:1000"
    },
    "750:1500": {
        ...
    }
    
}
"""


class DeferredSearchRequests(BaseModel):
    metadata: SearchStageMetadata
    request_map: SearchRequestMap
