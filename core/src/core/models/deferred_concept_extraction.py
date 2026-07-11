from __future__ import annotations
from pydantic import BaseModel
from typing import Optional

from core.models.field_types import RecursivelyTaggedConceptNode
from core.models.concept_extraction_results import ConceptExtractionMetadata
from core.models.deferred_phrase_extraction_requests import (
    LLMPhraseExtractionRequestBundle,
)

from open_ai_key_app.models.field_types import GPTBatchRequestCustomID


class ConceptExtractionRequestBundle(LLMPhraseExtractionRequestBundle):
    brute: set[str]
    llm_phrase_initial_grounding_req_id: Optional[GPTBatchRequestCustomID]
    llm_phrase_recursive_grounding_root_req_nodes: Optional[
        list[RecursivelyTaggedConceptNode]
    ]


ConceptExtractionRequestMap = dict[str, ConceptExtractionRequestBundle]


class DeferredConceptExtractionRequests(BaseModel):
    metadata: ConceptExtractionMetadata
    chunked_request_map: ConceptExtractionRequestMap
