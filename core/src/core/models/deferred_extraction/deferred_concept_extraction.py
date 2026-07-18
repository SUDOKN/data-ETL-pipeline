from __future__ import annotations
from pydantic import BaseModel
from typing import Optional
from requests.structures import CaseInsensitiveDict

from core.models.field_types import (
    PhraseAndReasonMap,
    PhraseToTagAndReasonMap,
    TagToPhraseAndReasonMap,
)
from core.models.extraction_results.concept_extraction_results import (
    ConceptExtractionMetadata,
    RecursivelyTaggedPhrase,
)
from core.models.deferred_extraction.deferred_phrase_extraction_requests import (
    LLMPhraseExtractionRequestBundle,
)
from data_etl_app.models.skos_concept import Concept

from open_ai_key_app.models.field_types import GPTBatchRequestCustomID

from core.utils.request_custom_id_util import (
    get_name_from_recursive_grounding_request_custom_id,
    get_level_from_recursive_request_custom_id,
)


class RecursiveTaggingRequest(
    BaseModel
):  # must always corresponds to a valid in-vocab concept
    descend_req_id: GPTBatchRequestCustomID

    @property
    def name(self):
        return get_name_from_recursive_grounding_request_custom_id(self.descend_req_id)

    @property
    def level(self):
        return get_level_from_recursive_request_custom_id(self.descend_req_id)

    def __hash__(self):
        return hash(self.name)

    def is_parent_of(
        self,
        child_tagging_req: RecursiveTaggingRequest,
        match_label_to_concept_map: CaseInsensitiveDict[Concept],
    ) -> bool:
        parent_concept_obj = match_label_to_concept_map.get(self.name)
        if not parent_concept_obj:
            raise ValueError(f"Could not find parent")
        child_concept_obj = match_label_to_concept_map.get(child_tagging_req.name)
        if not child_concept_obj:
            raise ValueError(f"Could not find child")
        return child_concept_obj.name in parent_concept_obj.children


class TaggedResult(
    BaseModel
):  # groups by tag as opposed to phrases in the original grounding
    tag: str
    phrase_reason_map: PhraseAndReasonMap


class TaggedConceptResult(
    BaseModel
):  # used to group phrases and their og tags(name/alt labels) by only concept names
    concept: Concept
    og_tag_w_phrase_reason_map: TagToPhraseAndReasonMap


class ConceptExtractionRequestBundle(LLMPhraseExtractionRequestBundle):
    brute: set[str]
    llm_phrase_initial_grounding_req_id: Optional[GPTBatchRequestCustomID]
    llm_phrase_recursive_tagging_reqs: dict[int, set[RecursiveTaggingRequest]]


ConceptExtractionRequestMap = dict[str, ConceptExtractionRequestBundle]


class DeferredConceptExtractionRequests(BaseModel):
    metadata: ConceptExtractionMetadata
    chunked_request_map: ConceptExtractionRequestMap
