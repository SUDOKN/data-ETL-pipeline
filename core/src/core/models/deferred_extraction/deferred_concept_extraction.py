from __future__ import annotations
from pydantic import BaseModel
from typing import Optional
import logging

from core.models.extraction_schemas.legacy_mapping_types import (
    PhraseAndReasonMap,
    TagToPhraseAndReasonMap,
)
from core.models.extraction_results.concept_extraction_results import (
    ConceptExtractionMetadata,
)
from core.models.deferred_extraction.deferred_phrase_extraction_requests import (
    LLMPhraseExtractionRequestBundle,
)
from core.models.skos_concept import Concept

from open_ai_key_app.models.field_types import GPTBatchRequestCustomID

from core.utils.request_custom_id_util import (
    get_name_from_recursive_grounding_request_custom_id,
    get_level_from_recursive_request_custom_id,
)

logger = logging.getLogger(__name__)


class IterativeTaggingRequest(
    BaseModel
):  # must always corresponds to a valid in-vocab concept
    parent_descend_req_id: Optional[GPTBatchRequestCustomID]
    descend_req_id: GPTBatchRequestCustomID

    @property
    def name(self):
        return get_name_from_recursive_grounding_request_custom_id(self.descend_req_id)

    @property
    def level(self):
        return get_level_from_recursive_request_custom_id(self.descend_req_id)

    def __hash__(self) -> int:
        return hash(self.name)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, IterativeTaggingRequest):
            return NotImplemented
        return self.__hash__() == other.__hash__()

    def is_parent_of(
        self,
        child_tagging_req: IterativeTaggingRequest,
    ) -> bool:
        retval = child_tagging_req.parent_descend_req_id == self.descend_req_id
        logger.info(f"{self.name} is parent of {child_tagging_req.name}: {retval}")
        return retval


class TaggingResult(BaseModel):
    """
    groups by tag which is common across phrases
    as opposed to grouping by phrases in the original grounding
    """

    group_id: str  # can be out-of-vocab tag, in-vocab name or in-vocab altLabel, converting to tcr will combine name/altLabel under one concept
    phrase_reason_map: PhraseAndReasonMap

    def __hash__(self) -> int:
        return hash(self.group_id)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, TaggingResult):
            return NotImplemented
        return self.__hash__() == other.__hash__()

    def __repr__(self) -> str:
        return f"TaggingResult(group_id={self.group_id}, phrase_reason_map={self.phrase_reason_map})"


class TaggingResultsGroupedByConcept(
    BaseModel
):  # used to group phrases and their og tags(name/alt labels) by only concept names
    concept: Concept
    og_tag_w_phrase_reason_map: TagToPhraseAndReasonMap

    # og_tag may be != concept.name
    # og_tag is group_tag from multiple TaggedResults
    def __hash__(self) -> int:
        return self.concept.__hash__()

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, TaggingResultsGroupedByConcept):
            return NotImplemented
        return self.__hash__() == other.__hash__()


class ConceptExtractionRequestBundle(LLMPhraseExtractionRequestBundle):
    brute: set[str]
    llm_phrase_initial_grounding_req_id: Optional[GPTBatchRequestCustomID]
    llm_phrase_recursive_tagging_reqs: Optional[dict[int, set[IterativeTaggingRequest]]]


ConceptExtractionRequestMap = dict[str, ConceptExtractionRequestBundle]


class DeferredConceptExtractionRequests(BaseModel):
    metadata: ConceptExtractionMetadata
    chunked_request_map: ConceptExtractionRequestMap
