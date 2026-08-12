from __future__ import annotations
from pydantic import BaseModel, Field
from typing import Optional
import logging

from core.models.extraction_schemas.grounding import (
    PhraseToAppliedRulesMap,
    StopReason,
    TagToPhraseAndRulesMap,
)
from core.models.extraction_results.concept_extraction_results import (
    ConceptExtractionMetadata,
)
from core.models.deferred_extraction.deferred_phrase_extraction_requests import (
    DeferredLLMPhraseExtractionRequests,
    LLMPhraseExtractionRequestBundle,
)
from core.models.skos_concept import Concept

from llm_providers.field_types import BatchRequestIDType

logger = logging.getLogger(__name__)


class IterativeTaggingRequest(BaseModel):
    """A node in the per-chunk descent tree.

    ``name``/``level``/``parent_name`` are explicit fields rather than parsed
    positionally out of ``descend_req_id`` — the ID stays the request's address,
    not its data. A node exists for anything a grounding stage tagged (in-vocab
    concept, out-of-vocab proposal, sentinel, false child) so its evidence can
    reach the phrase trail; only descendable nodes (see
    ``get_itr_descendable_concept``) ever get a request created for
    ``descend_req_id``.

    Identity is ``(parent_descend_req_id, name)``: two parents whose responses
    both stop at "None of the above" (or both propose the same out-of-vocab
    label) are two distinct records — a name-only hash collapsed them per level
    and silently discarded one parent's verdicts. Real concepts still appear
    once per level: the embed walk dedupes descendable nodes by concept name at
    creation time, because a concept's identity is global, its phrase evidence
    pools across parents, and one descent per concept per chunk is asserted
    downstream.
    """

    parent_descend_req_id: Optional[BatchRequestIDType]
    descend_req_id: BatchRequestIDType
    name: str
    level: int
    parent_name: Optional[str] = None
    stop_reason: Optional[StopReason] = None

    def __hash__(self) -> int:
        return hash((self.parent_descend_req_id, self.name))

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, IterativeTaggingRequest):
            return NotImplemented
        return (self.parent_descend_req_id, self.name) == (
            other.parent_descend_req_id,
            other.name,
        )

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
    phrase_rules_map: PhraseToAppliedRulesMap

    def __hash__(self) -> int:
        return hash(self.group_id)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, TaggingResult):
            return NotImplemented
        return self.__hash__() == other.__hash__()

    def __repr__(self) -> str:
        return f"TaggingResult(group_id={self.group_id}, phrase_rules_map={self.phrase_rules_map})"


class TaggingResultsGroupedByConcept(
    BaseModel
):  # used to group phrases and their og tags(name/alt labels) by only concept names
    concept: Concept
    og_tag_w_phrase_rules_map: TagToPhraseAndRulesMap

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
    # Ordered list of initial-grounding groups (group 1 == index 0). Each group
    # covers at most `max_pairs_per_request` screened out-of-vocab phrases for
    # this chunk; results are merged back together.
    llm_phrase_initial_grounding_req_ids: list[BatchRequestIDType] = Field(
        default_factory=list
    )
    llm_phrase_recursive_tagging_reqs: Optional[dict[int, set[IterativeTaggingRequest]]]


ConceptExtractionRequestMap = dict[str, ConceptExtractionRequestBundle]


class DeferredConceptExtractionRequests(DeferredLLMPhraseExtractionRequests):
    metadata: ConceptExtractionMetadata
    chunked_request_map: ConceptExtractionRequestMap
