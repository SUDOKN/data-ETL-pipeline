from __future__ import annotations
from pydantic import BaseModel
from typing import Optional

# Stage 1
LLMSearchResults = set[str]
"""
Example:
[
    "Reciprocating Surface Grinders",
    "Rotary, Trunnion (Horizontal & Vertical) Transfer Machines",
    "CNC Lathes",
    "Arbor Presses"
]
"""

# Stage 2
LLMPhraseRelationshipResults = dict[
    str, str
]  # Describe relationship between phrases extracted and the subject, the manufacturer.

# Stage 3
LLMScreeningResults = dict[str, str]  # To only keep the relationships we care about

# Stage 4
TagAndReasonMap = dict[str, str]
PhraseToTagAndReasonMap = dict[
    str, TagAndReasonMap  # { phrase -> {tag: reason}, ...}
]  # The tags phrases can be linked to, with the reason for the link.


# Stage 5
class IterativelyTaggedPhraseGroup(BaseModel):
    parent_group_id: Optional[str]
    group_id: str  # MUST BE EITHER IN-VOCAB CONCEPT **NAME** or OUT-OF-VOCAB TAG, no altLabel allowed

    # Provision for the case where tag used is an alt label
    # by explicitly storing original tag as well
    direct_phrases_to_og_tag_w_reason: PhraseToTagAndReasonMap
    # { phrase -> (og tag, reason) } because og tag can be an alt label
    # produced by initial grounding results

    iterative_phrases_to_og_tag_w_reason: PhraseToTagAndReasonMap
    # { phrase -> (og tag, reason) }
    # produced by iterative traversal of a phrase from its initially grounded concept

    # the tag inside each PhraseToTagAndReasonMap
    # will either be name/altLabel of in-vocab concept or out-of-vocab term
    # depending on the tag_id being in-vocab or not
    # but that's what each PhraseToTagAndReasonMap will have in common with
    # the tag_id

    def __hash__(self) -> int:
        return hash(self.group_id)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, IterativelyTaggedPhraseGroup):
            return NotImplemented
        return self.__hash__() == other.__hash__()


IterativeGroundingResult = dict[int, set[IterativelyTaggedPhraseGroup]]


# Phrase Trail
class PhraseTrail(BaseModel):
    phrase: str
    lvl_by_lvl_itps: dict[int, set[IterativelyTaggedPhrase]]
    # can start at different levels,
    # and from different group tags on the same level, hence list


class IterativelyTaggedPhrase(BaseModel):
    parent_group_id: Optional[str]
    group_id: str
    direct_og_tag_w_reason: TagAndReasonMap
    iterative_og_tag_w_reason: TagAndReasonMap

    def __hash__(self) -> int:
        return hash(self.group_id)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, IterativelyTaggedPhrase):
            return NotImplemented
        return self.__hash__() == other.__hash__()


# Others
PhraseAndReasonMap = dict[str, str]
TagToPhraseAndReasonMap = dict[str, PhraseAndReasonMap]


RawLLMMappingResult = dict[str, dict[str, str]]
"""
{ unknown --> {"mapped_known_1": "matching reason"}, {"mapped_known_2": "matching reason"} } where evidence is the text snippet from which LLM derived the mapping
Example:
{
    "Reciprocating Surface Grinders": {
        "Surface Grinding": "Correct, The phrase 'Reciprocating Surface Grinders' directly implies the capability for surface grinding processes, which matches the known process 'Surface Grinding' under 'Machining' and 'Abrasive Machining'."
    },
    "Arbor Presses": {
        "Press Work": "Correct, Arbor presses are equipment used for pressing operations, which aligns with 'Press Work' under 'Sheet Metal Processing' and 'Stamping'."
    },
    ...
}
"""

HumanVerificationResults = LLMPhraseRelationshipResults  # evidence for each identified term, where key is the term and value is the evidence text human must provide for the term

MfgETLDType = str
MfgURLType = str
OntologyVersionIDType = str
S3FileVersionIDType = str
