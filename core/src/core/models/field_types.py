from __future__ import annotations
from pydantic import BaseModel
from open_ai_key_app.models.field_types import GPTBatchRequestCustomID

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
LLMGroundingResults = dict[
    str, dict[str, str]  # { phrase -> {concept: reason}, ...}
]  # The immediate nodes phrases can be linked to, with the reason for the link.


# Stage 5
class MatchedLabelAndReasonPair(BaseModel):
    matched_label: str
    reason: str


class RecursivelyTaggedConceptNode(BaseModel):
    name: str  # can be in-vocab concept.name, out-of-vocab label or some semantic variant of "None of the above"
    level: int
    descend_req_id: GPTBatchRequestCustomID

    directly_tagged_phrase_reason_map: dict[str, MatchedLabelAndReasonPair]
    # { phrase -> (matched_concept_label, reason) }
    # produced by initial grounding results

    iteratively_tagged_phrase_reason_map: dict[str, MatchedLabelAndReasonPair]
    # { phrase -> list[reason] }
    # produced by iterative traversal of a phrase from its initially grounded concept
    # CAUTION: may include a phrase which was already directly tagged to this concept initially
    # and that too multiple times, hence list of reasons

    children: list[RecursivelyTaggedConceptNode]


LLMRecursiveGroundingResults = list[RecursivelyTaggedConceptNode]

# RawLLMMappingResult = dict[str, dict[str, str]]
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
