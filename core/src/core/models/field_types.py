from __future__ import annotations

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

# Stage 4 and 5
TagAndReasonMap = dict[str, str]

PhraseToTagAndReasonMap = dict[
    str, TagAndReasonMap  # { phrase -> {tag: reason}, ...}
]  # The tags phrases can be linked to, with the reason for the link.


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
