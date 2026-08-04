from __future__ import annotations

from packages.core.src.core.models.extraction_schemas.relationship import (
    LLMPhraseRelationshipResults,
)

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
