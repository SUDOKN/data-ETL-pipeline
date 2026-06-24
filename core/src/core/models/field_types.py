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

LLMDistillationResults = dict[str, str]
"""
Example:
{
    "Reciprocating Surface Grinders": "Yes, the text lists 'Reciprocating Surface Grinders' as a type of equipment available, implying capability for surface grinding processes.",
    "Arbor Presses": "Yes, 'Arbor Presses' are listed under types of equipment, indicating capability for pressing operations.",
    "Pallet Jacks": "No, 'Pallet Jacks' are listed as equipment but they are material handling devices, not manufacturing process equipment."
    ...
}

Each evidence starts with "Yes" or "No" indicating whether the identified term is supported by the text, followed by a brief explanation referencing the text.
"""
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

HumanVerificationResults = LLMDistillationResults  # evidence for each identified term, where key is the term and value is the evidence text human must provide for the term

MfgETLDType = str
MfgURLType = str
OntologyVersionIDType = str
S3FileVersionIDType = str
