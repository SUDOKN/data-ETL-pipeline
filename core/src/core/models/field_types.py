LLMEvidenceResults = dict[
    str, str
]  # evidence for each identified term, where key is the term and value is the evidence text which can be None if the LLM found no evidence
HumanEvidenceResults = LLMEvidenceResults  # evidence for each identified term, where key is the term and value is the evidence text human must provide for the term

# LLMMappingType = dict[str, set[str]]  # mapping from known concept to unknown concepts
# LLMMappingResult = dict[
#     str, list[str]
# ]  # json mapping result from LLM which is then converted to LLMMappingType after validation and parsing

RawLLMMappingResult = dict[
    str, dict[str, str]
]  # { unknown --> {"mapped_known_1": "matching reason"}, {"mapped_known_2": "matching reason"} } where evidence is the text snippet from which LLM derived the mapping
LLMMappingType = RawLLMMappingResult  # inverted mapping (that's actually stored in a manufacturer) from known concept to unknown concepts but same type, # { known --> {"mapped_unknown_1": "matching reason"}, {"mapped_unknown_2": "matching reason"} } where evidence is the text snippet from which LLM derived the mapping

MfgETLDType = str
MfgURLType = str
OntologyVersionIDType = str
S3FileVersionIDType = str
