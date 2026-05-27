HumanEvidenceResults = dict[
    str, str
]  # evidence for each identified term, where key is the term and value is the evidence text human must provide for the term
LLMEvidenceResults = dict[
    str, str
]  # evidence for each identified term, where key is the term and value is the evidence text which can be None if the LLM found no evidence
LLMMappingType = dict[str, set[str]]  # mapping from known concept to unknown concepts
LLMMappingResult = dict[str, list[str]]
MfgETLDType = str
MfgURLType = str
OntologyVersionIDType = str
S3FileVersionIDType = str
