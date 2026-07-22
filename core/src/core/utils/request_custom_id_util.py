"""
Example:

alecmfg.com>material_caps>llm_phrase_recursive_grounding>chunk>0:24294>l[2]>Composite>gpt-4.1|max_completion_tokens=10000|temperature=0.0|seed=12345
"""


def get_name_from_recursive_grounding_request_custom_id(req_custom_id: str) -> str:
    return req_custom_id.split(">l[")[1].split("]")[1].split(">")[1]


def get_level_from_recursive_request_custom_id(req_custom_id: str) -> int:
    return int(req_custom_id.split(">l[")[1].split("]")[0])
