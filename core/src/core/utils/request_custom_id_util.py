def get_name_from_recursive_grounding_request_custom_id(req_custom_id: str) -> str:
    return req_custom_id.split(">l[")[1].split("]")[1].split(">")[1]


def get_level_from_recursive_request_custom_id(req_custom_id: str) -> int:
    # TODO
    return int(req_custom_id.split(">l[")[1].split("]")[1].split(">")[1])
