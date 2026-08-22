"""
Example:

alecmfg.com>material_caps>llm_phrase_recursive_grounding>chunk>0:24294>l[2]>Composite>gpt-4.1|max_completion_tokens=10000|temperature=0.0|seed=12345
"""


import hashlib
import json
from typing import Any


def get_name_from_recursive_grounding_request_custom_id(req_custom_id: str) -> str:
    return req_custom_id.split(">l[")[1].split("]")[1].split(">")[1]


def get_level_from_recursive_request_custom_id(req_custom_id: str) -> int:
    return int(req_custom_id.split(">l[")[1].split("]")[0])


# --- v2 (pipeline v2, fork F12): the upstream-content digest -----------------
#
# A downstream request's custom_id has always named the model, params, prompt
# version and group cap — everything that decides what the request contains,
# EXCEPT the upstream content it was built from. So a re-run after an upstream
# change found the old downstream requests "complete" and replayed responses
# produced against inputs that no longer exist (the stale-replay hazard,
# recorded 2026-08-13). Appending `|ud=<digest of the group's own input>` makes
# upstream content part of request identity: change what a group would be
# asked, and its id changes, and the stale response is simply never found.
#
# The digest is of the PAYLOAD the group renders (records, candidates, options
# membership rides via prompt/catalog pins already), serialized canonically so
# dict ordering can never perturb identity.

UPSTREAM_DIGEST_LENGTH = 12


def upstream_content_digest(payload: Any) -> str:
    """A short stable digest of a request group's upstream-derived input."""
    canonical = json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[
        :UPSTREAM_DIGEST_LENGTH
    ]


def upstream_digest_segment(payload: Any) -> str:
    """The custom_id segment carrying the digest, e.g. ``|ud=3f9a1c2b4d5e``."""
    return f"|ud={upstream_content_digest(payload)}"
