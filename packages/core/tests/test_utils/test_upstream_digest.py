"""Fork F12: upstream content joins request identity via a canonical digest."""

from core.utils.request_custom_id_util import (
    UPSTREAM_DIGEST_LENGTH,
    upstream_content_digest,
    upstream_digest_segment,
)


def test_digest_is_stable_across_dict_ordering():
    a = {"r1": {"synthesis": "s", "mentions": []}, "r2": {"mentions": [], "synthesis": "t"}}
    b = {"r2": {"synthesis": "t", "mentions": []}, "r1": {"mentions": [], "synthesis": "s"}}
    assert upstream_content_digest(a) == upstream_content_digest(b)


def test_content_changes_change_the_digest():
    base = {"r1": {"candidates": ["Aerospace Industry"]}}
    changed = {"r1": {"candidates": ["Aerospace Industry", "Machining"]}}
    assert upstream_content_digest(base) != upstream_content_digest(changed)


def test_segment_shape():
    segment = upstream_digest_segment({"r1": {}})
    assert segment.startswith("|ud=")
    assert len(segment) == len("|ud=") + UPSTREAM_DIGEST_LENGTH
