from core.services.pipeline_nodes.multi_stage.llm_phrase_relationship_node_service import (
    _split_into_phrase_groups,
)


def test_no_candidates_still_yields_one_group():
    """The empty group is what the dummy-request path keys off: one request per
    chunk, still, when search found nothing."""
    assert _split_into_phrase_groups([], 50) == [[]]


def test_group_count_matches_ceil_division():
    candidates = [f"phrase {i}" for i in range(120)]
    groups = _split_into_phrase_groups(candidates, 50)
    assert [len(group) for group in groups] == [50, 50, 20]


def test_exact_multiple_produces_no_trailing_empty_group():
    """A trailing empty group would trip the create-side guard that only the
    single-group case may be empty."""
    candidates = [f"phrase {i}" for i in range(100)]
    assert [len(group) for group in _split_into_phrase_groups(candidates, 50)] == [
        50,
        50,
    ]


def test_groups_partition_the_candidates_in_order():
    """Order matters twice over: it decides which phrases share a request, and the
    node embeds request ids by group index before the groups themselves exist."""
    candidates = [f"phrase {i}" for i in range(7)]
    groups = _split_into_phrase_groups(candidates, 3)
    assert [phrase for group in groups for phrase in group] == candidates
