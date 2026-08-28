"""Descent declinations are recorded (2026-08-27).

The recursive descent offers the same escape hatch every grounding stage does
— an empty ``options`` array with an explanation — and until now the parser
built that explanation and dropped it one line later, so a node whose records
all declined was indistinguishable from a node nobody asked about. On run
20260825T194457 that was 165 of 216 descent requests (76%).

These cover the three places the declination now has to survive: the split out
of a parsed response, the trail node it lands on, and the dump lint that turns
what is left over — a node that WAS asked and answered neither way — into a
visible defect instead of another silence.
"""

from core.models.extraction_schemas.grounding import (
    PhraseToTagAndRulesMap,
    RecordGroundingEntry,
    StopReason,
)
from core.models.extraction_schemas.iterative_tagging import (
    IterativelyTaggedPhrase,
    IterativelyTaggedPhraseGroup,
)
from core.models.extraction_schemas.applied_rule import AppliedRule
from core.services.pipeline_nodes.multi_stage.llm_recursive_grounding_service import (
    get_descent_answer_from_record_groundings,
    get_phrase_trails,
    get_tagging_results_from_record_groundings,
)
from core.utils.extraction_dump_util import _mark_unexplained_stops

_RULE = AppliedRule(rule_id="RGR-E1", outcome="satisfied", explanation="e")


# ---------------------------------------------------------------------------
# the split
# ---------------------------------------------------------------------------


def test_a_response_splits_into_children_and_declinations():
    answer = get_descent_answer_from_record_groundings(
        {
            "r_child": RecordGroundingEntry(tags={"Steel": [_RULE]}),
            "r_declined": RecordGroundingEntry(
                tags={}, explanation="only a stock shape; no grade evidenced"
            ),
        }
    )
    assert [tr.group_id for tr in answer.tagging_results] == ["Steel"]
    assert answer.tagging_results[0].phrase_rules_map == {"r_child": [_RULE]}
    assert answer.declined_records == {
        "r_declined": "only a stock shape; no grade evidenced"
    }


def test_a_record_lands_on_exactly_one_side():
    """The stored entry's validator already guarantees tags XOR explanation;
    the split must not double-count or drop anyone."""
    groundings = {
        "r1": RecordGroundingEntry(tags={"Steel": [_RULE]}),
        "r2": RecordGroundingEntry(tags={"Aluminum": [_RULE]}),
        "r3": RecordGroundingEntry(tags={}, explanation="nothing more specific"),
    }
    answer = get_descent_answer_from_record_groundings(groundings)
    answered = {
        record_id
        for tr in answer.tagging_results
        for record_id in tr.phrase_rules_map
    } | set(answer.declined_records)
    assert answered == set(groundings)


def test_the_children_only_helper_still_answers_the_seed_path():
    groundings = {
        "r1": RecordGroundingEntry(tags={"Steel": [_RULE]}),
        "r2": RecordGroundingEntry(tags={}, explanation="declined"),
    }
    assert [
        tr.group_id for tr in get_tagging_results_from_record_groundings(groundings)
    ] == ["Steel"]


def test_an_all_declined_response_yields_no_children_and_every_reason():
    answer = get_descent_answer_from_record_groundings(
        {
            "r1": RecordGroundingEntry(tags={}, explanation="why one"),
            "r2": RecordGroundingEntry(tags={}, explanation="why two"),
        }
    )
    assert answer.tagging_results == []
    assert answer.declined_records == {"r1": "why one", "r2": "why two"}


# ---------------------------------------------------------------------------
# the trail
# ---------------------------------------------------------------------------


def _group(
    *,
    group_id: str = "Metal",
    parent_group_id: str | None = None,
    stop_reason: StopReason | None = None,
    declined_records: dict[str, str] | None = None,
    direct_phrases_to_og_tag_w_rules: PhraseToTagAndRulesMap | None = None,
    iterative_phrases_to_og_tag_w_rules: PhraseToTagAndRulesMap | None = None,
) -> IterativelyTaggedPhraseGroup:
    return IterativelyTaggedPhraseGroup(
        parent_group_id=parent_group_id,
        group_id=group_id,
        stop_reason=stop_reason,
        declined_records=declined_records or {},
        direct_phrases_to_og_tag_w_rules=direct_phrases_to_og_tag_w_rules or {},
        iterative_phrases_to_og_tag_w_rules=iterative_phrases_to_og_tag_w_rules or {},
    )


def test_a_declination_lands_on_the_record_s_own_trail_node():
    trails = get_phrase_trails(
        {
            1: {
                _group(
                    direct_phrases_to_og_tag_w_rules={"r1": {"Metal": [_RULE]}},
                    declined_records={"r1": "no specific metal evidenced"},
                )
            }
        }
    )
    (trail,) = trails
    assert trail.phrase == "r1"
    (node,) = trail.lvl_by_lvl_itps[1]
    assert node.group_id == "Metal"
    assert node.declined == "no specific metal evidenced"
    # and the node keeps the evidence that put the record there
    assert node.direct_og_tag_w_rules == {"Metal": [_RULE]}


def test_only_the_declining_record_is_marked():
    trails = get_phrase_trails(
        {
            1: {
                _group(
                    direct_phrases_to_og_tag_w_rules={
                        "r1": {"Metal": [_RULE]},
                        "r2": {"Metal": [_RULE]},
                    },
                    declined_records={"r1": "nothing more specific"},
                )
            }
        }
    )
    declined_by_record = {
        trail.phrase: next(iter(trail.lvl_by_lvl_itps[1])).declined for trail in trails
    }
    assert declined_by_record == {"r1": "nothing more specific", "r2": None}


def test_a_declination_never_invents_a_trail_node(caplog):
    """A group_id put into a record's trail on the strength of a declination
    would read as a finding to `get_deepest_concepts_and_oov`."""
    trails = get_phrase_trails(
        {1: {_group(declined_records={"r_unknown": "declined"})}}
    )
    assert trails == []
    assert "has no trail node there" in caplog.text


def test_a_node_with_no_declinations_is_unchanged():
    trails = get_phrase_trails(
        {1: {_group(direct_phrases_to_og_tag_w_rules={"r1": {"Metal": [_RULE]}})}}
    )
    (node,) = next(iter(trails)).lvl_by_lvl_itps[1]
    assert node.declined is None


def test_declinations_and_stop_reason_are_independent_fields():
    """Two granularities: the node's own stop, and a per-record one."""
    group = _group(
        stop_reason="false_child",
        direct_phrases_to_og_tag_w_rules={"r1": {"Metal": [_RULE]}},
        declined_records={"r1": "and this record declined too"},
    )
    assert group.stop_reason == "false_child"
    assert group.declined_records == {"r1": "and this record declined too"}
    reloaded = IterativelyTaggedPhraseGroup.model_validate_json(
        group.model_dump_json()
    )
    assert reloaded.declined_records == group.declined_records
    assert reloaded.stop_reason == "false_child"


def test_the_phrase_twin_round_trips_its_declination():
    node = IterativelyTaggedPhrase(
        parent_group_id="Metal",
        group_id="Steel",
        declined="no grade evidenced",
        direct_og_tag_w_rules={},
        iterative_og_tag_w_rules={},
    )
    reloaded = IterativelyTaggedPhrase.model_validate_json(node.model_dump_json())
    assert reloaded.declined == "no grade evidenced"


# ---------------------------------------------------------------------------
# the dump lint: what is left over after all three legitimate explanations
# ---------------------------------------------------------------------------


def _node(group_id: str, parent: str | None = None, **kwargs) -> dict:
    node = {
        "group_id": group_id,
        "parent_group_id": parent,
        "stop_reason": None,
        "declined": None,
        "descendable": True,
    }
    node.update(kwargs)
    return node


def test_a_descendable_node_that_stopped_for_no_reason_is_flagged():
    levels = {1: [_node("Metal")]}
    _mark_unexplained_stops(levels)
    assert levels[1][0]["unexplained_stop"] is True


def test_a_node_with_children_is_not_flagged():
    levels = {1: [_node("Metal")], 2: [_node("Steel", "Metal")]}
    _mark_unexplained_stops(levels)
    assert "unexplained_stop" not in levels[1][0]


def test_a_declined_node_is_not_flagged():
    levels = {1: [_node("Metal", declined="nothing more specific")]}
    _mark_unexplained_stops(levels)
    assert "unexplained_stop" not in levels[1][0]


def test_a_stopped_node_is_not_flagged():
    levels = {1: [_node("Metal", stop_reason="false_child")]}
    _mark_unexplained_stops(levels)
    assert "unexplained_stop" not in levels[1][0]


def test_a_leaf_is_not_flagged():
    """A leaf renders an empty option list, is never descended, and owes no
    reason for having no children."""
    levels = {1: [_node("Tungsten", descendable=False)]}
    _mark_unexplained_stops(levels)
    assert "unexplained_stop" not in levels[1][0]


def test_the_deepest_level_is_still_judged():
    """Nothing sits below the last level, so every descendable node there
    stands or falls on its own recorded reason."""
    levels = {
        1: [_node("Metal")],
        2: [_node("Steel", "Metal"), _node("Iron", "Metal", declined="d")],
    }
    _mark_unexplained_stops(levels)
    assert "unexplained_stop" not in levels[1][0]
    flagged = {n["group_id"]: n.get("unexplained_stop") for n in levels[2]}
    assert flagged == {"Steel": True, "Iron": None}
