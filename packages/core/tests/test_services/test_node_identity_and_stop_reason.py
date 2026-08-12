"""Node identity and stop_reason (2026-08-12).

Descent-tree nodes used to hash on name alone, so two parents whose descents
both stopped at "None of the above" collapsed into one node per level — the
measured l[4]>Aluminum regression, where the Aluminum descent's verdicts were
in the run log but absent from the trail because the surviving sentinel node
was parented to Steel. Identity is now (parent, name), ordinary concepts are
deduped by the embed walk instead of the hash, and the "-FALSE_CHILD" name
suffix became stop_reason="false_child" so the event is recorded rather than
silently erased.
"""

from requests.structures import CaseInsensitiveDict

from core.models.deferred_extraction.deferred_concept_extraction import (
    IterativeTaggingRequest,
)
from core.models.extraction_schemas.applied_rule import AppliedRule
from core.models.extraction_schemas.grounding import NONE_OF_THE_ABOVE_TAG
from core.models.extraction_schemas.iterative_tagging import (
    IterativelyTaggedPhrase,
    IterativelyTaggedPhraseGroup,
    PhraseTrail,
)
from core.models.pipeline_nodes.multi_stage.base.llm_phrase_iterative_grounding_node import (
    _merge_itrs_into_level,
)
from core.models.skos_concept import Concept
from core.services.pipeline_nodes.multi_stage.llm_recursive_grounding_service import (
    get_deepest_concepts_and_oov,
    get_itr_descendable_concept,
    get_phrase_trails,
)


def _concept(
    name: str,
    level: int,
    children: list[str] | None = None,
    ancestors: list[str] | None = None,
) -> Concept:
    return Concept(
        name=name,
        uri=f"http://asu.edu/semantics/SUDOKN/{name.replace(' ', '')}",
        level=level,
        altLabels=[],
        ancestors=ancestors or [],
        children=children or [],
        definition=f"The {name} concept.",
    )


def _match_label_map(*concepts: Concept) -> CaseInsensitiveDict:
    match_label_to_concept_map: CaseInsensitiveDict = CaseInsensitiveDict()
    for concept in concepts:
        for label in concept.matchLabels:
            match_label_to_concept_map[label] = concept
    return match_label_to_concept_map


def _rules(explanation: str) -> list[AppliedRule]:
    return [
        AppliedRule(rule_id="RGR-Q1", outcome="satisfied", explanation=explanation)
    ]


def _itr(
    name: str,
    *,
    parent_req_id: str | None = None,
    parent_name: str | None = None,
    level: int = 5,
    stop_reason=None,
) -> IterativeTaggingRequest:
    return IterativeTaggingRequest(
        parent_descend_req_id=parent_req_id,
        descend_req_id=f"req>l[{level}]>{name}>p[{parent_name or 'ROOT'}]",
        name=name,
        level=level,
        parent_name=parent_name,
        stop_reason=stop_reason,
    )


METAL = _concept("Metal", level=1, children=["Steel", "Aluminum"])
STEEL = _concept(
    "Steel", level=4, children=["Alloy Steel", "Galvanized Steel"], ancestors=["Metal"]
)
ALUMINUM = _concept("Aluminum", level=4, ancestors=["Metal"])
MMAP = _match_label_map(METAL, STEEL, ALUMINUM)


def test_same_named_sentinel_nodes_under_different_parents_coexist():
    """The l[4]>Aluminum regression at the request-tree layer: with name-only
    identity, one level's set kept a single 'None of the above' node and the
    other parent's verdicts vanished."""
    under_steel = _itr(
        NONE_OF_THE_ABOVE_TAG,
        parent_req_id="steel-req",
        parent_name="Steel",
        stop_reason="sentinel",
    )
    under_aluminum = _itr(
        NONE_OF_THE_ABOVE_TAG,
        parent_req_id="aluminum-req",
        parent_name="Aluminum",
        stop_reason="sentinel",
    )

    assert under_steel != under_aluminum
    assert len({under_steel, under_aluminum}) == 2


def test_identical_parent_and_name_still_dedupe():
    a = _itr(NONE_OF_THE_ABOVE_TAG, parent_req_id="steel-req", parent_name="Steel")
    b = _itr(NONE_OF_THE_ABOVE_TAG, parent_req_id="steel-req", parent_name="Steel")
    assert len({a, b}) == 1


def test_stopped_node_is_not_descendable_even_when_concept_qualifies():
    """A false child IS a real concept with children — descendable by label. The
    node's own stop_reason must override, or the misparented record would be
    descended and its ID expected by the completeness checks."""
    false_child = _itr(
        "Steel", parent_req_id="metal-req", parent_name="Metal",
        stop_reason="false_child",
    )
    assert get_itr_descendable_concept(false_child, MMAP) is None

    ordinary = _itr("Steel", parent_req_id="metal-req", parent_name="Metal")
    assert get_itr_descendable_concept(ordinary, MMAP) is STEEL


def test_merge_dedupes_ordinary_concepts_but_keeps_per_parent_records():
    """One concept, one node per level (the parented candidate wins over the
    direct-tagged one because it comes first); sentinel records stay one per
    parent."""
    level_set: set[IterativeTaggingRequest] = set()
    from_parent = _itr("Steel", parent_req_id="metal-req", parent_name="Metal")
    direct_tagged = _itr("Steel")  # parent None — same concept, dtc-seeded
    sentinel_a = _itr(
        NONE_OF_THE_ABOVE_TAG, parent_req_id="steel-req", parent_name="Steel",
        stop_reason="sentinel",
    )
    sentinel_b = _itr(
        NONE_OF_THE_ABOVE_TAG, parent_req_id="aluminum-req", parent_name="Aluminum",
        stop_reason="sentinel",
    )

    added = _merge_itrs_into_level(
        level_set,
        [from_parent, sentinel_a, sentinel_b, direct_tagged],
        MMAP,
    )

    assert added == [from_parent, sentinel_a, sentinel_b]
    assert level_set == {from_parent, sentinel_a, sentinel_b}


def test_merge_keeps_the_existing_node_across_passes():
    """An earlier pass's node may already carry a dispatched request; a later
    candidate for the same concept must not replace it (its ID differs by the
    parent segment, and the dispatched request would be stranded)."""
    direct_tagged = _itr("Steel")
    level_set = {direct_tagged}
    from_parent = _itr("Steel", parent_req_id="metal-req", parent_name="Metal")

    added = _merge_itrs_into_level(level_set, [from_parent], MMAP)

    assert added == []
    assert level_set == {direct_tagged}


def test_phrase_trail_keeps_both_parents_sentinel_verdicts():
    """The l[4]>Aluminum regression end to end: one phrase reached 'None of the
    above' under Steel AND under Aluminum at the same level; the trail must
    carry both stop records."""
    under_steel = IterativelyTaggedPhraseGroup(
        parent_group_id="Steel",
        group_id=NONE_OF_THE_ABOVE_TAG,
        stop_reason="sentinel",
        direct_phrases_to_og_tag_w_rules={},
        iterative_phrases_to_og_tag_w_rules={
            "Steel & Aluminum": {NONE_OF_THE_ABOVE_TAG: _rules("no specific steel")}
        },
    )
    under_aluminum = IterativelyTaggedPhraseGroup(
        parent_group_id="Aluminum",
        group_id=NONE_OF_THE_ABOVE_TAG,
        stop_reason="sentinel",
        direct_phrases_to_og_tag_w_rules={},
        iterative_phrases_to_og_tag_w_rules={
            "Steel & Aluminum": {NONE_OF_THE_ABOVE_TAG: _rules("no specific aluminum")},
            "High Strength Aluminum": {
                NONE_OF_THE_ABOVE_TAG: _rules("no matching subtype option")
            },
        },
    )

    trails = {t.phrase: t for t in get_phrase_trails({5: {under_steel, under_aluminum}})}

    both = trails["Steel & Aluminum"].lvl_by_lvl_itps[5]
    assert {itp.parent_group_id for itp in both} == {"Steel", "Aluminum"}
    assert all(itp.stop_reason == "sentinel" for itp in both)

    aluminum_only = trails["High Strength Aluminum"].lvl_by_lvl_itps[5]
    assert {itp.parent_group_id for itp in aluminum_only} == {"Aluminum"}


def test_false_child_retains_parent_and_asserts_nothing():
    """The model answered a real concept that is not a child of the parent it
    was asked under. The parent must stand, the named concept must not be
    asserted, and nothing lands in out-of-vocab."""
    phrase_trail = PhraseTrail(
        phrase="we process various metals",
        lvl_by_lvl_itps={
            1: {
                IterativelyTaggedPhrase(
                    parent_group_id=None,
                    group_id="Metal",
                    direct_og_tag_w_rules={"Metal": _rules("names metal")},
                    iterative_og_tag_w_rules={},
                )
            },
            2: {
                IterativelyTaggedPhrase(
                    parent_group_id="Metal",
                    group_id="Steel",
                    stop_reason="false_child",
                    direct_og_tag_w_rules={},
                    iterative_og_tag_w_rules={
                        "Steel": _rules("named under the wrong parent")
                    },
                )
            },
        },
    )

    concepts, oov = get_deepest_concepts_and_oov(
        phrase_trail=phrase_trail,
        match_label_to_concept_map=MMAP,
    )

    assert concepts == {METAL}
    assert oov == set()
