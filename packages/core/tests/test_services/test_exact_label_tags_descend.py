"""Every recognized initial tag descends — including exact label matches.

Replaces test_settled_initial_tags.py. The exact-label settle policy (a phrase
equal to its concept's own label was pruned from descent and the concept
diverted to reconcile through a "settled" channel) is removed as of
2026-08-12: descent evidence lives in the phrase's relationship summary, which
an exact phrase-label match says nothing about, and the pre-policy baselines
showed exact-match descents finding real deeper tags ('Automotive' ->
Automotive Components, 'Assembly' -> Mechanical Joining).

The regression these tests guard is the original concept-drop bug wearing a
new mechanism: a concept whose every phrase exactly matches its own labels
must still reach the results. It used to get there through the settled
channel; now it must be descend-worthy, so the trail carries it.
"""

from requests.structures import CaseInsensitiveDict

from core.models.deferred_extraction.deferred_concept_extraction import TaggingResult
from core.models.extraction_schemas.applied_rule import AppliedRule
from core.models.skos_concept import Concept
from core.services.pipeline_nodes.multi_stage.llm_initial_grounding_service import (
    get_descend_worthy_tcs_from_tagged_results,
    get_oov_tags_from_trs,
)


def _concept(name: str, level: int, altLabels: list[str] | None = None) -> Concept:
    return Concept(
        name=name,
        uri=f"http://asu.edu/semantics/SUDOKN/{name.replace(' ', '')}",
        level=level,
        altLabels=altLabels or [],
        ancestors=[],
        children=[],
        definition=f"The {name} concept.",
    )


def _rules(explanation: str) -> list[AppliedRule]:
    return [
        AppliedRule(rule_id="IGR-Q1", outcome="satisfied", explanation=explanation)
    ]


def _match_label_map(*concepts: Concept) -> CaseInsensitiveDict:
    match_label_to_concept_map: CaseInsensitiveDict = CaseInsensitiveDict()
    for concept in concepts:
        for label in concept.matchLabels:
            match_label_to_concept_map[label] = concept
    return match_label_to_concept_map


def test_exact_label_phrase_is_descend_worthy():
    """The Automotive case: a brute-seeded phrase equal to the concept's own
    label descends like any other tag, phrase intact — this is the path that
    now keeps the concept out of the drop the settled channel used to catch."""
    automotive = _concept("Automotive", level=1)
    trs = [
        TaggingResult(
            group_id="Automotive",
            phrase_rules_map={"Automotive": _rules("brute-seeded exact match")},
        )
    ]

    descend_worthy = get_descend_worthy_tcs_from_tagged_results(
        initially_tagged_trs=trs,
        match_label_to_concept_map=_match_label_map(automotive),
    )

    assert [tc.concept for tc in descend_worthy] == [automotive]
    assert descend_worthy[0].og_tag_w_phrase_rules_map == {
        "Automotive": {"Automotive": _rules("brute-seeded exact match")}
    }


def test_alt_label_phrase_is_descend_worthy_too():
    """matchLabels covers altLabels: the 'Assembly'->Joining shape must descend
    (pre-policy baselines found Mechanical Joining under it every time)."""
    tool_and_die = _concept("Tool and Die", level=1, altLabels=["Tool & Die"])
    trs = [
        TaggingResult(
            group_id="Tool and Die",
            phrase_rules_map={"Tool & Die": _rules("alt label match")},
        )
    ]

    descend_worthy = get_descend_worthy_tcs_from_tagged_results(
        initially_tagged_trs=trs,
        match_label_to_concept_map=_match_label_map(tool_and_die),
    )

    assert [tc.concept for tc in descend_worthy] == [tool_and_die]


def test_casing_variants_all_descend_alike():
    """Whether a concept descends must not depend on the model's casing that
    call — under the old prune 'household products' escaped while 'Household
    Products' settled."""
    household = _concept("Household Products", level=2)

    for phrase in ("household products", "Household Products", " Household Products "):
        trs = [
            TaggingResult(
                group_id="Household Products",
                phrase_rules_map={phrase: _rules("casing/whitespace variant")},
            )
        ]

        descend_worthy = get_descend_worthy_tcs_from_tagged_results(
            initially_tagged_trs=trs,
            match_label_to_concept_map=_match_label_map(household),
        )

        assert [tc.concept for tc in descend_worthy] == [household], (
            f"{phrase!r} did not make the concept descend-worthy"
        )


def test_phrase_maps_pass_through_unpruned():
    """Mixed map: the exact-label phrase and the free phrase both survive into
    the descent — the descent prompt sees every phrase with its summary."""
    household = _concept("Household Products", level=2)
    trs = [
        TaggingResult(
            group_id="Household Products",
            phrase_rules_map={
                "household products": _rules("casing variant of the label"),
                "kitchen appliance housings": _rules("free phrase"),
            },
        )
    ]

    descend_worthy = get_descend_worthy_tcs_from_tagged_results(
        initially_tagged_trs=trs,
        match_label_to_concept_map=_match_label_map(household),
    )

    assert descend_worthy[0].og_tag_w_phrase_rules_map == {
        "Household Products": {
            "household products": _rules("casing variant of the label"),
            "kitchen appliance housings": _rules("free phrase"),
        }
    }
    assert set(trs[0].phrase_rules_map) == {
        "household products",
        "kitchen appliance housings",
    }


def test_out_of_vocab_tag_is_reported_as_oov_not_descend_worthy():
    trs = [
        TaggingResult(
            group_id="Flexible Manufacturing",
            phrase_rules_map={"flexible manufacturing": _rules("proposed option")},
        )
    ]

    assert (
        get_descend_worthy_tcs_from_tagged_results(
            initially_tagged_trs=trs,
            match_label_to_concept_map=_match_label_map(),
        )
        == []
    )
    assert get_oov_tags_from_trs(
        initially_tagged_trs=trs,
        match_label_to_concept_map=_match_label_map(),
    ) == {"Flexible Manufacturing"}


def test_sentinel_tag_is_neither_descend_worthy_nor_oov():
    """The sentinel records that the model declined to match; it must not leak
    into descent or the persisted out-of-vocab labels."""
    trs = [
        TaggingResult(
            group_id="None of the above",
            phrase_rules_map={"unmatched phrase": _rules("sentinel verdict")},
        )
    ]

    assert (
        get_descend_worthy_tcs_from_tagged_results(
            initially_tagged_trs=trs,
            match_label_to_concept_map=_match_label_map(),
        )
        == []
    )
    assert (
        get_oov_tags_from_trs(
            initially_tagged_trs=trs,
            match_label_to_concept_map=_match_label_map(),
        )
        == set()
    )
