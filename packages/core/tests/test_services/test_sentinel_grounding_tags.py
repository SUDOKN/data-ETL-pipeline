from requests.structures import CaseInsensitiveDict

from core.models.extraction_schemas.applied_rule import AppliedRule
from core.models.extraction_schemas.grounding import (
    NONE_OF_THE_ABOVE_TAG,
    is_sentinel_grounding_label,
)
from core.models.extraction_schemas.iterative_tagging import (
    IterativelyTaggedPhrase,
    PhraseTrail,
)
from core.models.deferred_extraction.deferred_concept_extraction import TaggingResult
from core.models.skos_concept import Concept
from core.services.pipeline_nodes.multi_stage.llm_initial_grounding_service import (
    get_tcs_and_oov_trs_from_trs,
)
from core.services.pipeline_nodes.multi_stage.llm_recursive_grounding_service import (
    get_deepest_concepts_and_oov,
)


def _concept(name: str, level: int, ancestors: list[str]) -> Concept:
    return Concept(
        name=name,
        uri=f"http://asu.edu/semantics/SUDOKN/{name.replace(' ', '')}",
        level=level,
        altLabels=[],
        ancestors=ancestors,
        children=[],
        definition=f"The {name} concept.",
    )


def _rules(explanation: str) -> list[AppliedRule]:
    """Stands in for parsed output. One type serves both the wire and the stored
    side, so a fixture is the same shape either way."""
    return [
        AppliedRule(rule_id="RGR-Q1", outcome="satisfied", explanation=explanation)
    ]


def _match_label_map(*concepts: Concept) -> CaseInsensitiveDict:
    match_label_to_concept_map: CaseInsensitiveDict = CaseInsensitiveDict()
    for concept in concepts:
        for label in concept.matchLabels:
            match_label_to_concept_map[label] = concept
    return match_label_to_concept_map


def test_initial_grounding_sentinel_is_not_an_out_of_vocab_discovery():
    """Initial grounding gained an escape hatch (2026-08-11), so "None of the
    above" now reaches this split. It matches no concept by construction, and
    without a filter every declined phrase would land in ``oov_trs`` and persist
    as a discovered label — the same leak already closed on the recursive and
    freehand paths."""
    metal = _concept("Metal", level=1, ancestors=[])

    oov_trs, tcs = get_tcs_and_oov_trs_from_trs(
        trs=[
            TaggingResult(
                group_id="Metal",
                phrase_rules_map={"we work with metals": _rules("names metal")},
            ),
            TaggingResult(
                group_id=NONE_OF_THE_ABOVE_TAG,
                phrase_rules_map={
                    "perform inspections": _rules("not a process of this kind")
                },
            ),
        ],
        match_label_to_concept_map=_match_label_map(metal),
    )

    assert oov_trs == []
    assert [tc.concept for tc in tcs] == [metal]


def test_initial_grounding_still_reports_genuine_out_of_vocab_tags():
    """The filter must catch the sentinel only — a real label the ontology does
    not carry is a discovery and has to survive."""
    metal = _concept("Metal", level=1, ancestors=[])

    oov_trs, tcs = get_tcs_and_oov_trs_from_trs(
        trs=[
            TaggingResult(
                group_id="Inconel",
                phrase_rules_map={"we machine inconel": _rules("names inconel")},
            )
        ],
        match_label_to_concept_map=_match_label_map(metal),
    )

    assert [tr.group_id for tr in oov_trs] == ["Inconel"]
    assert tcs == []


def test_is_sentinel_grounding_label_ignores_case_and_surrounding_whitespace():
    assert is_sentinel_grounding_label(NONE_OF_THE_ABOVE_TAG)
    assert is_sentinel_grounding_label("none of the above")
    assert is_sentinel_grounding_label("  None Of The Above  ")
    assert not is_sentinel_grounding_label("Metal")
    assert not is_sentinel_grounding_label("None of the above alloys")


def test_sentinel_tag_is_neither_a_concept_nor_an_out_of_vocab_label():
    """A "None of the above" tag records that the descent stopped, so it must not
    be reported as a discovered out-of-vocabulary label."""
    metal = _concept("Metal", level=1, ancestors=[])
    phrase_trail = PhraseTrail(
        phrase="we work with a range of metals",
        lvl_by_lvl_itps={
            1: {
                IterativelyTaggedPhrase(
                    parent_group_id=None,
                    group_id="Metal",
                    direct_og_tag_w_rules={"Metal": _rules("names metal directly")},
                    iterative_og_tag_w_rules={},
                )
            },
            2: {
                IterativelyTaggedPhrase(
                    parent_group_id="Metal",
                    group_id=NONE_OF_THE_ABOVE_TAG,
                    direct_og_tag_w_rules={},
                    iterative_og_tag_w_rules={
                        NONE_OF_THE_ABOVE_TAG: _rules("no specific metal is named")
                    },
                )
            },
        },
    )

    concepts, oov = get_deepest_concepts_and_oov(
        phrase_trail=phrase_trail,
        match_label_to_concept_map=_match_label_map(metal),
    )

    assert oov == set()
    # The parent survives: "nothing more specific qualifies" means Metal stands.
    assert concepts == {metal}


def test_genuine_out_of_vocab_label_is_still_reported():
    """The sentinel filter must not swallow real out-of-vocabulary discoveries."""
    metal = _concept("Metal", level=1, ancestors=[])
    phrase_trail = PhraseTrail(
        phrase="we machine inconel and other metals",
        lvl_by_lvl_itps={
            2: {
                IterativelyTaggedPhrase(
                    parent_group_id="Metal",
                    group_id="Inconel",
                    direct_og_tag_w_rules={},
                    iterative_og_tag_w_rules={
                        "Inconel": _rules("names inconel")
                    },
                )
            },
        },
    )

    concepts, oov = get_deepest_concepts_and_oov(
        phrase_trail=phrase_trail,
        match_label_to_concept_map=_match_label_map(metal),
    )

    assert oov == {"Inconel"}
    assert concepts == set()


def test_a_sentinel_alongside_a_real_option_yields_the_real_one():
    """A response may carry both — the sentinel says nothing else qualified, the
    real option says something did. The sentinel is dropped and the real option
    proceeds; neither raises. Today this falls out of the in-vocab filter rather
    than from anything that says so, which is why it is pinned here."""
    metal = _concept("Metal", level=1, ancestors=[])
    steel = _concept("Steel", level=2, ancestors=["Metal"])

    phrase_trail = PhraseTrail(
        phrase="we machine steel and other metals",
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
                    direct_og_tag_w_rules={},
                    iterative_og_tag_w_rules={"Steel": _rules("names steel")},
                ),
                IterativelyTaggedPhrase(
                    parent_group_id="Metal",
                    group_id=NONE_OF_THE_ABOVE_TAG,
                    direct_og_tag_w_rules={},
                    iterative_og_tag_w_rules={
                        NONE_OF_THE_ABOVE_TAG: _rules("nothing else named")
                    },
                ),
            },
        },
    )

    concepts, oov = get_deepest_concepts_and_oov(
        phrase_trail=phrase_trail,
        match_label_to_concept_map=_match_label_map(metal, steel),
    )

    assert {c.name for c in concepts} == {"Steel"}
    assert oov == set()
