from requests.structures import CaseInsensitiveDict

from core.models.deferred_extraction.deferred_concept_extraction import TaggingResult
from core.models.extraction_schemas.applied_rule import AppliedRule
from core.models.skos_concept import Concept
from core.services.pipeline_nodes.multi_stage.llm_initial_grounding_service import (
    get_settled_concepts_and_oov_from_trs,
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


def test_exact_label_phrase_settles_at_its_own_concept():
    """The Automotive regression (2026-08-11): a brute-seeded phrase equal to the
    concept's own label is pruned from descent, so it never enters iterative
    tagging and the phrase trail never carries it. It must surface here or the
    concept silently vanishes from the results."""
    automotive = _concept("Automotive", level=1)
    trs = [
        TaggingResult(
            group_id="Automotive",
            phrase_rules_map={"Automotive": _rules("brute-seeded exact match")},
        )
    ]

    settled, oov = get_settled_concepts_and_oov_from_trs(
        initially_tagged_trs=trs,
        match_label_to_concept_map=_match_label_map(automotive),
    )

    assert settled == {automotive}
    assert oov == set()


def test_descend_worthy_tag_is_not_settled():
    """A phrase that is not one of the concept's labels makes the concept
    descend-worthy; the trail carries it, so it must not also settle here —
    settling it would resurrect a parent the descent replaced."""
    automotive = _concept("Automotive", level=1)
    trs = [
        TaggingResult(
            group_id="Automotive",
            phrase_rules_map={
                "serving the automotive industry": _rules("free phrase")
            },
        )
    ]

    settled, oov = get_settled_concepts_and_oov_from_trs(
        initially_tagged_trs=trs,
        match_label_to_concept_map=_match_label_map(automotive),
    )

    assert settled == set()
    assert oov == set()


def test_mixed_phrases_go_to_the_trail_not_here():
    """One exact-label phrase plus one free phrase: the concept is descend-worthy
    on the free phrase, so the trail owns it and nothing settles here."""
    automotive = _concept("Automotive", level=1)
    trs = [
        TaggingResult(
            group_id="Automotive",
            phrase_rules_map={
                "Automotive": _rules("brute-seeded exact match"),
                "serving the automotive industry": _rules("free phrase"),
            },
        )
    ]

    settled, oov = get_settled_concepts_and_oov_from_trs(
        initially_tagged_trs=trs,
        match_label_to_concept_map=_match_label_map(automotive),
    )

    assert settled == set()
    assert oov == set()


def test_alt_label_phrase_settles_too():
    """matchLabels covers altLabels, so a phrase equal to an altLabel is pruned
    from descent exactly like the primary name and must settle the same way."""
    tool_and_die = _concept("Tool and Die", level=1, altLabels=["Tool & Die"])
    trs = [
        TaggingResult(
            group_id="Tool and Die",
            phrase_rules_map={"Tool & Die": _rules("alt label match")},
        )
    ]

    settled, oov = get_settled_concepts_and_oov_from_trs(
        initially_tagged_trs=trs,
        match_label_to_concept_map=_match_label_map(tool_and_die),
    )

    assert settled == {tool_and_die}
    assert oov == set()


def test_out_of_vocab_tag_is_reported_as_oov():
    trs = [
        TaggingResult(
            group_id="Flexible Manufacturing",
            phrase_rules_map={"flexible manufacturing": _rules("proposed option")},
        )
    ]

    settled, oov = get_settled_concepts_and_oov_from_trs(
        initially_tagged_trs=trs,
        match_label_to_concept_map=_match_label_map(),
    )

    assert settled == set()
    assert oov == {"Flexible Manufacturing"}


def test_callers_tagging_results_are_not_mutated():
    """The descend-worthy filter prunes phrase maps in place, so this function
    must run it on copies: reconcile still holds the originals."""
    automotive = _concept("Automotive", level=1)
    trs = [
        TaggingResult(
            group_id="Automotive",
            phrase_rules_map={
                "Automotive": _rules("brute-seeded exact match"),
                "serving the automotive industry": _rules("free phrase"),
            },
        )
    ]

    get_settled_concepts_and_oov_from_trs(
        initially_tagged_trs=trs,
        match_label_to_concept_map=_match_label_map(automotive),
    )

    assert set(trs[0].phrase_rules_map) == {
        "Automotive",
        "serving the automotive industry",
    }
