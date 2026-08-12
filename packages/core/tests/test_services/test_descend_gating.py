"""The single descent gate (2026-08-12).

Descent used to be gated three different ways: the exact-label prune, an
ITP-level in-vocab+has-phrases check, and bare ``name in match_label_map``
lookups in the embed/create paths. None of them asked whether the concept had
children, so 30 of 55 descent LLM calls in the measured anchor-mfg.com run went
into leaf concepts whose option list rendered empty — the model could only
answer the sentinel. ``get_descendable_concept`` is now the one gate consulted
everywhere a descent request is expected, awaited, created, or parsed; these
tests pin its semantics and the leaf regression.
"""

from requests.structures import CaseInsensitiveDict

from core.models.extraction_schemas.iterative_tagging import (
    IterativelyTaggedPhraseGroup,
)
from core.models.extraction_schemas.applied_rule import AppliedRule
from core.models.skos_concept import Concept
from core.services.pipeline_nodes.multi_stage.llm_recursive_grounding_service import (
    get_descendable_concept,
    is_rtp_descend_worthy,
)


def _concept(
    name: str,
    level: int,
    children: list[str] | None = None,
    altLabels: list[str] | None = None,
) -> Concept:
    return Concept(
        name=name,
        uri=f"http://asu.edu/semantics/SUDOKN/{name.replace(' ', '')}",
        level=level,
        altLabels=altLabels or [],
        ancestors=[],
        children=children or [],
        definition=f"The {name} concept.",
    )


def _match_label_map(*concepts: Concept) -> CaseInsensitiveDict:
    match_label_to_concept_map: CaseInsensitiveDict = CaseInsensitiveDict()
    for concept in concepts:
        for label in concept.matchLabels:
            match_label_to_concept_map[label] = concept
    return match_label_to_concept_map


def _rules(explanation: str) -> dict[str, list[AppliedRule]]:
    return {
        "some tag": [
            AppliedRule(rule_id="RGR-Q1", outcome="satisfied", explanation=explanation)
        ]
    }


STEEL = _concept("Steel", level=4, children=["Alloy Steel", "Galvanized Steel"])
ALUMINUM = _concept("Aluminum", level=4)  # leaf: 0 children in the ontology
MACHINING = _concept(
    "Machining", level=1, children=["CNC Machining"], altLabels=["Machine Work"]
)


def test_concept_with_children_is_descendable():
    assert get_descendable_concept("Steel", _match_label_map(STEEL, ALUMINUM)) is STEEL


def test_leaf_concept_is_not_descendable():
    """The 30-of-55 regression: a leaf renders an empty option list, so a
    descent into it buys a guaranteed sentinel answer."""
    assert get_descendable_concept("Aluminum", _match_label_map(STEEL, ALUMINUM)) is None


def test_out_of_vocab_label_is_not_descendable():
    assert get_descendable_concept("Welding", _match_label_map(STEEL)) is None


def test_sentinel_label_is_not_descendable():
    assert get_descendable_concept("None of the above", _match_label_map(STEEL)) is None


def test_alt_label_resolves_case_insensitively():
    assert (
        get_descendable_concept("machine work", _match_label_map(MACHINING))
        is MACHINING
    )


def _itp(group_id: str, *, direct: bool = False, iterative: bool = False):
    return IterativelyTaggedPhraseGroup(
        parent_group_id=None,
        group_id=group_id,
        direct_phrases_to_og_tag_w_rules=(
            {"some phrase": _rules("direct evidence")} if direct else {}
        ),
        iterative_phrases_to_og_tag_w_rules=(
            {"another phrase": _rules("iterative evidence")} if iterative else {}
        ),
    )


def test_itp_with_children_and_phrases_is_descend_worthy():
    mmap = _match_label_map(STEEL, ALUMINUM)
    assert is_rtp_descend_worthy(_itp("Steel", direct=True), mmap)
    assert is_rtp_descend_worthy(_itp("Steel", iterative=True), mmap)


def test_leaf_itp_is_not_descend_worthy_even_with_phrases():
    mmap = _match_label_map(STEEL, ALUMINUM)
    assert not is_rtp_descend_worthy(_itp("Aluminum", direct=True), mmap)


def test_itp_without_phrases_is_not_descend_worthy():
    mmap = _match_label_map(STEEL, ALUMINUM)
    assert not is_rtp_descend_worthy(_itp("Steel"), mmap)


def test_out_of_vocab_itp_is_not_descend_worthy():
    mmap = _match_label_map(STEEL, ALUMINUM)
    assert not is_rtp_descend_worthy(_itp("Welding", direct=True), mmap)
