"""The stage blocks' structural contract.

The load-bearing pieces: the ScreeningLLMCopy branch shape (mirrors
ScreeningVerdict), the passed-is-a-cache fold over REPORTED rows only (the
catalog-free half of ``passed_implied_by``), and full keyword-shaped and
concept-shaped phrase instances round-tripping through serialization.
"""

from datetime import datetime, timezone
from typing import Any

import pytest
from pydantic import ValidationError

from core.models.ground_truth.audits import AuditVerdict, TextFieldAudit
from core.models.ground_truth.rule_tree import AuditedRule, AuditedSection
from core.models.ground_truth.stage_blocks import (
    ExtractedPhraseGT,
    GroundingDerivation,
    HumanScreeningDerivation,
    InVocabGroundingGT,
    InVocabNodeGT,
    MissedPhraseEntry,
    OovGroundingGT,
    RelationshipGT,
    ScreeningGT,
    ScreeningLLMCopy,
    TagGroundingGT,
)

_AUDIT = TextFieldAudit(
    type=AuditVerdict.AGREE,
    author_email="annotator@example.com",
    at=datetime(2026, 8, 15, tzinfo=timezone.utc),
    source="api",
)


def _condition(rule_id: str, outcome: str = "satisfied") -> AuditedRule:
    return AuditedRule(
        rule_id=rule_id,
        kind="condition",
        outcome=outcome,
        explanation="cited from the relationship summary",
        reported=True,
    )


def _sections(*rules: AuditedRule) -> list[AuditedSection]:
    return [
        AuditedSection(
            section_id="conditions", combinator="all", applied_rules=list(rules)
        )
    ]


def _screening_copy(**overrides) -> ScreeningLLMCopy:
    fields: dict[str, Any] = dict(
        passed=True,
        identified_entity="CNC machining",
        sections=_sections(_condition("SCR-1")),
    )
    fields.update(overrides)
    return ScreeningLLMCopy(**fields)


# --- ScreeningLLMCopy: passed is a cache of the reported-rules fold ---


def test_satisfied_conditions_fold_to_passed():
    assert _screening_copy().passed is True


@pytest.mark.parametrize("outcome", ["failed", "not_triggered"])
def test_condition_not_holding_forces_passed_false(outcome):
    with pytest.raises(ValidationError, match="cache of the fold"):
        _screening_copy(sections=_sections(_condition("SCR-1", outcome)))
    copy = _screening_copy(
        passed=False, sections=_sections(_condition("SCR-1", outcome))
    )
    assert copy.passed is False


def test_reported_guard_rejects():
    guard = AuditedRule(
        rule_id="SCR-G1",
        kind="guard",
        outcome="violated",
        explanation="the phrase is about a customer's process",
        reported=True,
    )
    with pytest.raises(ValidationError, match="cache of the fold"):
        _screening_copy(sections=_sections(_condition("SCR-1"), guard))


def test_synthesized_unreported_guard_does_not_reject():
    silent_guard = AuditedRule(
        rule_id="SCR-G1", kind="guard", outcome="not_violated", reported=False
    )
    copy = _screening_copy(sections=_sections(_condition("SCR-1"), silent_guard))
    assert copy.passed is True


def test_note_children_are_ignored_by_the_fold():
    parent = _condition("SCR-1")
    parent.sub_rules = [AuditedRule(rule_id="SCR-1-note", kind="note", reported=False)]
    assert _screening_copy(sections=_sections(parent)).passed is True


def test_passed_true_without_any_reported_rule_is_rejected():
    with pytest.raises(ValidationError, match="always carries reported rules"):
        _screening_copy(sections=_sections())


# --- ScreeningLLMCopy: branch shape mirrors ScreeningVerdict ---


def _no_candidate_copy(**overrides) -> ScreeningLLMCopy:
    fields: dict[str, Any] = dict(
        passed=False,
        identified_entity=None,
        no_candidate_explanation="the phrase names no capability of theirs",
        sections=_sections(),
    )
    fields.update(overrides)
    return ScreeningLLMCopy(**fields)


def test_no_candidate_branch_is_valid():
    assert _no_candidate_copy().passed is False


def test_no_candidate_with_reported_rules_is_rejected():
    with pytest.raises(ValidationError, match="no rule can have been reported"):
        _no_candidate_copy(sections=_sections(_condition("SCR-1")))


def test_no_candidate_without_explanation_is_rejected():
    with pytest.raises(ValidationError, match="must say why"):
        _no_candidate_copy(no_candidate_explanation=None)


def test_identified_entity_with_no_candidate_explanation_is_rejected():
    with pytest.raises(ValidationError, match="only on the no-candidate branch"):
        _screening_copy(no_candidate_explanation="but there was one")


def test_entity_without_reported_rules_is_rejected():
    with pytest.raises(ValidationError, match="always carries reported rules"):
        _screening_copy(passed=False, sections=_sections())


# --- Grounding blocks ---


def test_human_asserted_tag_without_derivation_is_rejected():
    with pytest.raises(ValidationError, match="asserts nothing"):
        TagGroundingGT(llm_result=None)


def test_human_asserted_tag_with_derivation_is_valid():
    entry = TagGroundingGT(
        llm_result=None,
        audits=[
            GroundingDerivation(
                tag="Milling Machines", sections=_sections(_condition("FGR-1"))
            )
        ],
    )
    assert entry.llm_result is None


# --- Full phrase instances: keyword-shaped and concept-shaped ---


def _keyword_phrase() -> ExtractedPhraseGT:
    return ExtractedPhraseGT(
        search_round=1,
        llm_relationship=RelationshipGT(
            llm_result="a machine they operate in-house", audits=[_AUDIT]
        ),
        llm_screening=ScreeningGT(
            llm_result=_screening_copy(identified_entity="5-axis CNC mill"),
            audits=[
                HumanScreeningDerivation(
                    identified_entity="5-axis CNC mill",
                    audits=[_AUDIT],
                    sections=_sections(_condition("SCR-1")),
                )
            ],
        ),
        oov_grounding=OovGroundingGT(
            tags={
                "CNC Milling Machine": TagGroundingGT(
                    llm_result=_sections(_condition("FGR-1"))
                )
            }
        ),
    )


def _concept_phrase() -> ExtractedPhraseGT:
    return ExtractedPhraseGT(
        search_round=2,
        llm_relationship=RelationshipGT(llm_result="an industry they serve"),
        llm_screening=ScreeningGT(llm_result=_screening_copy()),
        oov_grounding=OovGroundingGT(
            tags={"Aerospace": TagGroundingGT(llm_result=_sections(_condition("IGR-Q1")))}
        ),
        in_vocab_grounding=InVocabGroundingGT(
            levels={
                1: [
                    InVocabNodeGT(
                        parent_group_id=None,
                        group_id="Aerospace",
                        sections=_sections(_condition("RGR-1")),
                    )
                ],
                2: [
                    InVocabNodeGT(
                        parent_group_id="Aerospace",
                        group_id="Aircraft Engines",
                        stop_reason="sentinel",
                        sections=[],
                        audits=[
                            GroundingDerivation(
                                tag="Aircraft Engines",
                                audits=[_AUDIT],
                                sections=_sections(_condition("RGR-1")),
                            )
                        ],
                    )
                ],
            }
        ),
    )


@pytest.mark.parametrize("build", [_keyword_phrase, _concept_phrase])
def test_phrase_instance_round_trips(build):
    phrase = build()
    dumped = phrase.model_dump(mode="json")
    assert ExtractedPhraseGT.model_validate(dumped) == phrase


def test_keyword_shape_has_no_recursive_descent():
    assert _keyword_phrase().in_vocab_grounding is None


def test_concept_descent_keeps_identity_and_stop_reason():
    dumped = _concept_phrase().model_dump(mode="json")
    node = dumped["in_vocab_grounding"]["levels"]["2"][0]
    assert node["parent_group_id"] == "Aerospace"
    assert node["stop_reason"] == "sentinel"


# --- Missed phrases: a full positive extraction claim, happy path enforced ---


def _missed(**overrides) -> MissedPhraseEntry:
    fields: dict[str, Any] = dict(
        phrase="wire EDM",
        author_email="annotator@example.com",
        at=datetime(2026, 8, 15, tzinfo=timezone.utc),
        source="api",
        relationship_text="a process they perform for customers",
        screening=HumanScreeningDerivation(
            identified_entity="wire EDM",
            sections=_sections(_condition("SCR-1")),
        ),
        groundings=[
            GroundingDerivation(
                tag="Electrical Discharge Machining",
                sections=_sections(_condition("FGR-1")),
            )
        ],
    )
    fields.update(overrides)
    return MissedPhraseEntry(**fields)


def test_missed_phrase_entry_is_human_only_and_round_trips():
    entry = _missed()
    assert MissedPhraseEntry.model_validate(entry.model_dump(mode="json")) == entry


def test_missed_phrase_requires_screening():
    with pytest.raises(ValidationError, match="screening"):
        _missed(screening=None)


def test_missed_phrase_requires_an_identified_entity():
    with pytest.raises(ValidationError, match="must identify an entity"):
        _missed(
            screening=HumanScreeningDerivation(
                identified_entity=None, sections=_sections(_condition("SCR-1"))
            )
        )


@pytest.mark.parametrize("outcome", ["failed", "not_triggered", None])
def test_missed_phrase_screening_condition_must_be_satisfied(outcome):
    with pytest.raises(ValidationError, match="screen to satisfaction"):
        _missed(
            screening=HumanScreeningDerivation(
                identified_entity="wire EDM",
                sections=_sections(
                    AuditedRule(
                        rule_id="SCR-1",
                        kind="condition",
                        outcome=outcome,
                        explanation="human derivation" if outcome else None,
                        reported=False,
                    )
                ),
            )
        )


def test_missed_phrase_screening_with_fired_guard_is_rejected():
    fired = AuditedRule(
        rule_id="SCR-G1",
        kind="guard",
        outcome="violated",
        explanation="the process is the customer's",
        reported=False,
    )
    with pytest.raises(ValidationError, match="guard 'SCR-G1' fired"):
        _missed(
            screening=HumanScreeningDerivation(
                identified_entity="wire EDM",
                sections=_sections(_condition("SCR-1"), fired),
            )
        )


def test_missed_phrase_screening_without_any_condition_is_rejected():
    with pytest.raises(ValidationError, match="no condition was applied"):
        _missed(
            screening=HumanScreeningDerivation(
                identified_entity="wire EDM", sections=_sections()
            )
        )


def test_missed_phrase_grounding_must_reach_satisfaction():
    with pytest.raises(ValidationError, match="ground to satisfaction.*FGR-1"):
        _missed(
            groundings=[
                GroundingDerivation(
                    tag="Electrical Discharge Machining",
                    sections=_sections(_condition("FGR-1", "failed")),
                )
            ]
        )


def test_missed_phrase_grounding_preferences_do_not_gate():
    chosen = AuditedRule(
        rule_id="IGR-M1",
        kind="preference",
        outcome="chosen",
        explanation="exact label match",
        reported=False,
    )
    entry = _missed(
        groundings=[
            GroundingDerivation(
                tag="Aerospace", sections=_sections(_condition("IGR-Q1"), chosen)
            )
        ]
    )
    assert entry.groundings[0].tag == "Aerospace"


def test_missed_phrase_at_has_no_default():
    with pytest.raises(ValidationError, match="at"):
        MissedPhraseEntry(  # type: ignore[call-arg] — the missing `at` IS the test
            phrase="wire EDM",
            author_email="a@example.com",
            source="api",
            relationship_text="a process they perform",
            screening=HumanScreeningDerivation(
                identified_entity="wire EDM",
                sections=_sections(_condition("SCR-1")),
            ),
        )
