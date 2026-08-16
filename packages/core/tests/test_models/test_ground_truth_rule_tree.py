"""The structural contract of the audited rule tree.

Catalog-free checks only: what is true of every catalog lives here (wire rows
carry outcome+explanation, notes are pure context); what depends on a specific
catalog's data (outcome vocabularies, rule ids, section completeness) is the
submission service's job (P2.3).
"""

from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from core.models.ground_truth.audits import AuditVerdict, TextFieldAudit
from core.models.ground_truth.rule_tree import AuditedRule, AuditedSection

_AUDIT = TextFieldAudit(
    type=AuditVerdict.DISAGREE,
    corrected_text="the entity is the customer's, not theirs",
    author_email="annotator@example.com",
    at=datetime(2026, 8, 15, tzinfo=timezone.utc),
    source="api",
)


def _reported(rule_id: str, **overrides) -> dict:  # dict[str, Any] by design: splat fixture
    row = dict(
        rule_id=rule_id,
        kind="condition",
        outcome="pass",
        explanation="the phrase names something they make",
        reported=True,
    )
    row.update(overrides)
    return row


def test_three_level_recursion_round_trips_with_audits():
    leaf = AuditedRule(**_reported("SCR-1a-i"), audits=[_AUDIT])
    tree = AuditedRule(
        **_reported("SCR-1"),
        sub_rules=[AuditedRule(**_reported("SCR-1a"), sub_rules=[leaf])],
    )
    section = AuditedSection(
        section_id="conditions", combinator="all", applied_rules=[tree]
    )

    dumped = section.model_dump(mode="json")
    assert AuditedSection.model_validate(dumped) == section
    reloaded_leaf = dumped["applied_rules"][0]["sub_rules"][0]["sub_rules"][0]
    assert reloaded_leaf["audits"][0]["type"] == "disagree"


@pytest.mark.parametrize("dropped", ["outcome", "explanation"])
def test_reported_row_missing_wire_fields_is_rejected(dropped):
    with pytest.raises(ValidationError, match="wire rows always carry"):
        AuditedRule(**_reported("SCR-1", **{dropped: None}))


def test_unreported_guard_carries_implied_outcome_without_explanation():
    guard = AuditedRule(
        rule_id="SCR-G1",
        kind="guard",
        outcome="not_violated",
        explanation=None,
        reported=False,
    )
    assert not guard.reported and guard.explanation is None


def test_note_context_node_is_valid_bare():
    note = AuditedRule(rule_id="SCR-1-note", kind="note", reported=False)
    assert note.outcome is None and note.explanation is None


@pytest.mark.parametrize(
    "violation",
    [
        dict(reported=True, outcome="pass", explanation="notes never report"),
        dict(outcome="pass"),
    ],
    ids=["reported", "outcome"],
)
def test_note_with_report_or_outcome_is_rejected(violation):
    node = dict(rule_id="SCR-1-note", kind="note", reported=False) | violation
    with pytest.raises(ValidationError, match="never reported"):
        AuditedRule(**node)


def test_kind_outside_catalog_vocabulary_is_rejected():
    with pytest.raises(ValidationError, match="kind"):
        AuditedRule(**_reported("SCR-1", kind="exception"))


def test_combinator_outside_catalog_vocabulary_is_rejected():
    with pytest.raises(ValidationError, match="combinator"):
        AuditedSection(section_id="conditions", combinator="most", applied_rules=[])  # type: ignore[arg-type] — invalid on purpose


def test_extra_fields_are_rejected():
    with pytest.raises(ValidationError, match="passed"):
        AuditedRule(**_reported("SCR-1"), passed=True)  # type: ignore[call-arg] — extra field on purpose


def test_sibling_rules_get_independent_default_lists():
    a = AuditedRule(**_reported("SCR-1"))
    b = AuditedRule(**_reported("SCR-2"))
    a.audits.append(_AUDIT)
    assert b.audits == []
