"""The requiredness contract of TextFieldAudit, branch by branch.

Every rejection here is a rule the audit-submission service (P2.3) gets to
assume instead of re-checking: a plain agree carries no text, and both
agree_but and disagree always carry a non-blank corrected_text.
"""

from datetime import datetime, timezone
from typing import Any

import pytest
from pydantic import ValidationError

from core.models.ground_truth.audits import (
    AuditVerdict,
    EntityFieldAudit,
    TextFieldAudit,
)

_COMMON: dict[str, Any] = dict(
    author_email="annotator@example.com",
    at=datetime(2026, 8, 15, tzinfo=timezone.utc),
    source="api",
)


def test_agree_without_corrected_text_is_valid():
    audit = TextFieldAudit(type=AuditVerdict.AGREE, **_COMMON)
    assert audit.corrected_text is None


def test_agree_with_corrected_text_is_rejected():
    with pytest.raises(ValidationError, match="forbidden on an 'agree'"):
        TextFieldAudit(
            type=AuditVerdict.AGREE, corrected_text="sneaky correction", **_COMMON
        )


@pytest.mark.parametrize("verdict", [AuditVerdict.AGREE_BUT, AuditVerdict.DISAGREE])
def test_non_agree_with_corrected_text_is_valid(verdict):
    audit = TextFieldAudit(type=verdict, corrected_text="the real result", **_COMMON)
    assert audit.corrected_text == "the real result"


@pytest.mark.parametrize("verdict", [AuditVerdict.AGREE_BUT, AuditVerdict.DISAGREE])
@pytest.mark.parametrize("missing_text", [None, "", "   "], ids=["none", "empty", "blank"])
def test_non_agree_without_corrected_text_is_rejected(verdict, missing_text):
    with pytest.raises(ValidationError, match="corrected_text is required"):
        TextFieldAudit(type=verdict, corrected_text=missing_text, **_COMMON)


def test_agree_with_note_is_rejected():
    with pytest.raises(ValidationError, match="note is forbidden on an 'agree'"):
        TextFieldAudit(type=AuditVerdict.AGREE, note="but actually…", **_COMMON)


@pytest.mark.parametrize("verdict", [AuditVerdict.AGREE_BUT, AuditVerdict.DISAGREE])
def test_non_agree_with_note_is_valid(verdict):
    audit = TextFieldAudit(
        type=verdict,
        corrected_text="the real result",
        note="the LLM's text names the vendor, not the equipment",
        **_COMMON,
    )
    assert audit.note == "the LLM's text names the vendor, not the equipment"


@pytest.mark.parametrize("verdict", [AuditVerdict.AGREE_BUT, AuditVerdict.DISAGREE])
def test_note_is_optional_on_non_agree(verdict):
    audit = TextFieldAudit(type=verdict, corrected_text="the real result", **_COMMON)
    assert audit.note is None


@pytest.mark.parametrize("verdict", [AuditVerdict.AGREE_BUT, AuditVerdict.DISAGREE])
@pytest.mark.parametrize("blank", ["", "   "], ids=["empty", "blank"])
def test_blank_note_is_rejected(verdict, blank):
    with pytest.raises(ValidationError, match="must not be blank"):
        TextFieldAudit(
            type=verdict, corrected_text="the real result", note=blank, **_COMMON
        )


def test_at_has_no_default():
    with pytest.raises(ValidationError, match="at"):
        TextFieldAudit(  # type: ignore[call-arg] — the missing `at` IS the test
            type=AuditVerdict.AGREE, author_email="a@example.com", source="api"
        )


def test_extra_fields_are_rejected():
    with pytest.raises(ValidationError, match="judge"):
        TextFieldAudit(type=AuditVerdict.AGREE, judge_model="gpt-5", **_COMMON)  # type: ignore[call-arg]


# --- EntityFieldAudit: the identifier-surface audit --------------------------


def test_entity_audit_agree_is_bare():
    audit = EntityFieldAudit(type=AuditVerdict.AGREE, **_COMMON)
    assert audit.note is None


def test_entity_audit_has_no_corrected_text_at_all():
    with pytest.raises(ValidationError, match="corrected_text"):
        EntityFieldAudit(
            type=AuditVerdict.AGREE,
            corrected_text="smuggled",  # type: ignore[call-arg] — the extra field IS the test
            **_COMMON,
        )


def test_entity_audit_agree_but_does_not_apply():
    with pytest.raises(ValidationError, match="kept or replaced"):
        EntityFieldAudit(type=AuditVerdict.AGREE_BUT, note="partial", **_COMMON)


def test_entity_audit_disagree_requires_a_note():
    audit = EntityFieldAudit(
        type=AuditVerdict.DISAGREE, note="the text names a lathe", **_COMMON
    )
    assert audit.note == "the text names a lathe"


@pytest.mark.parametrize("missing", [None, "", "   "], ids=["none", "empty", "blank"])
def test_entity_audit_disagree_without_note_is_rejected(missing):
    with pytest.raises(ValidationError, match="requires a non-blank note"):
        EntityFieldAudit(type=AuditVerdict.DISAGREE, note=missing, **_COMMON)


def test_entity_audit_note_forbidden_on_agree():
    with pytest.raises(ValidationError, match="forbidden on an 'agree'"):
        EntityFieldAudit(type=AuditVerdict.AGREE, note="but…", **_COMMON)


def test_entity_audit_round_trips():
    audit = EntityFieldAudit(
        type=AuditVerdict.DISAGREE, note="why the replacement is right", **_COMMON
    )
    dumped = audit.model_dump(mode="json")
    assert dumped["type"] == "disagree"
    assert "corrected_text" not in dumped
    assert EntityFieldAudit.model_validate(dumped) == audit


def test_round_trips_through_serialization():
    audit = TextFieldAudit(
        type=AuditVerdict.DISAGREE,
        corrected_text="corrected",
        note="why the replacement is right",
        **_COMMON,
    )
    dumped = audit.model_dump(mode="json")
    assert dumped["type"] == "disagree"
    assert dumped["note"] == "why the replacement is right"
    assert TextFieldAudit.model_validate(dumped) == audit
