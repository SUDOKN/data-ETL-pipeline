"""The requiredness contract of TextFieldAudit, branch by branch.

Every rejection here is a rule the audit-submission service (P2.3) gets to
assume instead of re-checking: a plain agree carries no text, and both
agree_but and disagree always carry a non-blank corrected_text.
"""

from datetime import datetime, timezone
from typing import Any

import pytest
from pydantic import ValidationError

from core.models.ground_truth.audits import AuditVerdict, TextFieldAudit

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


def test_at_has_no_default():
    with pytest.raises(ValidationError, match="at"):
        TextFieldAudit(  # type: ignore[call-arg] — the missing `at` IS the test
            type=AuditVerdict.AGREE, author_email="a@example.com", source="api"
        )


def test_extra_fields_are_rejected():
    with pytest.raises(ValidationError, match="judge"):
        TextFieldAudit(type=AuditVerdict.AGREE, judge_model="gpt-5", **_COMMON)  # type: ignore[call-arg]


def test_round_trips_through_serialization():
    audit = TextFieldAudit(
        type=AuditVerdict.DISAGREE, corrected_text="corrected", **_COMMON
    )
    dumped = audit.model_dump(mode="json")
    assert dumped["type"] == "disagree"
    assert TextFieldAudit.model_validate(dumped) == audit
