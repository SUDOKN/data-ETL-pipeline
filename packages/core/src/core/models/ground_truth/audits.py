from __future__ import annotations

from datetime import datetime
from enum import Enum

from pydantic import BaseModel, ConfigDict, model_validator


class AuditVerdict(str, Enum):
    """A human's judgment on one LLM-authored field.

    ``AGREE_BUT`` means "correct but incomplete" — the LLM's text stands, and
    ``corrected_text`` carries the addendum. It is the recall flag: counting
    these across a run measures what the LLM systematically leaves out.
    ``DISAGREE`` means the text is wrong and ``corrected_text`` carries the
    replacement.
    """

    AGREE = "agree"
    AGREE_BUT = "agree_but"
    DISAGREE = "disagree"


class TextFieldAudit(BaseModel):
    """One audit entry on a single LLM-authored text field.

    Entries accumulate in submission order in an ``audits`` list next to the
    field they judge; ordering, the same-author-resubmission pop, and the
    latest-wins fold are the audit-submission service's contract, not this
    model's. ``at`` has no default on purpose (the ``ConceptCorrectionLog``
    convention): the timestamp is the submission time the service witnessed,
    never the moment this object happened to be constructed.

    ``corrected_text`` is forbidden on a plain ``agree`` so that agreement can
    never smuggle in a correction — anything worth typing is worth the
    ``agree_but`` recall flag or a ``disagree``.

    ``source`` stays a plain string here: the enum of permitted sources lives
    app-side (``GroundTruthSource``) and ``packages/core`` cannot import the
    app; the Document layer binds it.
    """

    model_config = ConfigDict(extra="forbid")

    type: AuditVerdict
    corrected_text: str | None = None
    author_email: str
    at: datetime
    source: str

    @model_validator(mode="after")
    def _corrected_text_matches_verdict(self) -> "TextFieldAudit":
        if self.type is AuditVerdict.AGREE:
            if self.corrected_text is not None:
                raise ValueError(
                    "corrected_text is forbidden on an 'agree' audit; use "
                    "'agree_but' for an addendum or 'disagree' for a correction"
                )
        elif self.corrected_text is None or not self.corrected_text.strip():
            raise ValueError(
                f"corrected_text is required for an '{self.type.value}' audit"
            )
        return self
