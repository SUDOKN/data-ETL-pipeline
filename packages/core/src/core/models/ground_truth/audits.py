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

    ``note`` is the free-form rationale beside the machine-consumed
    ``corrected_text``: on a ``disagree`` the corrected_text carries the
    verbatim replacement (the submission contract holds them equal on
    replacement surfaces), so the note is where "why the original is wrong and
    why the replacement is right" lives; on an ``agree_but`` it is optional
    context for the addendum. Forbidden on a plain ``agree`` for the same
    reason corrected_text is.

    ``source`` stays a plain string here: the enum of permitted sources lives
    app-side (``GroundTruthSource``) and ``packages/core`` cannot import the
    app; the Document layer binds it.
    """

    model_config = ConfigDict(extra="forbid")

    type: AuditVerdict
    corrected_text: str | None = None
    note: str | None = None
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

    @model_validator(mode="after")
    def _note_matches_verdict(self) -> "TextFieldAudit":
        if self.note is None:
            return self
        if self.type is AuditVerdict.AGREE:
            raise ValueError(
                "note is forbidden on an 'agree' audit; anything worth "
                "explaining is worth the 'agree_but' flag or a 'disagree'"
            )
        if not self.note.strip():
            raise ValueError(
                f"note on an '{self.type.value}' audit, when given, "
                f"must not be blank"
            )
        return self


class EntityFieldAudit(BaseModel):
    """One audit entry on an IDENTIFIER a derivation asserts — screening's
    ``identified_entity`` and grounding's ``tag`` (the design's own likening:
    the tag plays the entity's role).

    Deliberately NOT a ``TextFieldAudit``: an identifier's replacement value
    lives in the derivation itself (``identified_entity`` / ``tag``), so this
    model carries no ``corrected_text`` — there is no duplicate to keep
    coherent. Consequences:

    - Verdicts are ``agree | disagree`` only. ``agree_but`` ("correct but
      incomplete") does not map to an identifier: a missing grounding is
      asserted structurally (a human-asserted tag entry), and a caveat about a
      kept identifier belongs on the specific rule (a rule-level
      ``agree_but``).
    - ``note`` is the only prose slot, so a ``disagree`` REQUIRES it — the
      rationale for replacing an identifier must be recorded somewhere. It is
      forbidden on ``agree`` (the ``TextFieldAudit`` doctrine).

    Provenance mirrors ``TextFieldAudit``: ``at`` has no default on purpose.
    """

    model_config = ConfigDict(extra="forbid")

    type: AuditVerdict
    note: str | None = None
    author_email: str
    at: datetime
    source: str

    @model_validator(mode="after")
    def _verdict_is_binary(self) -> "EntityFieldAudit":
        if self.type is AuditVerdict.AGREE_BUT:
            raise ValueError(
                "an identifier is kept or replaced — 'agree_but' does not "
                "apply; assert a missing grounding as its own entry, and put "
                "caveats about a kept identifier on the specific rule"
            )
        return self

    @model_validator(mode="after")
    def _note_matches_verdict(self) -> "EntityFieldAudit":
        if self.type is AuditVerdict.DISAGREE:
            if self.note is None or not self.note.strip():
                raise ValueError(
                    "a 'disagree' on an identifier requires a non-blank note "
                    "— the replacement lives in the derivation, so the note "
                    "is the only record of why"
                )
        elif self.note is not None:
            raise ValueError(
                "note is forbidden on an 'agree' audit; anything worth "
                "explaining is worth a 'disagree' or a rule-level audit"
            )
        return self
