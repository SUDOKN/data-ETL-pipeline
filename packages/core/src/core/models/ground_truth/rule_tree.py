from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, ConfigDict, model_validator

from core.models.ground_truth.audits import TextFieldAudit
from core.models.rule_catalog import Combinator, RuleKind


class AuditedRule(BaseModel):
    """One catalog rule as it stands in a ground-truth document, audits attached.

    The wire reports rules FLAT (``AppliedRule {rule_id, outcome, explanation}``);
    the tree shape here comes from catalog inflation (P2.1), never from the report
    alone. ``reported`` records which of the two a node is, and has no default so
    the inflation code must say so consciously:

    - ``reported=True`` — the LLM's response contained this row, so ``outcome``
      and ``explanation`` are both present (the wire shape requires them).
    - ``reported=False`` — the template synthesized the node from the catalog.
      An unreported guard carries its implied outcome and no explanation — that
      is the auditable silence the design wants. A ``note`` child is pure
      context: never reported, no outcome vocabulary, so neither field.

    ``outcome`` is a plain string for the same reason as ``AppliedRule``: the
    permitted values are catalog *data* (``outcome_vocab`` by kind), checked at
    submission time against the pinned ``catalog_version`` — this model only
    enforces what is true of every catalog.
    """

    model_config = ConfigDict(extra="forbid")

    rule_id: str
    kind: RuleKind
    outcome: Optional[str] = None
    explanation: Optional[str] = None
    reported: bool
    audits: list[TextFieldAudit] = []
    sub_rules: list["AuditedRule"] = []

    @model_validator(mode="after")
    def check_reported_implies_wire_fields(self) -> "AuditedRule":
        if self.reported and (self.outcome is None or self.explanation is None):
            raise ValueError(
                f"rule {self.id_for_errors}: reported=True means the wire row "
                "existed, and wire rows always carry outcome and explanation"
            )
        return self

    @model_validator(mode="after")
    def check_note_is_pure_context(self) -> "AuditedRule":
        if self.kind == "note" and (self.reported or self.outcome is not None):
            raise ValueError(
                f"rule {self.id_for_errors}: kind='note' is never reported and "
                "carries no outcome vocabulary"
            )
        return self

    @property
    def id_for_errors(self) -> str:
        return self.rule_id or "<missing rule_id>"


class AuditedSection(BaseModel):
    """A catalog section's rules as they stand in a ground-truth document.

    ``combinator`` is the catalog's own vocabulary (imported, not re-declared);
    how it folds the rules into a ``passed`` verdict is the fold's business
    (P2.4), not this model's.
    """

    model_config = ConfigDict(extra="forbid")

    section_id: str
    combinator: Combinator
    applied_rules: list[AuditedRule]
