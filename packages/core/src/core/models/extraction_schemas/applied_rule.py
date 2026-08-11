from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class AppliedRule(BaseModel):
    """One catalog rule, the outcome it reached, and why.

    ``explanation`` is the whole of the justification. It is asked to cite the
    phrase and its relationship summary and to quote the words it relied on, but
    nothing here checks that: the quoting is a prompt-level instruction aimed at
    the annotators who read these records, not a machine-verified field.

    An earlier version carried a structured ``evidence`` array of quoted spans,
    validated slice by slice against the source text. It was removed on
    2026-08-11. The removal is worth recording because the failures that drove
    that design are instructive: each attempt to forbid a fabrication ("never
    reworded", "never assembled from separate places", "never added to") was
    satisfied by the next one, since a model asked for a span the source does not
    contain has no compliant move available. What replaced it is a positive
    instruction — cite the sources, quote what you relied on — carried in prose
    where a paraphrase is a legible weakness rather than a parse failure that
    costs a whole group request.

    This one model is both the WIRE shape — what the model returns, and what
    OpenAI's strict mode builds its json_schema from — and the stored shape.
    There is no longer any resolution step between them to justify two types.

    ``outcome`` is a plain string here because the permitted values depend on the
    rule's kind, which only the catalog knows. It is checked against
    ``RuleCatalog.valid_outcomes`` at parse time.
    """

    model_config = ConfigDict(extra="forbid")

    rule_id: str
    outcome: str
    explanation: str
