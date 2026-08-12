from __future__ import annotations

from typing import Optional

from pydantic import BaseModel

from core.models.extraction_schemas.applied_rule import AppliedRule

# Stage 3
LLMScreeningResults = dict[str, str]  # To only keep the relationships we care about


class ScreeningVerdict(BaseModel):
    """Structured verdict from live GPT phrase_relationship_screening (strict json_schema
    output). Distinct from LLMScreeningResults, which is the legacy Yes—/No— string
    convention still used by human-corrected ground-truth data.

    ``passed`` is DERIVED, not reported: the model returns only its rules, and
    ``passed_implied_by`` reads the verdict off them at parse time. It is stored
    because every downstream reader wants the answer rather than the derivation.

    ``applied_rules`` is the model's own report, checked against the catalog that
    asked for it but otherwise stored as returned. Empty exactly when the phrase
    offered no candidate — every catalog declares at least one always-reported
    condition, so a judged phrase always carries rules.

    The WIRE shape is generated per catalog by ``catalog_wire_schema``; this is the
    stored shape it flattens into. The two were deliberately one type while the
    wire format was a flat rule list, and are two now because the wire type carries
    cardinality guarantees a stored record has no way to express and no need to.
    """

    passed: bool
    identified_entity: Optional[str]
    applied_rules: list[AppliedRule]

    # Why nothing was identified — populated only on the no-candidate branch, where
    # there are no rules to carry the reasoning. Before it existed, ~45% of screened
    # phrases were rejected with no recorded reason at all, and the report that broke
    # the parser on 2026-08-11 was a model trying to volunteer one into a slot the
    # schema did not have.
    no_candidate_explanation: Optional[str] = None


LiveScreeningResults = dict[str, ScreeningVerdict]  # phrase -> verdict
