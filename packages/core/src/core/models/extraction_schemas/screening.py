from __future__ import annotations

from pydantic import BaseModel

from core.models.extraction_schemas.applied_rule import AppliedRule

# Per-candidate screening (pipeline v2).
#
# Candidates are SUPPLIED by grounding, so there is no identified_entity and no
# no-candidate branch: every candidate a record carries gets judged, and a
# record with no candidates is never sent. ``passed`` is DERIVED, not reported:
# the model returns only its rules, and ``passed_implied_by`` reads the verdict
# off them at parse time; it is stored because every downstream reader wants
# the answer rather than the derivation.


class CandidateScreeningVerdict(BaseModel):
    passed: bool
    applied_rules: list[AppliedRule]


# record_id -> {candidate -> verdict}: one judgment per supplied candidate.
RecordScreeningResults = dict[str, dict[str, CandidateScreeningVerdict]]
