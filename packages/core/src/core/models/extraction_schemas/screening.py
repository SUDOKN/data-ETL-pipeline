from __future__ import annotations

from typing import Literal, Optional

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
    # Step 2 unit screening (2026-09-21): per accepted record the evidence
    # distance the model reported — "named" when the record's own words name
    # the candidate, "inferred" when it is one plain step from them — kept as
    # METADATA until the census has calibrated it against the judges (never a
    # gate); per not-accepted record the first condition or guard that failed;
    # and the words of the record that decided it either way. Defaulted so
    # verdicts stored before the fields existed load unchanged.
    evidence: Optional[Literal["named", "inferred"]] = None
    failed_rule: Optional[str] = None
    quote: Optional[str] = None
    # Whether an accepted record's quote was found verbatim in the record
    # (2026-09-22, first Step 2 run): the model paraphrases the record about
    # as often as it quotes it, so a non-verbatim quote keeps the verdict and
    # is marked False here — the same rule grounding applies (``unverified``)
    # — instead of failing the whole request. None = not checked (a rejection,
    # or an acceptance offered with no quote at all).
    quote_verified: Optional[bool] = None


# record_id -> {candidate -> verdict}: one judgment per supplied candidate.
RecordScreeningResults = dict[str, dict[str, CandidateScreeningVerdict]]
