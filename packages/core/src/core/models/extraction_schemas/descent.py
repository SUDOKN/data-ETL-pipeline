"""Step 2 descent in depth waves (design draft §6.3 / §6.5, user decisions
2026-09-20 and 2026-09-22): the request bookkeeping a chunk's bundle keeps,
and the trail the stage stores.

Per chunk the loop runs one WAVE per vocabulary depth: the wave's units
(the vocabulary labels matched directly at that depth, plus the ones descent
reached from the wave before, minus any pair whose ancestor failed on the
same record) are screened in one batched round, then every accepted label
with children gets a descent request over its accepted records, and an
accepted label without children gets the leaf step when the run flag is on.
After the last wave, EVERY proposal from every stage — the grounding call,
the proposal pass, descent's sibling proposals, the leaf steps — is screened
together in one batch (the proposal wave, key 0), so no proposal ships
unvetted and none is screened per level.
"""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field

from core.models.extraction_schemas.grounding import RecordGroundingResults
from core.models.extraction_schemas.screening import RecordScreeningResults

# The unit-screening wave that holds every proposal, issued after the last
# depth wave. Depth waves are 1..max depth.
PROPOSAL_WAVE = 0


class DescentRequest(BaseModel):
    """One descent request issued after a wave's screening: the accepted
    parent it descends from (by its vocabulary name), the wave whose
    screening accepted it, whether it is the leaf step (no children listed),
    and the request id (digest included)."""

    parent: str
    wave: int
    leaf: bool
    req_id: str


class WaveTrail(BaseModel):
    """What one depth wave did for a chunk, as stored."""

    # label → the records screened under it (the wave's units)
    units: dict[str, list[str]] = Field(default_factory=dict)
    # record → candidate → verdict, this wave's screening
    screening: RecordScreeningResults = Field(default_factory=dict)
    # accepted parent → per-record descent answer (children matched, sibling
    # proposals, or the declination that lets the parent stand)
    descent: dict[str, RecordGroundingResults] = Field(default_factory=dict)
    # accepted leaf → per-record leaf-step answer (proposals only)
    leaf: dict[str, RecordGroundingResults] = Field(default_factory=dict)
    # parent → record → vocabulary labels the model named that are not that
    # parent's children: recorded, never screened, never descended (the
    # parent stands — user decision 2026-09-22)
    false_children: dict[str, dict[str, list[str]]] = Field(default_factory=dict)
    # pairs removed before screening because an ancestor failed on the record
    removed_under_failed_ancestor: dict[str, list[str]] = Field(default_factory=dict)


class DescentTrail(BaseModel):
    """A chunk's whole descent: the depth waves, then the proposal wave."""

    waves: dict[int, WaveTrail] = Field(default_factory=dict)
    # proposal label → the records it was proposed for (all stages folded)
    proposal_units: dict[str, list[str]] = Field(default_factory=dict)
    # the proposal wave's verdicts: record → proposal → verdict
    proposal_screening: RecordScreeningResults = Field(default_factory=dict)
    # proposal label → where it came from ("grounding", "proposal_pass",
    # "descent:<parent>", "leaf:<parent>"), for the review of candidates
    proposal_sources: dict[str, list[str]] = Field(default_factory=dict)
    max_depth: Optional[int] = None
