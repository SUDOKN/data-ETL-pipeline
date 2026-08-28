from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field

from core.models.extraction_schemas.grounding import (
    PhraseToTagAndRulesMap,
    StopReason,
    TagToAppliedRulesMap,
)


# Stage 5
class IterativelyTaggedPhraseGroup(BaseModel):
    parent_group_id: Optional[str]
    group_id: str  # MUST BE EITHER IN-VOCAB CONCEPT **NAME** or OUT-OF-VOCAB TAG, no altLabel allowed
    # --- why nothing came of this node: two fields, two granularities -------
    #
    # `stop_reason` is about the NODE: it exists but must never be descended.
    # `declined_records` is about a (node, RECORD) pair: that record, asked
    # under this node, evidenced nothing more specific — the empty-options stop
    # — and this is the model's own reason for it. One field cannot carry both
    # without lying about which it means.
    #
    # They are neighbours because they are the same event in two eras: v1's
    # `sentinel` WAS a declination, expressed as a node wearing a non-label.
    # v2 made declination structural and record-level, which is exactly why the
    # node stopped being created and the trace vanished — 76% of descent
    # requests on run 20260825T194457 ended in a declination that reached
    # neither the stats nor the dump. `false_child` is the genuinely different
    # member: a real concept named under the wrong parent.
    #
    # Reading them together answers "why is there nothing under this node?":
    #   stop_reason set                        -> never should have been asked
    #   declined_records covers every record   -> asked; all declined
    #   declined_records covers some           -> asked; the rest have children
    #   neither, and no children               -> A DEFECT (thinned by the
    #       response hold, or a bug) — an absence that used to be
    #       indistinguishable from a legitimate declination.
    stop_reason: Optional[StopReason] = None
    # record_id -> why that record yields nothing more specific than group_id.
    # Keys are always a subset of this node's evidence (`direct_` ∪
    # `iterative_`): the descent request is built from exactly those records.
    declined_records: dict[str, str] = Field(default_factory=dict)

    # Provision for the case where tag used is an alt label
    # by explicitly storing original tag as well
    direct_phrases_to_og_tag_w_rules: PhraseToTagAndRulesMap
    # { phrase -> (og tag, applied rules) } because og tag can be an alt label
    # produced by initial grounding results

    iterative_phrases_to_og_tag_w_rules: PhraseToTagAndRulesMap
    # { phrase -> (og tag, applied rules) }
    # produced by iterative traversal of a phrase from its initially grounded concept

    # the tag inside each PhraseToTagAndRulesMap
    # will either be name/altLabel of in-vocab concept or out-of-vocab term
    # depending on the tag_id being in-vocab or not
    # but that's what each PhraseToTagAndRulesMap will have in common with
    # the tag_id

    # Identity matches IterativeTaggingRequest: (parent, group). Two parents
    # whose descents both stopped at the same non-label are distinct records; a
    # group_id-only hash collapsed them per level and lost one parent's verdicts.
    def __hash__(self) -> int:
        return hash((self.parent_group_id, self.group_id))

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, IterativelyTaggedPhraseGroup):
            return NotImplemented
        return (self.parent_group_id, self.group_id) == (
            other.parent_group_id,
            other.group_id,
        )


IterativeGroundingResult = dict[int, set[IterativelyTaggedPhraseGroup]]


# Phrase Trail
class PhraseTrail(BaseModel):
    phrase: str
    lvl_by_lvl_itps: dict[int, set["IterativelyTaggedPhrase"]]
    # can start at different levels,
    # and from different group tags on the same level, hence list


class IterativelyTaggedPhrase(BaseModel):
    parent_group_id: Optional[str]
    group_id: str
    stop_reason: Optional[StopReason] = None
    # This record's own half of the group's `declined_records`: why the descent
    # under `group_id` yielded nothing more specific FOR THIS RECORD. None when
    # the record was not asked here, or was asked and produced children.
    declined: Optional[str] = None
    direct_og_tag_w_rules: TagToAppliedRulesMap
    iterative_og_tag_w_rules: TagToAppliedRulesMap

    # (parent, group) identity, same reasoning as IterativelyTaggedPhraseGroup:
    # one phrase can reach the same non-label under two parents at one level,
    # and both verdicts belong in its trail.
    def __hash__(self) -> int:
        return hash((self.parent_group_id, self.group_id))

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, IterativelyTaggedPhrase):
            return NotImplemented
        return (self.parent_group_id, self.group_id) == (
            other.parent_group_id,
            other.group_id,
        )
