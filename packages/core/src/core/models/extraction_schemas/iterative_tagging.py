from __future__ import annotations

from typing import Optional

from pydantic import BaseModel

from core.models.extraction_schemas.grounding import (
    PhraseToTagAndRulesMap,
    StopReason,
    TagToAppliedRulesMap,
)


# Stage 5
class IterativelyTaggedPhraseGroup(BaseModel):
    parent_group_id: Optional[str]
    group_id: str  # MUST BE EITHER IN-VOCAB CONCEPT **NAME** or OUT-OF-VOCAB TAG, no altLabel allowed
    # Why this node exists but was never descended (sentinel / false child).
    # None for ordinary nodes. Mirrors IterativeTaggingRequest.stop_reason.
    stop_reason: Optional[StopReason] = None

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
