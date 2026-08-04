from __future__ import annotations

from typing import Optional

from pydantic import BaseModel

from packages.core.src.core.models.extraction_schemas.grounding import (
    PhraseToTagAndReasonMap,
    TagAndReasonMap,
)


# Stage 5
class IterativelyTaggedPhraseGroup(BaseModel):
    parent_group_id: Optional[str]
    group_id: str  # MUST BE EITHER IN-VOCAB CONCEPT **NAME** or OUT-OF-VOCAB TAG, no altLabel allowed

    # Provision for the case where tag used is an alt label
    # by explicitly storing original tag as well
    direct_phrases_to_og_tag_w_reason: PhraseToTagAndReasonMap
    # { phrase -> (og tag, reason) } because og tag can be an alt label
    # produced by initial grounding results

    iterative_phrases_to_og_tag_w_reason: PhraseToTagAndReasonMap
    # { phrase -> (og tag, reason) }
    # produced by iterative traversal of a phrase from its initially grounded concept

    # the tag inside each PhraseToTagAndReasonMap
    # will either be name/altLabel of in-vocab concept or out-of-vocab term
    # depending on the tag_id being in-vocab or not
    # but that's what each PhraseToTagAndReasonMap will have in common with
    # the tag_id

    def __hash__(self) -> int:
        return hash(self.group_id)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, IterativelyTaggedPhraseGroup):
            return NotImplemented
        return self.__hash__() == other.__hash__()


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
    direct_og_tag_w_reason: TagAndReasonMap
    iterative_og_tag_w_reason: TagAndReasonMap

    def __hash__(self) -> int:
        return hash(self.group_id)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, IterativelyTaggedPhrase):
            return NotImplemented
        return self.__hash__() == other.__hash__()
