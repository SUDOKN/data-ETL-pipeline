from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict, model_validator

from core.models.extraction_schemas.applied_rule import AppliedRule

# v1's reserved escape-hatch labels. The sentinel CONTRACT is retired in v2 —
# no prompt offers these, no wire arm accepts them, no catalog declares them —
# but the strings survive as a tripwire: a model can still echo one as a minted
# candidate or descent option out of habit, and the one result path screening
# never vets (the descent trail) filters them so a declination can never
# persist as a discovered out-of-vocabulary label (the none-of-the-above bug,
# measured twice before the guard existed).
NONE_OF_THE_ABOVE_TAG = "None of the above"
CANNOT_CATEGORIZE_TAG = "Cannot categorize"

SENTINEL_GROUNDING_LABELS = frozenset({NONE_OF_THE_ABOVE_TAG, CANNOT_CATEGORIZE_TAG})

_SENTINEL_GROUNDING_LABELS_FOLDED = frozenset(
    label.casefold() for label in SENTINEL_GROUNDING_LABELS
)


def is_sentinel_grounding_label(label: str) -> bool:
    """Whether ``label`` is one of v1's reserved non-labels. Compared case- and
    surrounding-whitespace-insensitively because a model's casing drifts
    between calls, and label lookups elsewhere (``match_label_to_concept_map``)
    are already case-insensitive."""
    return label.strip().casefold() in _SENTINEL_GROUNDING_LABELS_FOLDED


# Why a descent node exists but must never be descended. "false_child": the
# response named a real concept that is not a child of the parent it was asked
# under — recorded as the event it is rather than asserted as a finding.
# Carried as data on the node (previously a "-FALSE_CHILD" suffix mangled into
# the node name, which made the name unmatchable and silently erased the
# record). "sentinel" is HISTORICAL: v1's reserved "None of the above" answer;
# v2 retired the sentinel (an empty options array with an explanation is the
# declination), so no new node carries it — the member survives only so v1
# deferred documents still load.
StopReason = Literal["sentinel", "false_child"]


# Stage 4. Every map below is on the STORED side of parsing. It carries the same
# AppliedRule the model returns: validation checks a report against its catalog
# but no longer transforms it, so there is one type for both sides.
TagToAppliedRulesMap = dict[str, list[AppliedRule]]
PhraseToTagAndRulesMap = dict[
    str, TagToAppliedRulesMap  # { phrase -> {tag: [applied_rule, ...]}, ...}
]  # The tags phrases can be linked to, with the rules that justified each link.

# v2 (pipeline v2): the same shape keyed by the masked record_id instead of the
# phrase. Structurally identical; a separate name because which identity the
# outer key carries is not recoverable from the type, and v2 stage results are
# joined through the id→phrase map the masked relationship stats persist.
RecordToTagAndRulesMap = dict[
    str, TagToAppliedRulesMap  # { record_id -> {tag: [applied_rule, ...]}, ...}
]


class RecordGroundingEntry(BaseModel):
    """One record's stored grounding result in v2: its tags, or its declination.

    The wire's structural declination (empty units + required-nullable
    explanation) survives INTO storage — v1's ``no_candidate_explanation``
    exists because ~45% of rejections carried no recorded reason, and a stored
    shape that dropped the explanation would reopen that hole one stage over.
    The validator pins the correlation, so a stored entry can never read as
    silently declined or as explained-away tags.
    """

    tags: TagToAppliedRulesMap
    # Non-null exactly when ``tags`` is empty: why the record yields nothing.
    explanation: Optional[str] = None

    @model_validator(mode="after")
    def check_declination_correlation(self) -> "RecordGroundingEntry":
        if not self.tags and not (self.explanation and self.explanation.strip()):
            raise ValueError(
                "a record with no tags must carry its declination explanation"
            )
        if self.tags and self.explanation is not None:
            raise ValueError(
                "a record with tags carries no explanation slot; the rules on "
                "each tag are the record's reasoning"
            )
        return self


# record_id -> its grounding entry: the v2 stored shape for in-vocab, OOV and
# freehand grounding alike.
RecordGroundingResults = dict[str, RecordGroundingEntry]

# Stage 5 — the same links, regrouped tag-major so a descent can be planned per
# tag. Structurally identical to the two above; kept as separate names because
# which side the outer key comes from is not recoverable from the type.
PhraseToAppliedRulesMap = dict[
    str, list[AppliedRule]  # { phrase -> [applied_rule, ...], ...}
]
TagToPhraseAndRulesMap = dict[
    str, PhraseToAppliedRulesMap  # { tag -> {phrase: [applied_rule, ...]}, ...}
]


# The two grounding wire schemas are the same shape and deliberately not shared.
# Freehand grounding invents its own label, and what it is asked for is a CATEGORY —
# the family the thing named in the phrase belongs to, never the thing itself.
# Initial and recursive grounding pick from a supplied list, so what comes back is an
# OPTION — one of the ones provided, one the model proposes when none fit, or the
# reserved "None of the above". Naming both "tag" put a third, undefined concept in
# the prompts: nothing said how the thing identified from the phrase became a tag.
# The field name the model sees matches the word its rules use.
#
# The split stops here. Both collapse to PhraseToTagAndRulesMap at the end of
# parsing, and everything downstream of that keeps its existing tag-based names —
# initial and recursive grounding also land in those types and do not produce
# categories, so "tag" stays the right word for the shared side.
#
# Both wire schemas are GENERATED per catalog now, by
# `catalog_wire_schema.build_option_grounding_response_model` and its category
# twin: the rule slots an entry carries are named for that catalog's own rule ids,
# so there is no one class either stage could share. The distinction above lives on
# in the `unit_key` those builders take.
