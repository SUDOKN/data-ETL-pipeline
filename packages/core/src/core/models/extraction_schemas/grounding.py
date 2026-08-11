from __future__ import annotations

from pydantic import BaseModel, ConfigDict

from core.models.extraction_schemas.applied_rule import AppliedRule

# Reserved labels a grounding stage may return in place of a real one. Initial and
# recursive grounding return NONE_OF_THE_ABOVE_TAG when nothing qualifies — those
# stages choose from a list of options, and "None of the above" reads as one of
# them. Freehand grounding has no options to choose from, so its job is to
# categorize and its escape hatch is CANNOT_CATEGORIZE_TAG. Both record that the
# model declined, so they are deliberations rather than discovered labels and must
# never reach the results.
NONE_OF_THE_ABOVE_TAG = "None of the above"
CANNOT_CATEGORIZE_TAG = "Cannot categorize"

SENTINEL_GROUNDING_LABELS = frozenset({NONE_OF_THE_ABOVE_TAG, CANNOT_CATEGORIZE_TAG})

_SENTINEL_GROUNDING_LABELS_FOLDED = frozenset(
    label.casefold() for label in SENTINEL_GROUNDING_LABELS
)


def is_sentinel_grounding_label(label: str) -> bool:
    """Whether ``label`` is a reserved non-label — a freehand ``category`` or an
    initial/recursive ``option``, which is why the name says neither. Compared case-
    and surrounding-whitespace-insensitively because the model's casing drifts
    between calls, and label lookups elsewhere (``match_label_to_concept_map``) are
    already case-insensitive."""
    return label.strip().casefold() in _SENTINEL_GROUNDING_LABELS_FOLDED


# Stage 4. Every map below is on the STORED side of parsing. It carries the same
# AppliedRule the model returns: validation checks a report against its catalog
# but no longer transforms it, so there is one type for both sides.
TagToAppliedRulesMap = dict[str, list[AppliedRule]]
PhraseToTagAndRulesMap = dict[
    str, TagToAppliedRulesMap  # { phrase -> {tag: [applied_rule, ...]}, ...}
]  # The tags phrases can be linked to, with the rules that justified each link.

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


class PhraseGroundingCategory(BaseModel):
    model_config = ConfigDict(extra="forbid")

    category: str
    applied_rules: list[AppliedRule]


class PhraseCategoryGroundingEntry(BaseModel):
    model_config = ConfigDict(extra="forbid")

    phrase: str
    categories: list[PhraseGroundingCategory]


class PhraseCategoryGroundingResponse(BaseModel):
    """Wire schema for freehand grounding, where the model names the category itself."""

    model_config = ConfigDict(extra="forbid")

    groundings: list[PhraseCategoryGroundingEntry]


class PhraseGroundingOption(BaseModel):
    model_config = ConfigDict(extra="forbid")

    option: str
    applied_rules: list[AppliedRule]


class PhraseOptionGroundingEntry(BaseModel):
    model_config = ConfigDict(extra="forbid")

    phrase: str
    options: list[PhraseGroundingOption]


class PhraseOptionGroundingResponse(BaseModel):
    """Wire schema for initial and recursive grounding, which choose from options."""

    model_config = ConfigDict(extra="forbid")

    groundings: list[PhraseOptionGroundingEntry]
