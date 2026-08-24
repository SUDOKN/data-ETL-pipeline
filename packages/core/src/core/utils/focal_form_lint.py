"""Record-only lint for the synthesis stage: did the synthesis describe the
entity it was asked about?

The synthesis hold validates that the model echoed the right ``record_id``. It
does not — and cannot — check that the paragraph is about the right *entity*, so
a swapped name is indistinguishable downstream from a correct one and grounds as
the wrong thing. Run 20260823T200044 produced exactly that: the record whose
focal form and only snippet both read ``FE Series Double-Egress Frames`` came
back describing ``DE Series Double-Egress Frames``, a real and different product
that sat in the same 50-entry request.

The check is the obvious one — the focal form, or one of its member forms, ought
to appear in its own synthesis — and the whole difficulty is separating a swapped
entity from ordinary good writing.

**Shape.** The lint fires only on ENTITY-SHAPED focal forms: two to six tokens
with a capital letter or digit somewhere after the first token — the signature of
a proper name ("FE Series Double-Egress Frames", "Paladin PW Series"), and not of
a clause lifted out of a bullet. Applied to clause-shaped forms the check is pure
noise: a bullet gets grammatically re-inflected ("Factory prepared for
field-installed silencers" -> "the manufacturer factory prepares frames for
field-installed silencers"), which is correct writing, not a defect.

**Match.** A verbatim occurrence (punctuation and case collapsed) satisfies the
check outright. Failing that, every *distinctive* token of the form — connectives
and articles dropped, plurals folded — must appear somewhere in the synthesis.
This second path was added on 2026-08-24, when retiring the own-name ban changed
how the stage writes: the old statics QUOTED the focal form ("'Precision
Fixturing & Machining' is described as..."), the new ones write it into the
sentence's grammar ("Alec Model performs precision fixturing and machining"). A
contiguous-substring test reads that re-inflection as a missing entity, and the
flag count went 2 -> 13 on identical inputs with every one of the 13 false.

Order is deliberately NOT required, and matching is on whole tokens, never
substrings. A swap is caught by the *discriminating* token going missing ("fe" is
absent from a paragraph about DE Series), and that token is often two characters
long — which is also why the plural fold leaves short tokens alone.

Measured over three runs of the same 2,133 records, 700 of them entity-shaped:

    run 20260823T195031   3 flagged (contiguous)  ->  1 flagged, 0 real
    run 20260823T200044   2 flagged (contiguous)  ->  1 flagged, 1 real (the swap)
    run 20260824T002404  13 flagged (contiguous)  ->  1 flagged, 0 real
    run 20260824T010654   6 flagged (contiguous)  ->  0 flagged

One row per run, and on the run that contained a real swap that row IS the swap.
The two residual false flags are both an abbreviation or a rating expanded into
words ("Surface Finishing & QA" -> "surface finishing and quality assurance"),
which is worth leaving visible rather than folding away. The check still catches
the one failure mode nothing else in the pipeline can see.

Deliberately NOT a retry trigger, and deliberately NOT a hard failure: like
``subject_name_lint``, it renders a verdict about a string and changes nothing
downstream. Promote it only if the measured rate climbs.
"""

from __future__ import annotations

import re
from typing import Iterable

_NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")

# Stripped before anything else: "Steelcraft's" would otherwise split into
# "steelcraft" plus a bare "s" that no synthesis contains, flagging every
# possessive focal form. Measured on run 20260824T010654, where the prose moved
# from "Steelcraft's Express Stock program" to "Steelcraft operates the Express
# Stock program": 3 of the 3 flags were this and nothing else.
_POSSESSIVE_RE = re.compile(r"['\u2019]s\b")

# A proper name carries a capital or digit past its first token. The lower bound
# of two tokens keeps bare one-word forms out (they re-inflect freely — "polish"
# -> "polishing"); the upper bound of six keeps whole bullets out, which is where
# the contiguous check's false positives all live.
_MIN_TOKENS = 2
_MAX_TOKENS = 6

# Dropped from the form before matching: the synthesis rewrites these freely, and
# "&" normalizes to nothing while the prose spells it "and". None of them carries
# the identity of an entity, so losing them cannot hide a swap.
_CONNECTIVES = frozenset(
    {"and", "or", "the", "a", "an", "of", "for", "as", "in", "on", "with", "to", "by"}
)

# Below this length a trailing "s" is more likely to belong to the name than to
# be a plural ("DS", "MS Series"), and folding it is how a swap slips through.
_MIN_FOLD_LENGTH = 4


def _normalize(text: str) -> str:
    lowered = _POSSESSIVE_RE.sub("", (text or "").lower())
    return _NON_ALNUM_RE.sub(" ", lowered).strip()


def _fold(token: str) -> str:
    """Fold the one inflection the synthesis actually applies to a name: the
    plural ("Fit Test" -> "fit tests")."""
    if len(token) >= _MIN_FOLD_LENGTH and token.endswith("s"):
        return token[:-1]
    return token


def _distinctive(text: str) -> list[str]:
    return [_fold(t) for t in _normalize(text).split() if t not in _CONNECTIVES]


def is_entity_shaped(focal_form: str) -> bool:
    """Whether ``focal_form`` looks like a proper name rather than a clause."""
    tokens = (focal_form or "").split()
    if not _MIN_TOKENS <= len(tokens) <= _MAX_TOKENS:
        return False
    return any(re.match(r"^[A-Z0-9]", token) for token in tokens[1:])


def focal_form_absent(
    synthesis: str, focal_form: str, forms: Iterable[str] = ()
) -> bool:
    """True when an entity-shaped ``focal_form`` — and every one of the group's
    member ``forms`` — is missing from the synthesis written for it.

    Member forms count because the site's own spelling is what the synthesis is
    told to use, and a group holds every casing and punctuation variant of it.
    """
    if not synthesis or not is_entity_shaped(focal_form):
        return False
    candidates = [c for c in (focal_form, *forms) if c]

    haystack = _normalize(synthesis)
    if any(_normalize(needle) in haystack for needle in candidates):
        return False

    present = {_fold(token) for token in haystack.split()}
    return not any(
        (tokens := _distinctive(needle)) and all(t in present for t in tokens)
        for needle in candidates
    )
