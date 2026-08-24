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
to appear in its own synthesis — but applied naively it is noise: 24 of 2,133
records (1.1%) fail it, nearly all because a sentence-shaped focal form was
grammatically re-inflected ("Factory prepared for field-installed silencers" ->
"the manufacturer factory prepares frames for field-installed silencers"), which
is correct writing, not a defect.

So the lint fires only on ENTITY-SHAPED focal forms: two to six tokens with a
capital letter or digit somewhere after the first token — the signature of a
proper name ("FE Series Double-Egress Frames", "Paladin PW Series"), and not of
a clause lifted out of a bullet. Measured over both arms of that day's A/B:

    run 20260823T195031   700 entity-shaped records   3 flagged   0 real
    run 20260823T200044   700 entity-shaped records   2 flagged   1 real

Two rows per run is cheap enough to read by eye, and it catches the one failure
mode nothing else in the pipeline can see.

Deliberately NOT a retry trigger, and deliberately NOT a hard failure: like
``subject_name_lint``, it renders a verdict about a string and changes nothing
downstream. Promote it only if the measured rate climbs.
"""

from __future__ import annotations

import re
from typing import Iterable

_NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")

# A proper name carries a capital or digit past its first token. The lower bound
# of two tokens keeps bare one-word forms out (they re-inflect freely — "polish"
# -> "polishing"); the upper bound of six keeps whole bullets out, which is where
# the naive check's false positives all live.
_MIN_TOKENS = 2
_MAX_TOKENS = 6


def _normalize(text: str) -> str:
    return _NON_ALNUM_RE.sub(" ", (text or "").lower()).strip()


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
    haystack = _normalize(synthesis)
    candidates = [focal_form, *forms]
    return not any(
        needle and _normalize(needle) in haystack for needle in candidates
    )
