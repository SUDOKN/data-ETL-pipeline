"""Record-only lint for the relationship stage's own-name ban.

The relationship prompt bans the manufacturer's name from every description,
quotations included, and the pipeline knows exactly what that name is — it is
the ``subject_name`` the request context opened with. That makes the ban the
one prompt contract that is mechanically checkable, and measurement (2026-08-13,
5-subject corpus) found it violated in 14.6% of summaries. This module is the
checker; it renders a verdict about a string, never about the pipeline — hits
are recorded in the phrase-trail dumps and change nothing downstream.

Deliberately NOT a repair: substituting "the manufacturer" mechanically is a
possible later step, but it rewrites evidence, so it stays off until the
recorded rate after the prompt rework says it is still needed.
"""

from __future__ import annotations

import re

# A trailing legal form is dropped before matching because the measured leak
# mode is the bare name — a summary says "Steelcraft" where the pinned subject
# is "Steelcraft, Inc." — and a full-string match would miss every one of them.
_LEGAL_SUFFIX_RE = re.compile(
    r"[\s,]+(?:inc|incorporated|llc|l\.l\.c|ltd|limited|corp|corporation|co|company)\.?\s*$",
    re.IGNORECASE,
)

# Below this length the stripped name is too likely to be an ordinary word
# ("Co", "AB") for a substring count to mean anything; such a subject falls
# back to its unstripped form rather than flooding the dump with false hits.
_MIN_CORE_LENGTH = 3


def _core_name(subject_name: str) -> str:
    core = _LEGAL_SUFFIX_RE.sub("", subject_name).strip()
    if len(core) < _MIN_CORE_LENGTH:
        core = subject_name.strip()
    return core


def count_own_name_hits(text: str, subject_name: str) -> int:
    """Occurrences of the subject's name in ``text``, word-bounded.

    Case-insensitive, because the leaked name often mirrors the site's casing
    ("STEELCRAFT" in a heading) rather than the pinned subject_name. Bounded by
    lookarounds instead of ``\\b`` so a name ending in a non-word character
    still anchors, and "Steelcraft's" still counts while "Steelcrafting" does
    not.
    """
    if not text or not subject_name or not subject_name.strip():
        return 0
    core = _core_name(subject_name)
    pattern = re.compile(
        rf"(?<![A-Za-z0-9]){re.escape(core)}(?![A-Za-z0-9])",
        re.IGNORECASE,
    )
    return len(pattern.findall(text))
