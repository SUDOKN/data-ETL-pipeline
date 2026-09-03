"""Designation-shaped tokens: the mechanical vocabulary of the synthesis
stage's under-enumeration check (Phase B of the search-recall roadmap,
2026-09-02 — ``docs_local/SEARCH_RECALL_ROADMAP_2026-09-02.md``).

A "designation" is a token whose information lives in its FORM, not its
meaning — a model code, grade, or standard id. The check built on this module
is a CONSERVATION law, never a semantic judgment: designation tokens found in
a record's entries must appear verbatim in its synthesis, and a synthesis
that names fewer than the entries carry gets the stage's one retry (measured
2026-09-02: the hardened synthesis prompt preserves 333/333 designations on a
401-entry record, but one 12/48 tail outlier under repetition is why the
mechanical check exists).

The tiers are deliberately asymmetric — high precision over recall, because a
false POSITIVE here becomes a demanded token and a wasted retry, while a
false negative merely goes unchecked (the prompt still asks for it):

- T1 (core): a token carrying BOTH a letter and a digit (``J-2550``,
  ``A357``, ``AS9100``). In ordinary prose such tokens essentially never
  occur, so every hit is an identifier.
- T2 (pairs): an ALL-CAPS word or a kind-word (``Grade``, ``Alloy``, ...)
  followed by a digit-bearing token — ``ISO 9001``, ``PRO 125``,
  ``Grade 630``. Bare years after non-kind-words are excluded (``in 2023``
  is a date, ``Section 172`` is a designation).
- T3 (focal-adjacent): pure-digit tokens in the short span after an
  occurrence of the record's focal form — ``Aluminum 319, 356`` — where the
  focal form itself vouches that the numbers designate. Year-shaped tokens
  are excluded here too.

Fields whose entities carry no identifiers (industries, most process
capabilities) yield no tokens and the check is vacuous — by design. A field
whose identifier grammar is unusual (NAICS's pure six-digit codes) extends
via ``extra_patterns``, never by loosening the core tiers.
"""

from __future__ import annotations

import re
from typing import Iterable, Optional

# T1: both a letter and a digit somewhere in a /- delimited token.
_CORE_TOKEN_RE = re.compile(
    r"\b(?=[A-Za-z0-9/-]*[A-Za-z])(?=[A-Za-z0-9/-]*\d)[A-Za-z0-9][A-Za-z0-9/-]*\b"
)

# T2: pair head — ALL-CAPS (2-8 chars) or a capitalized kind-word.
_KIND_WORDS = frozenset({
    "Grade", "Alloy", "Series", "Model", "Type", "Class", "Mark",
    "Section", "Regulation", "Clause", "Form", "Schedule",
})
_PAIR_RE = re.compile(
    r"\b([A-Z]{2,8}|[A-Z][a-z]{2,11})\s+((?=[A-Za-z0-9/-]*\d)[A-Za-z0-9][A-Za-z0-9/.-]*)\b"
)
_YEAR_RE = re.compile(r"^(?:19|20)\d{2}$")

# T3: pure digits (2-4, non-year) within this many characters after the focal
# form; enough for a short enumeration, too short to wander into the next
# claim. The lookarounds exclude digit runs living inside a larger token
# (``J-2550``'s 2550) — those are whole designations and T1's business.
_FOCAL_SPAN_CHARS = 80
_FOCAL_DIGITS_RE = re.compile(r"(?<![A-Za-z0-9/-])(\d{2,4})(?![A-Za-z0-9/.-])")

_MIN_TOKEN_LEN = 3


def designation_tokens(
    text: str,
    *,
    focal_form: Optional[str] = None,
    extra_patterns: Iterable[re.Pattern[str]] = (),
) -> set[str]:
    """Every designation-shaped token (or T2 pair, space-normalized) in *text*."""
    tokens: set[str] = set()
    for match in _CORE_TOKEN_RE.finditer(text):
        token = match.group(0)
        if len(token) >= _MIN_TOKEN_LEN:
            tokens.add(token)
    for match in _PAIR_RE.finditer(text):
        head, tail = match.group(1), match.group(2)
        if _YEAR_RE.match(tail) and head not in _KIND_WORDS and not head.isupper():
            continue
        if head.isupper() or head in _KIND_WORDS:
            tokens.add(f"{head} {tail}")
    if focal_form:
        for occurrence in re.finditer(re.escape(focal_form), text, re.IGNORECASE):
            span = text[occurrence.end(): occurrence.end() + _FOCAL_SPAN_CHARS]
            span = span.split(". ")[0]  # never past the sentence
            for match in _FOCAL_DIGITS_RE.finditer(span):
                if not _YEAR_RE.match(match.group(1)):
                    tokens.add(match.group(1))
    for pattern in extra_patterns:
        tokens.update(m.group(0) for m in pattern.finditer(text))
    return tokens


def _present(token: str, text: str) -> bool:
    boundary = r"(?<![A-Za-z0-9]){}(?![A-Za-z0-9])"
    pattern = boundary.format(re.escape(token).replace(r"\ ", r"\s+"))
    return re.search(pattern, text, re.IGNORECASE) is not None


def missing_designations(
    entry_texts: Iterable[str],
    synthesis: str,
    *,
    focal_form: Optional[str] = None,
) -> set[str]:
    """Designation tokens the entries carry that *synthesis* does not.

    The conservation check: word-boundary presence, case-insensitive, elastic
    whitespace inside T2 pairs. Empty set = the synthesis conserves every
    checkable designation."""
    demanded: set[str] = set()
    for entry_text in entry_texts:
        demanded |= designation_tokens(entry_text, focal_form=focal_form)
    return {token for token in demanded if not _present(token, synthesis)}


def designation_coverage(
    entry_texts: Iterable[str],
    synthesis: str,
    *,
    focal_form: Optional[str] = None,
) -> tuple[int, int]:
    """(present, demanded) counts — the comparator the under-enumeration
    retry uses to keep the better of two answers for one record."""
    demanded: set[str] = set()
    for entry_text in entry_texts:
        demanded |= designation_tokens(entry_text, focal_form=focal_form)
    present = sum(1 for token in demanded if _present(token, synthesis))
    return present, len(demanded)
