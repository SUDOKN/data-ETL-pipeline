"""Text-matching primitives shared by every stage evaluation.

WHY THIS EXISTS. Every stage instrument compares model output against the
scraped site text — a search form against its window, a synthesized sentence
against its snippet, a grounding quote against its record. That comparison
looks trivial and is not: this corpus carries three traps that make a NAIVE
matcher return confident WRONG answers, silently, with no error to notice.

  1. NON-BREAKING SPACES MID-PHRASE. steelcraft.com alone holds 843 of them
     ("in accordance with\\u00a0ASTM D4585"). A model that echoes the phrase with
     an ordinary space does not match the text byte-for-byte, so a faithful
     quote scores as absent.

  2. WHITESPACE RUNS INSIDE PHRASES. The scrape leaves double spaces and
     newlines mid-phrase ("Full  glass architectural entrance doors"). Same
     consequence: a faithful echo reads as fabricated.

  3. TYPOGRAPHIC HYPHENS. The corpus prints a NON-BREAKING HYPHEN (U+2011)
     inside ordinary hyphenated words ("heat\u2011treating", "e\u2011mail",
     "pre\u2011treatment") and SOFT HYPHENS (U+00AD) inside words where they are
     completely INVISIBLE ("X\u00adray", "high\u00adresolution"). A model echoing
     either faithfully writes the ordinary form, which then fails to match.
     Measured 2026-08-27 over all 20 corpus texts: 5 non-breaking hyphens in 3
     subjects, 3 soft hyphens in 1. Small counts, but one of them sits on
     med-tekinc's ONLY capability, so it is a whole subject's recall.
     EN and EM DASHES are deliberately NOT normalized: 1,046 of 1,094 en dashes
     are whitespace-adjacent separators, and folding them to "-" would credit
     "steel-and" for "steel\u2014and". Leniency there would be false credit, not
     a rescued match.

  4. SHORT-FORM SUBSTRINGS. Plain containment credits "tight" for TIG,
     "absolute" for ABS, "recommendation" for CMM. This is the same defect
     that made the pipeline's brute search score `Lead` at 52 hits and 0 real
     ones — reproduced, independently, inside an evaluation harness.

Traps 1-3 bias toward FALSE ALARMS (correct output judged fabricated or
missing); trap 4 biases toward FALSE CREDIT (junk judged correct). A stage
instrument that hits either direction reports numbers that look plausible and
are wrong, which is worse than reporting nothing.

Found and fixed in the search-stage harness on 2026-08-26, where trap 1 had
already produced a false RED verdict on correct output before it was caught.
Extracted here so the other stage instruments do not each rediscover it.

Pure stdlib on purpose — import it from any stage without dragging in the
pipeline packages:

    import sys; from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from _shared.text_matching import normalize_spaces, occurs_in, forms_overlap
"""

from __future__ import annotations

import re
from typing import Optional

# Every Unicode space that must read as an ordinary space. Each maps ONE
# character to ONE character, so a translated string keeps its length and any
# offsets computed against it stay valid.
UNICODE_SPACES = (
    "\u00a0\u2000\u2001\u2002\u2003\u2004\u2005\u2006\u2007"
    "\u2008\u2009\u200a\u202f\u205f\u3000"
)  # written as escapes on purpose: literal invisibles are unreviewable
   # and an editor that "cleans whitespace" would silently delete them
# Characters that ARE an ordinary hyphen, typographically. Deliberately excludes
# EN DASH and EM DASH, which are separators in this corpus, not hyphens.
HYPHENS = "\u2010\u2011\u2012"  # HYPHEN, NON-BREAKING HYPHEN, FIGURE DASH

# SOFT HYPHEN marks a discretionary line break and renders as nothing, but in
# this corpus it only ever sits where the word is genuinely hyphenated
# ("X\u00adray", "high\u00adresolution"). Folding it to "-" keeps the translation
# one-for-one, so offsets stay valid. RESIDUAL, accepted knowingly: a model that
# echoes the rendered form with no hyphen at all ("Xray") still will not match.
# Deleting the character instead would fix that case and break the opposite one,
# and would cost the length-preservation guarantee the rest of the module rests on.
HYPHENS = HYPHENS + "\u00ad"

_SPACE_TRANSLATION = {ord(ch): " " for ch in UNICODE_SPACES}
_SPACE_TRANSLATION.update({ord(ch): "-" for ch in HYPHENS})

# Forms at or below this length match case-sensitively and on word boundaries.
SHORT_FORM_MAX_LENGTH = 3


def normalize_spaces(text: str) -> str:
    """Unicode spaces -> ASCII space, typographic hyphens -> ASCII hyphen; one
    character for one character.

    Length-preserving, so it is safe to apply to text you will index into.
    """
    return text.translate(_SPACE_TRANSLATION)


def collapse_whitespace(text: str) -> str:
    """Normalize Unicode spaces and squeeze every whitespace run to one space.

    NOT length-preserving — use it for comparison, never for offsets.
    """
    return " ".join(normalize_spaces(text).split())


def is_short_form(form: str) -> bool:
    return len(form.strip()) <= SHORT_FORM_MAX_LENGTH


def flexible_pattern(form: str, *, case_sensitive: bool) -> Optional[re.Pattern[str]]:
    """``form`` as a word-boundary-anchored pattern whose internal whitespace
    matches ANY whitespace run (traps 1 and 2). None for an empty form.

    Only the pattern is elastic; the text is never rewritten, so offsets taken
    from a match are offsets into the caller's own string.
    """
    stripped = normalize_spaces(form).strip()
    if not stripped:
        return None
    body = r"\s+".join(re.escape(part) for part in stripped.split())
    left = r"(?<!\w)" if re.match(r"\w", stripped[0]) else ""
    right = r"(?!\w)" if re.match(r"\w", stripped[-1]) else ""
    return re.compile(left + body + right, 0 if case_sensitive else re.IGNORECASE)


def occurs_in(form: str, text: str, *, case_sensitive: bool = True) -> bool:
    """Does ``form`` occur in ``text`` as a whole word, tolerating whitespace
    differences? Short forms are forced case-sensitive (trap 3)."""
    if is_short_form(form):
        case_sensitive = True
    pattern = flexible_pattern(form, case_sensitive=case_sensitive)
    if pattern is None:
        return False
    return bool(pattern.search(normalize_spaces(text)))


def forms_overlap(one: str, other: str) -> bool:
    """Do two surface forms refer to the same thing by containment?

    Whitespace is collapsed on both sides first. If EITHER side is short, the
    short one must appear in the other on WORD BOUNDARIES, case-sensitively —
    otherwise "TIG" would match "tight" and "ABS" would match "absolute".
    """
    a, b = collapse_whitespace(one), collapse_whitespace(other)
    if not a or not b:
        return False
    if a.casefold() == b.casefold():
        return True
    if is_short_form(a) or is_short_form(b):
        short, long_form = (a, b) if is_short_form(a) else (b, a)
        pattern = flexible_pattern(short, case_sensitive=True)
        return bool(pattern and pattern.search(long_form))
    af, bf = a.casefold(), b.casefold()
    return af in bf or bf in af
