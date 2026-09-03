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

  5. CONTAINMENT IN THE WRONG DIRECTION. Asking "do these two forms overlap?"
     accepts a RETURNED fragment of an EXPECTED designation as a find: the bare
     word "titanium" credits the eval entry "titanium fusion cages", "metal"
     credits "sheet metal", "Cutting" credits "laser cutting". Recall credit is
     directional and must be asked directionally -- did the model return AT
     LEAST the designation? -- so use `form_covers`, never `forms_overlap`.
     Measured 2026-08-28 over 410 blind spot-checks of AWARDED credits in the
     search harness: 33 false, 8.0% overall and 19.1% for products, which is
     1.7 points of overall confirmed recall (95.4% -> 93.7%). Long forms also
     matched with no LEFT word boundary, so "Stem" was credited by "MICROWAVE
     SYSTEM" -- the trap-4 guard existed but stopped at 3 characters. The RIGHT
     edge stays open on purpose, so "shear" still credits "Shearing".

Traps 1-3 bias toward FALSE ALARMS (correct output judged fabricated or
missing); traps 4 and 5 bias toward FALSE CREDIT (junk judged correct). A stage
instrument that hits either direction reports numbers that look plausible and
are wrong, which is worse than reporting nothing. Trap 5 is the more dangerous
of the two: false credit only ever FLATTERS the stage, so nothing downstream
looks broken enough to investigate.

Found and fixed in the search-stage harness on 2026-08-26, where trap 1 had
already produced a false RED verdict on correct output before it was caught.
Extracted here so the other stage instruments do not each rediscover it.

Pure stdlib on purpose — import it from any stage without dragging in the
pipeline packages:

    import sys; from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from _shared.text_matching import normalize_spaces, occurs_in, form_covers
"""

from __future__ import annotations

import re
from functools import lru_cache
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


@lru_cache(maxsize=100_000)
def covering_pattern(form: str) -> Optional[re.Pattern[str]]:
    """The ONE pattern `form_covers` matches with, exposed so callers that
    precompile per acceptable form (the diff-set exporter) cannot drift from it.

    Short forms keep both boundaries and case sensitivity (trap 4); longer forms
    are anchored on the left only (see `_left_anchored_pattern`).

    Memoized: recall scoring calls this once per (acceptable form, returned
    form) pair — millions of times over a 20-subject run — and `re`'s own cache
    holds 512 patterns, far short of this corpus's distinct forms.
    """
    if is_short_form(collapse_whitespace(form)):
        return flexible_pattern(form, case_sensitive=True)
    return _left_anchored_pattern(form)


def _left_anchored_pattern(form: str) -> Optional[re.Pattern[str]]:
    """`flexible_pattern` with the RIGHT boundary dropped, case-insensitive.

    The open right edge is a DELIBERATE leniency, pinned by tests: a returned
    form may inflect or pluralize the expected designation ("shear" ->
    "Shearing", "press brake" -> "CNC 7 Axis Press Brakes"). The left edge is
    NOT negotiable — without it "Stem" matches inside "MICROWAVE SYSTEM".
    """
    stripped = normalize_spaces(form).strip()
    if not stripped:
        return None
    body = r"\s+".join(re.escape(part) for part in stripped.split())
    left = r"(?<!\w)" if re.match(r"\w", stripped[0]) else ""
    return re.compile(left + body, re.IGNORECASE)


def form_covers(expected: str, returned: str) -> bool:
    """Did ``returned`` deliver AT LEAST the designation ``expected`` (trap 5)?

    DIRECTIONAL on purpose, and that is the whole point: the expected
    designation must occur inside what was RETURNED, never the reverse. "304
    stainless steel bar" covers "stainless steel"; "steel" does NOT cover
    "steel front panel for speaker".

    Short forms keep the trap-4 rule (both boundaries, case-sensitive: TIG must
    not match "tight"). Longer forms are anchored on the LEFT only, so
    inflections still credit — see `_left_anchored_pattern` for why that
    asymmetry is deliberate rather than an oversight.

    NOT sufficient on its own. Two designations can nest and still be different
    things ("steel" inside "stainless steel", "Miller syncrowave 350" inside
    "Miller Syncrowave 350 LX", which the site lists as a separate machine).
    That residue is a judgment about the world, not about strings, and belongs
    to the reader; this function only refuses the mechanical mistakes.
    """
    a, b = collapse_whitespace(expected), collapse_whitespace(returned)
    if not a or not b:
        return False
    if a.casefold() == b.casefold():
        return True
    pattern = covering_pattern(a)
    return bool(pattern and pattern.search(normalize_spaces(b)))


def forms_overlap(one: str, other: str) -> bool:
    """Symmetric: does EITHER form cover the other?

    NEVER use this to award recall credit — that is trap 5, and `form_covers`
    is the primitive for it. Kept for the genuinely symmetric questions (are
    these two forms plausibly the same designation, in either direction), and
    now word-boundary-safe in both directions.
    """
    return form_covers(one, other) or form_covers(other, one)
