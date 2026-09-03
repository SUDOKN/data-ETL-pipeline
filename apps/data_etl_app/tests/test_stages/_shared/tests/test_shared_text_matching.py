"""The three matching traps this corpus sets, pinned once for every stage.

Each test names the trap, the direction it biases (false alarm vs false
credit), and a real string from the corpus that triggers it.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from _shared.text_matching import (  # noqa: E402
    collapse_whitespace,
    flexible_pattern,
    form_covers,
    forms_overlap,
    is_short_form,
    normalize_spaces,
    occurs_in,
)


# --- trap 1: non-breaking spaces mid-phrase (biases to FALSE ALARM) ---------

def test_nbsp_inside_a_phrase_still_matches():
    """steelcraft: "in accordance with ASTM D4585-D4585M-18" (843 NBSPs)."""
    text = "Condensation testing in accordance with ASTM D4585-D4585M-18"
    assert occurs_in("ASTM D4585", text)
    assert forms_overlap("ASTM D4585", "ASTM D4585-D4585M-18")


def test_space_normalization_preserves_length():
    """Offsets taken against normalized text must stay valid."""
    raw = "a b c　d"
    assert len(normalize_spaces(raw)) == len(raw)
    assert normalize_spaces(raw) == "a b c d"


# --- trap 2: whitespace runs inside phrases (biases to FALSE ALARM) ---------

def test_double_spaces_and_newlines_inside_a_phrase_still_match():
    """steelcraft A14 page: "Full  glass architectural entrance doors"."""
    assert occurs_in("Full glass architectural entrance doors",
                     "applications\nFull  glass architectural entrance doors.")
    assert occurs_in("steel doors and frames", "we make steel doors\nand frames")


def test_collapse_whitespace_is_for_comparison_not_offsets():
    assert collapse_whitespace("a  b\n\nc") == "a b c"


# --- trap 3: short-form substrings (biases to FALSE CREDIT) ----------------

def test_short_forms_do_not_match_inside_longer_words():
    """The brute `Lead` bug: 52 hits, 0 real. Same shape as TIG in "tight"."""
    assert not forms_overlap("TIG", "tight tolerances")
    assert not forms_overlap("ABS", "absolute positioning")
    assert not occurs_in("TIG", "tight tolerances")
    assert forms_overlap("TIG", "TIG welding")
    assert occurs_in("CMM", "CMM inspection report")


def test_short_forms_are_case_sensitive_even_when_asked_otherwise():
    """`is_short_form` overrides the caller: "abs" must not credit "ABS"."""
    assert is_short_form("ABS") and not is_short_form("ABSOLUTE")
    assert not occurs_in("ABS", "abs values", case_sensitive=False)
    assert occurs_in("ABS", "ABS plastic", case_sensitive=False)


# --- trap 5: containment in the wrong direction (biases to FALSE CREDIT) ---

def test_a_returned_fragment_does_not_cover_the_designation():
    """The 2026-08-28 finding: every one of these was an AWARDED credit that a
    blind judge overturned. The bare word is not the designation."""
    assert not form_covers("titanium fusion cages", "titanium")
    assert not form_covers("steel front panel for speaker", "steel")
    assert not form_covers("Vespel Parts & Shapes", "parts")
    assert not form_covers("laser cutting", "Cutting")
    assert not form_covers("Semiconductor Manufacturing", "manufacturing")
    assert not form_covers("sheet metal", "metal")
    assert not form_covers("metal industries", "industries")
    assert not form_covers("earth science & engineering", "engineering")
    assert not form_covers("Engine Mounting Systems", "Engine Mounting System")


def test_the_returned_form_may_be_longer_than_the_designation():
    """The direction that IS a find: the model returned at least the thing."""
    assert form_covers("stainless steel", "304 stainless steel bar")
    assert form_covers("brazing", "vacuum brazing")  # the deliberate shadowing
    assert form_covers("ASTM D4585", "ASTM D4585-D4585M-18")
    assert form_covers("laser cutting", "CNC laser cutting")


def test_the_left_word_boundary_holds_above_the_short_form_threshold():
    """"Stem" is four characters, so the trap-4 guard never saw it, and a lab
    microwave scored a point for a valve stem. The LEFT edge is what stops it;
    the right edge must stay open for inflections, which is why this is not
    simply `occurs_in`."""
    assert not form_covers("Stem", "GRT- S5 MICROWAVE SYSTEM")
    assert form_covers("Stem", "valve stem")
    assert not form_covers("Nylon", "Nickel Alloys")
    assert form_covers("shear", "Shearing")            # right edge open
    assert form_covers("press brake", "CNC 7 Axis Press Brakes")
    assert not form_covers("EMS", "quality assurance systems")  # trap 4 intact


def test_nesting_that_form_covers_cannot_settle_is_left_to_the_reader():
    """Documented residue, not a bug: these still credit, and only a human can
    say they are different things."""
    assert form_covers("steel", "stainless steel")
    assert form_covers("Miller syncrowave 350", "Miller Syncrowave 350 LX")


def test_forms_overlap_is_the_symmetric_question_and_not_recall_credit():
    assert forms_overlap("titanium fusion cages", "titanium")   # symmetric: yes
    assert not form_covers("titanium fusion cages", "titanium")  # credit: no


# --- ordinary behaviour still works ----------------------------------------

def test_long_forms_keep_containment_in_both_directions():
    assert forms_overlap("stainless steel", "304 stainless steel bar")
    assert forms_overlap("304 stainless steel bar", "stainless steel")
    assert not forms_overlap("stainless steel", "carbon steel")


def test_word_boundaries_hold_for_long_forms_too():
    assert occurs_in("steel", "steel doors")
    assert not occurs_in("steel", "steelcraft doors")


def test_empty_and_degenerate_inputs_are_safe():
    assert flexible_pattern("   ", case_sensitive=True) is None
    assert not forms_overlap("", "anything")
    assert not occurs_in("", "anything")


# --- Trap 3: typographic hyphens (added 2026-08-27) ---------------------------


def test_non_breaking_hyphen_inside_a_word_still_matches():
    """med-tekinc prints its ONLY capability as "heat‑treating" on two lines and
    "heat-treating" on three others. Without this, a faithful ASCII echo scores
    as a miss on exactly the lines that matter most."""
    text = "we take on huge heat‑treating jobs for commercial clients"
    assert occurs_in("heat-treating", text)
    assert occurs_in("heat‑treating", text)


def test_soft_hyphen_is_invisible_so_a_faithful_echo_omits_it():
    """agstech prints "X­ray" and "high­resolution". The soft hyphen renders as
    nothing, so no model will ever echo it back — the matcher has to drop it."""
    text = "opportunities in optics for high­resolution X­ray applications"
    assert occurs_in("X-ray", text)
    assert occurs_in("high-resolution", text)
    # Accepted residual: the fully de-hyphenated echo is still a miss. Rescuing
    # it would cost the length-preservation guarantee for 3 corpus occurrences.
    assert not occurs_in("highresolution", text)


def test_en_and_em_dashes_are_NOT_folded_to_hyphens():
    """Deliberate. 1,046 of 1,094 en dashes in the corpus are whitespace-adjacent
    separators; folding them would manufacture false credit rather than rescue a
    faithful match."""
    assert not occurs_in("steel-and", "we work steel—and aluminum")
    assert not occurs_in("2020-2024", "the 2020–2024 expansion")


def test_hyphen_normalization_still_preserves_length():
    """The offset contract holds: hyphens map one character to one character."""
    text = "pre‑treatment for RO"
    assert len(normalize_spaces(text)) == len(text)
    assert normalize_spaces(text).index("treatment") == text.index("treatment")
