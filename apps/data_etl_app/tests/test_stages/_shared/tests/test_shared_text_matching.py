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
