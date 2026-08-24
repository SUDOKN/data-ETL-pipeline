"""The record-only focal-form lint (2026-08-23).

The synthesis hold checks the ``record_id`` echo and nothing about the entity, so
a swapped name reaches grounding unremarked. Run 20260823T200044 produced one:
focal form and sole snippet both read ``FE Series Double-Egress Frames``, the
paragraph described ``DE Series Double-Egress Frames``.

These tests pin the band that makes the check usable rather than noisy — it fires
on proper-name-shaped focal forms only, accepts any member form of the group as
satisfying it, and (since 2026-08-24) accepts the form re-inflected into a
sentence rather than quoted. Measured over three runs, the band leaves exactly
one flagged row per 700 entity-shaped records.
"""

from core.utils.focal_form_lint import focal_form_absent, is_entity_shaped


def test_the_measured_entity_swap_is_flagged():
    assert focal_form_absent(
        "DE Series Double-Egress Frames are presented as a product line by the "
        "manufacturer, with their name appearing as a subcategory name on the "
        "main frames product listing page.",
        "FE Series Double-Egress Frames",
        ["FE Series Double-Egress Frames"],
    )


def test_a_correct_synthesis_is_not_flagged():
    assert not focal_form_absent(
        "FE Series Double-Egress Frames are introduced as a product series "
        "heading on the main frames product listing page.",
        "FE Series Double-Egress Frames",
        ["FE Series Double-Egress Frames"],
    )


def test_any_member_form_satisfies_the_check():
    # The group holds every casing the scan found; the synthesis is told to use
    # the site's spelling, so a member form counts as the entity being named.
    assert not focal_form_absent(
        "Paladin™ PW Series Flush Doors & Frames are listed as a product line.",
        "Paladin PW Series flush doors and frames",
        [
            "Paladin PW Series flush doors and frames",
            "Paladin™ PW Series Flush Doors & Frames",
        ],
    )


def test_punctuation_and_case_do_not_matter():
    assert not focal_form_absent(
        "the fe series double egress frames are offered by the manufacturer.",
        "FE Series Double-Egress Frames",
        [],
    )


def test_a_clause_shaped_focal_form_is_never_flagged():
    # The naive check's false positives all live here: correct writing
    # re-inflects a bullet lifted whole, and that is not a defect.
    assert not focal_form_absent(
        "The manufacturer factory prepares frames for field-installed silencers.",
        "Factory prepared for field-installed silencers",
        ["Factory prepared for field-installed silencers"],
    )


def test_a_single_token_focal_form_is_never_flagged():
    assert not focal_form_absent(
        "The manufacturer offers polishing as a finishing step.", "Polished", []
    )


def test_entity_shape_needs_a_capital_or_digit_past_the_first_token():
    assert is_entity_shaped("FE Series Double-Egress Frames")
    assert is_entity_shaped("Paladin PW Series")
    assert is_entity_shaped("Falcon SZ Series")
    assert not is_entity_shaped("Aluminum")
    assert not is_entity_shaped("stainless steel")
    assert not is_entity_shaped("Surface finishing and quality assurance")
    assert not is_entity_shaped(
        "Factory-applied baked-on rust inhibiting primer meets ANSI A250.10-2011"
    )


def test_an_empty_synthesis_is_never_flagged():
    assert not focal_form_absent("", "FE Series Double-Egress Frames", [])
    assert not focal_form_absent(None, "FE Series Double-Egress Frames", [])  # type: ignore[arg-type]


# --- the re-inflection path, added 2026-08-24 -------------------------------------
#
# Retiring the own-name ban changed how the stage writes: the old statics quoted
# the focal form, the new ones write it into the sentence's grammar. Every case
# below is a real row from run 20260824T002404 that the contiguous-only check
# flagged, and none of them is a defect.


def test_an_ampersand_written_as_the_word_and_is_not_flagged():
    assert not focal_form_absent(
        "Alec Model performs precision fixturing and machining as part of its "
        "process for high-precision CNC machined aluminum baseplates.",
        "Precision Fixturing & Machining",
        ["Precision Fixturing & Machining"],
    )


def test_a_comma_written_as_the_word_and_is_not_flagged():
    assert not focal_form_absent(
        "Alec Model notes that threaded inserts are required for M4 and M6 fasteners.",
        "M4, M6 fasteners",
        ["M4, M6 fasteners"],
    )


def test_a_re_ordered_form_is_not_flagged():
    assert not focal_form_absent(
        "Steelcraft offers an optional mineral board core for its hurricane "
        "impact doors, described as rigid.",
        "Mineral Board (optional)",
        ["Mineral Board (optional)"],
    )


def test_words_inserted_into_the_form_do_not_flag_it():
    assert not focal_form_absent(
        "Steelcraft describes its Hurricane products as having uniquely engineered designs.",
        "Steelcraft Hurricane products",
        ["Steelcraft Hurricane products"],
    )


def test_a_plural_inflection_is_folded():
    assert not focal_form_absent(
        "Alec Model conducts inspection and assembly fit tests as the fourth step.",
        "Inspection & Assembly Fit Test",
        ["Inspection & Assembly Fit Test"],
    )


def test_the_swap_is_still_caught_when_only_one_token_differs():
    # The whole point of the relaxation: it must not become so loose that the
    # discriminating token going missing stops registering.
    assert focal_form_absent(
        "DE Series Double-Egress Frames are offered by Steelcraft as a product "
        "line, listed as a subcategory on the main frames product listing page.",
        "FE Series Double-Egress Frames",
        ["FE Series Double-Egress Frames"],
    )


def test_a_short_token_is_not_plural_folded():
    # "MS" must not fold to "M" — a two-character token is usually the part of a
    # product name that tells two entities apart.
    assert focal_form_absent(
        "The M Series frames are offered by the manufacturer.", "MS Series frames", []
    )


def test_tokens_must_match_whole_not_as_substrings():
    # "DE" appearing inside "Double" must not satisfy a focal form of "DE Series".
    assert focal_form_absent(
        "Double-Egress Frames are listed by the manufacturer.", "DE Series", []
    )


def test_a_possessive_focal_form_is_not_flagged():
    # "Steelcraft's" splits into "steelcraft" plus a bare "s" that no synthesis
    # contains. Run 20260824T010654 flagged three rows on this alone, once the
    # prose moved from "Steelcraft's Express Stock program" to "Steelcraft
    # operates the Express Stock program".
    assert not focal_form_absent(
        "Steelcraft operates the Express Stock program, which offers 48-hour shipping.",
        "Steelcraft's Express Stock program",
        ["Steelcraft's Express Stock program"],
    )
