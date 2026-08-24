"""The record-only focal-form lint (2026-08-23).

The synthesis hold checks the ``record_id`` echo and nothing about the entity, so
a swapped name reaches grounding unremarked. Run 20260823T200044 produced one:
focal form and sole snippet both read ``FE Series Double-Egress Frames``, the
paragraph described ``DE Series Double-Egress Frames``.

These tests pin the band that makes the check usable rather than noisy — it fires
on proper-name-shaped focal forms only, and accepts any member form of the group
as satisfying it. Measured over that day's A/B, the band leaves 2–3 flagged rows
per 700 entity-shaped records.
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
