"""The designation-token conservation vocabulary (2026-09-02, Phase B):
precision-first tiers — T1 letter+digit tokens, T2 ALL-CAPS/kind-word pairs,
T3 focal-adjacent digits — and the presence check the synthesis
under-enumeration retry keys on."""

from core.utils.designation_tokens import (
    designation_coverage,
    designation_tokens,
    missing_designations,
)


class TestCoreTier:
    def test_letter_digit_tokens_are_designations(self):
        tokens = designation_tokens("We stock UNS N07718 and grade A357 and J-2550 (1).")
        assert {"N07718", "A357", "J-2550"} <= tokens

    def test_plain_words_and_pure_numbers_are_not(self):
        tokens = designation_tokens("Fifty machines shipped in 2023 to 12 states.")
        assert tokens == set()

    def test_short_mixed_tokens_below_three_chars_are_ignored(self):
        assert "a1" not in {t.lower() for t in designation_tokens("see a1 hall")}


class TestPairTier:
    def test_all_caps_plus_number_is_one_designation(self):
        tokens = designation_tokens("Certified to ISO 9001 and AS 9100D standards.")
        assert "ISO 9001" in tokens

    def test_kind_word_plus_number_is_one_designation(self):
        tokens = designation_tokens("Available in Grade 630 and under Section 172.")
        assert {"Grade 630", "Section 172"} <= tokens

    def test_sentence_capitalized_word_plus_year_is_not(self):
        tokens = designation_tokens("Founded In 2023 the plant grew.")
        assert not any("2023" in t for t in tokens)

    def test_all_caps_plus_year_still_counts(self):
        # FEMA 320 (2021)-style ids: the ALL-CAPS head vouches for the pair
        tokens = designation_tokens("Meets FEMA 320 requirements.")
        assert "FEMA 320" in tokens


class TestFocalTier:
    def test_digits_after_focal_form_are_designations(self):
        tokens = designation_tokens(
            "Aluminum castings in 319, 356, and A357 alloys.", focal_form="Aluminum"
        )
        assert {"319", "356", "A357"} <= tokens

    def test_digits_far_from_focal_form_are_not(self):
        text = "Aluminum is stocked here. " + "x" * 100 + " Call 555 today."
        tokens = designation_tokens(text, focal_form="Aluminum")
        assert "555" not in tokens

    def test_years_near_focal_form_are_not(self):
        tokens = designation_tokens("Aluminum since 1985 and 2020.", focal_form="Aluminum")
        assert not ({"1985", "2020"} & tokens)


class TestConservation:
    ENTRIES = ["JET: J-2550 (1)", "JET: 1015VS (1)", "Certified to ISO 9001."]

    def test_conserving_synthesis_has_no_missing(self):
        synthesis = "The entries list JET models J-2550 and 1015VS; ISO 9001 is claimed."
        assert missing_designations(self.ENTRIES, synthesis, focal_form="JET") == set()

    def test_eliding_synthesis_reports_each_dropped_token(self):
        synthesis = "The entries list numerous JET model codes, among others."
        missing = missing_designations(self.ENTRIES, synthesis, focal_form="JET")
        assert {"J-2550", "1015VS", "ISO 9001"} <= missing

    def test_presence_is_case_insensitive_and_boundary_checked(self):
        assert missing_designations(["grade A357 alloy"], "cast in a357 form") == set()
        assert missing_designations(["grade A357 alloy"], "cast in A3571 form") == {"A357"}

    def test_coverage_counts_drive_the_comparator(self):
        present, demanded = designation_coverage(
            self.ENTRIES, "Only J-2550 is named.", focal_form="JET"
        )
        assert (present, demanded) == (1, 3)
