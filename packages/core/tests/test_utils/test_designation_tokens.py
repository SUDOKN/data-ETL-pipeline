"""The focal-form designation rule (2026-09-10, D2/D14/D16): a record OWNS
the designations inside its focal form and those written directly beside
it; everything attached to another item — a sibling form, a Capitalized
name, an ordinary word's own code, the next list item — is not its demand.
Fixtures are the trigger-scope survey's (``docs_local/trigger_scope_survey_
20260910/README.md``) and the retry memo's Specimens A–D."""

from core.utils.designation_tokens import (
    designation_coverage,
    designation_shaped_tokens,
    designation_tokens,
    missing_designations,
    own_tokens,
)


def demand(text: str, focal: str, siblings: list[str] | None = None) -> set[str]:
    return designation_tokens(text, focal_form=focal, sibling_forms=siblings or [])


class TestOwnTokens:
    def test_two_character_letter_digit_tokens_count_inside_the_focal_form(self):
        assert own_tokens("D2") == {"D2"}
        assert own_tokens("Tool Steels") == set()

    def test_head_pairs_inside_the_focal_form(self):
        assert own_tokens("UNS N06625") == {"UNS N06625"}
        assert own_tokens("GRADE 4130") == {"GRADE 4130"}
        assert own_tokens("LOW ALLOY 4130") == {"ALLOY 4130"}

    def test_punctuation_breaks_a_pair(self):
        assert own_tokens("SAE - 4130") == {"4130"}

    def test_a_year_tail_counts_only_behind_a_head(self):
        assert own_tokens("ISO 9001:2015") == {"ISO 9001"}
        assert own_tokens("Founded 2015") == set()


class TestAfterSpan:
    def test_digits_after_the_focal_form_are_its_own(self):
        assert demand("Aluminum castings in 319, 356, and A357 alloys", "Aluminum") == {
            "319",
            "356",
            "A357",
        }

    def test_parenthesised_grades_belong_to_the_form_before_them(self):
        assert demand("Tool Steels (H13, D2)", "Tool Steels") == {"H13", "D2"}
        # the two-character D2 counts once H13 opened the span
        assert demand("Tool Steels (H13, D2)", "Steels") == {"H13", "D2"}

    def test_a_variant_suffix_the_focal_occurrence_ends_inside(self):
        assert demand("JET: JTM-1050EVS/460 (1)", "JTM-1050EVS") == {"JTM-1050EVS", "460"}

    def test_a_colon_attached_to_the_focal_form_is_kept(self):
        assert demand("## UNS N06625: B446 AMS 5666", "UNS N06625") == {
            "UNS N06625",
            "B446",
            "AMS 5666",
        }

    def test_a_later_colon_cuts_and_a_sibling_owns_its_own_code(self):
        text = "LOW ALLOY 4130: ASTM A29: Certified: ISO 9001: 2015"
        assert demand(text, "LOW ALLOY 4130", ["ASTM A29"]) == {"ALLOY 4130"}

    def test_a_capitalized_word_names_another_item(self):
        assert demand("MATSUURA MC-600 Machining Center", "Machining") == set()

    def test_a_peer_run_of_designations_gets_no_span(self):
        text = "8620H 414020MnCr5 18NiCrMo4 En19 42CrMo4F5"
        assert demand(text, "18NiCrMo4") == {"18NiCrMo4"}

    def test_a_spaced_colon_run_is_own_only(self):
        assert demand("# GRADE 4130 : 4140 : 4145 TUBE", "GRADE 4130") == {"GRADE 4130"}

    def test_a_list_item_focal_form_stops_at_the_comma(self):
        assert demand("Wire EDM, 2D/3D Laser Cutting", "Wire EDM") == set()

    def test_a_prefix_designation_belongs_to_the_next_item(self):
        # 2D/3D prefixes "Laser Cutting": that record's own, not the preceding item's
        assert demand("Wire EDM, 2D/3D Laser Cutting", "Laser Cutting") == {"2D/3D"}

    def test_a_head_pair_after_the_focal_form(self):
        assert demand("doors above STC 45", "doors") == {"STC 45"}


class TestSiblingRule:
    def test_the_first_sibling_form_ends_the_span(self):
        text = "Inconel 625 and Inconel 718"
        assert demand(text, "Inconel") == {"625", "718"}
        assert demand(text, "Inconel", ["Inconel 625", "Inconel 718"]) == set()

    def test_a_sibling_that_contains_the_focal_occurrence_owns_both_sides(self):
        assert demand("Steel Doors 101", "Doors", ["Steel Doors 101"]) == set()
        assert demand("JET: JTM-1050EVS/460-CNC (1)", "JTM-1050EVS", ["JTM-1050EVS/460-CNC"]) == {
            "JTM-1050EVS"
        }


class TestBeforeSpan:
    def test_codes_and_head_pairs_before_the_focal_form(self):
        assert demand("Grade 5 titanium and Grade 2 titanium bar", "titanium") == {
            "Grade 5",
            "Grade 2",
        }
        assert demand("Type 316 stainless plate", "stainless") == {"Type 316"}
        assert demand("A60 galvannealed steel", "steel") == {"A60"}

    def test_bare_digits_before_the_focal_form_are_never_demanded(self):
        assert demand("The 235 machines in our shops", "machines") == set()

    def test_a_clause_verb_stops_the_walk(self):
        assert demand("A725 is also used for offshore and marine applications", "marine") == set()

    def test_no_before_span_when_the_focal_form_opens_a_longer_name(self):
        assert demand("MATSUURA MC-600 Machining Center", "Machining", ["MC-600 Machining Center"]) == set()


class TestNeverDesignations:
    def test_measurements_counts_and_sequences(self):
        assert demand("CNC Lathes: SHIMADA (11)", "CNC Lathes") == set()
        assert demand("Stadco Precision: 937 878 0911", "Stadco Precision") == set()
        assert demand("forgings weighing up to 70,000 pounds, lengths to 57 feet", "forgings") == set()
        assert demand("tolerances to 1/16th of an inch", "tolerances") == set()
        assert demand("assemblies of over 100 individual parts", "assemblies") == set()
        assert demand("Sheet up to 05mm and 1mm thick, Cary NC 27513", "Sheet") == set()

    def test_url_continuations(self):
        assert demand("see P65Warnings.ca.gov for details on frames", "frames") == set()

    def test_specimen_a_the_table_row(self):
        # Tool Steels' own grades, not the neighbouring Inconel cell
        text = "Tool Steels (H13, D2) | Inconel 625 | Ti 6-4"
        assert demand(text, "Tool Steels", ["Inconel 625"]) == {"H13", "D2"}


class TestPresenceAndCoverage:
    def test_presence_tolerates_spacing_and_hyphenation(self):
        assert missing_designations(["ISO14001 certified"], "holds ISO 14001", focal_form="ISO14001") == set()
        assert missing_designations(["ISO 9001"], "ISO9001:2015 certified", focal_form="ISO 9001") == set()
        assert missing_designations(["6061-T6 bar"], "works 6061 T6 bar", focal_form="6061-T6") == set()

    def test_presence_is_boundary_checked(self):
        assert missing_designations(["A357 alloy"], "cast in a357 form", focal_form="A357") == set()
        assert missing_designations(["A357 alloy"], "cast in A3571 form", focal_form="A357") == {"A357"}

    def test_own_designations_are_demanded_even_when_no_snippet_repeats_them(self):
        assert missing_designations(["the alloy ships daily"], "an alloy", focal_form="UNS N06625") == {
            "UNS N06625"
        }

    def test_coverage_counts_drive_the_comparator(self):
        snippets = ["Aluminum castings in 319, 356, and A357 alloys"]
        assert designation_coverage(snippets, "Only A357 is named.", focal_form="Aluminum") == (1, 3)
        assert designation_coverage(snippets, "319, 356 and A357.", focal_form="Aluminum") == (3, 3)


class TestRawShapes:
    """``designation_shaped_tokens`` is the nomination vocabulary, owner-blind."""

    def test_letter_digit_tokens_and_pairs(self):
        tokens = designation_shaped_tokens("We stock UNS N07718, grade A357, J-2550 and ISO 9001.")
        assert {"N07718", "A357", "J-2550", "ISO 9001", "UNS N07718"} <= tokens

    def test_plain_words_and_years_are_not(self):
        assert designation_shaped_tokens("Fifty machines shipped in 2023 to 12 states.") == set()
        assert not any("2023" in t for t in designation_shaped_tokens("Founded In 2023 the plant grew."))
