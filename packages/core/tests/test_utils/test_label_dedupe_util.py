from core.utils.label_dedupe_util import (
    dedupe_case_insensitive,
    dedupe_equivalent_keywords,
)


def test_case_variants_collapse_to_one():
    """The Tooling/tooling pair from the 2026-08-11 baseline: a case-sensitive
    set union kept both."""
    assert dedupe_case_insensitive({"Tooling", "tooling"}) == {"Tooling"}


def test_case_dedupe_keeps_distinct_labels():
    labels = {"Tool & Die", "Tool & Die Services", "Welding"}
    assert dedupe_case_insensitive(labels) == labels


def test_singular_and_plural_collapse_to_singular():
    """The baseline products list carried six such pairs."""
    assert dedupe_equivalent_keywords({"metal stampings", "metal stamping"}) == {
        "metal stamping"
    }
    assert dedupe_equivalent_keywords({"chassis components", "chassis component"}) == {
        "chassis component"
    }


def test_short_and_double_s_words_are_not_treated_as_plurals():
    """"gas" and "press" end in s without being plurals; stripping them would
    collide unrelated keywords."""
    keywords = {"gas turbine", "press brake", "presses"}
    assert dedupe_equivalent_keywords(keywords) == keywords


def test_distinct_keywords_survive():
    keywords = {"welded assembly", "fabricated part", "metal stamping"}
    assert dedupe_equivalent_keywords(keywords) == keywords


def test_keyword_winner_is_deterministic_regardless_of_input_order():
    variants = ["metal stampings", "Metal Stamping", "metal stamping"]
    # Shortest wins; the length tie between the two singulars breaks
    # lexicographically, and uppercase sorts first.
    assert dedupe_equivalent_keywords(variants) == {"Metal Stamping"}
    assert dedupe_equivalent_keywords(reversed(variants)) == {"Metal Stamping"}
