"""The record-only own-name lint (2026-08-13).

The relationship prompt bans the subject's name from every description, and the
measured leak mode (5-subject corpus, 24/164 summaries) is the BARE name — the
summary says "Steelcraft" where the pinned subject is "Steelcraft, Inc." These
tests pin the matching band: suffix-stripped, case-insensitive, word-bounded,
and silent on the names too short to count without flooding the dump.
"""

from core.utils.subject_name_lint import count_own_name_hits


def test_bare_name_leak_is_counted_case_insensitively():
    assert (
        count_own_name_hits(
            "The manufacturer, formerly STEELCRAFT, makes doors. "
            "Steelcraft doors recur in headings.",
            "Steelcraft",
        )
        == 2
    )


def test_legal_suffix_is_stripped_so_the_bare_name_still_matches():
    assert (
        count_own_name_hits(
            "Steelcraft has served the industry for 90 years.",
            "Steelcraft, Inc.",
        )
        == 1
    )


def test_possessive_counts_but_longer_words_do_not():
    text = "Steelcraft's hollow metal doors, unlike steelcrafting hobbyists."
    assert count_own_name_hits(text, "Steelcraft") == 1


def test_clean_summary_counts_zero():
    assert (
        count_own_name_hits(
            "The manufacturer offers powder coating services.", "Steelcraft"
        )
        == 0
    )


def test_multi_word_name_matches_as_a_whole():
    text = "Parts are machined by Apex Machining for aerospace customers."
    assert count_own_name_hits(text, "Apex Machining Co") == 1
    # The head word alone is NOT the subject name; only the whole (stripped)
    # name counts, which is what keeps common head words from false-positives.
    assert count_own_name_hits("Apex quality is our promise.", "Apex Machining Co") == 0


def test_too_short_core_falls_back_to_the_unstripped_name():
    # "AB Co" strips to "AB" (< 3 chars), so the unstripped form is matched.
    assert count_own_name_hits("AB Co supplies fasteners.", "AB Co") == 1
    assert count_own_name_hits("Absolutely nothing here.", "AB Co") == 0


def test_empty_inputs_count_zero():
    assert count_own_name_hits("", "Steelcraft") == 0
    assert count_own_name_hits("some text", "") == 0
    assert count_own_name_hits("some text", "   ") == 0
