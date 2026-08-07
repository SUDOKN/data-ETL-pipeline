from pure_utils.text_normalize import normalize_scraped_text


def test_dashes_folded_to_ascii_hyphen():
    # en, em, figure dash, and minus sign all become ASCII hyphen
    assert normalize_scraped_text("Laser \u2013 Turret Press") == "Laser - Turret Press"
    assert normalize_scraped_text("a \u2014 b") == "a - b"
    assert normalize_scraped_text("a \u2012 b") == "a - b"
    assert normalize_scraped_text("a \u2212 b") == "a - b"


def test_more_dashes_folded():
    # hyphen, non-breaking hyphen, horizontal bar, small/fullwidth forms
    for dash in (
        "\u2010",
        "\u2011",
        "\u2015",
        "\u2e3a",
        "\u2e3b",
        "\ufe58",
        "\ufe63",
        "\uff0d",
    ):
        assert normalize_scraped_text(f"a{dash}b") == "a-b"


def test_soft_hyphen_removed():
    # soft hyphen is invisible; must be stripped, not folded to a visible hyphen
    assert normalize_scraped_text("man\u00adufacturing") == "manufacturing"


def test_smart_quotes_folded():
    assert normalize_scraped_text("\u2018hi\u2019") == "'hi'"
    assert normalize_scraped_text("\u201chi\u201d") == '"hi"'
    assert normalize_scraped_text("Anchor\u2019s core") == "Anchor's core"


def test_primes_and_guillemets_folded():
    # prime/double-prime (feet/inches in manufacturing text) and guillemets
    assert normalize_scraped_text("10\u2032") == "10'"
    assert normalize_scraped_text("48\u2033") == '48"'
    assert normalize_scraped_text("\u00abhi\u00bb") == '"hi"'
    assert normalize_scraped_text("\u2039hi\u203a") == "'hi'"


def test_ellipsis_expanded():
    assert normalize_scraped_text("wait\u2026") == "wait..."


def test_spaces_and_zero_width_removed():
    assert normalize_scraped_text("a\u00a0b") == "a b"
    assert normalize_scraped_text("a\u202fb") == "a b"
    assert normalize_scraped_text("a\u200bb") == "ab"
    assert normalize_scraped_text("\ufeffhello") == "hello"


def test_more_unicode_spaces_folded():
    # en/em/thin/hair spaces, ideographic space, medium math space, ogham space
    for sp in ("\u2000", "\u2003", "\u2009", "\u200a", "\u3000", "\u205f", "\u1680"):
        assert normalize_scraped_text(f"a{sp}b") == "a b"


def test_word_joiner_removed():
    assert normalize_scraped_text("a\u2060b") == "ab"


def test_line_separators_become_newline():
    assert normalize_scraped_text("a\u2028b") == "a\nb"
    assert normalize_scraped_text("a\u2029b") == "a\nb"


def test_control_chars_stripped_but_whitespace_kept():
    assert normalize_scraped_text("a\u0013b") == "ab"
    assert normalize_scraped_text("line1\nline2\tcol") == "line1\nline2\tcol"


def test_idempotent():
    raw = (
        "Laser \u2013 Turret\u00a0Press \u201cA\u201d\u2026\u200b man\u00aduf 48\u2033"
    )
    once = normalize_scraped_text(raw)
    assert normalize_scraped_text(once) == once


def test_plain_ascii_unchanged():
    text = "Over 60 presses\n2000 ton to 110 ton"
    assert normalize_scraped_text(text) == text
