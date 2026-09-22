"""Phase 2.1 of pipeline v3 (PIPELINE_V3_PLAN.md D9–D11): the normalizer that IS
the grouper, its guards, its version tripwire, and the group id.

The dry run behind the rules (2026-08-21, 4,157 real (field, phrase) pairs from
the dump corpus; plan appendix E): L0 162 multi-member groups, +L1 dictionary
189, +guarded fallback 191, +L2 (process/material) 198 — zero wrong merges at
L0/L1/fallback, one debatable L2 merge (injection molds ↔ Injection Molding).
"""

import hashlib
import re

import pytest
from hypothesis import assume, given, settings
from hypothesis import strategies as st

from core.utils import form_normalizer
from core.utils.form_normalizer import (
    NORMALIZER_VERSION,
    NormalizerEnvironmentError,
    is_code_token,
    l0_tokens,
    normalize,
)

# ---------------------------------------------------------------------------
# Golden corpus + digest: the version tripwire. Change a rule, the fallback,
# or the lemmatizer pin and this digest moves — bump NORMALIZER_VERSION and
# re-record it HERE, deliberately, in the same commit.
# ---------------------------------------------------------------------------

GOLDEN_CORPUS = [
    # L0
    "Fire Rated", "fire-rated", "FIRE RATED", "Paladin™ PW Series", "Paladin PW Series",
    "Doors & Frames", "doors and frames", "  Stainless   Steel ", "CNC/Manual Machining",
    "5-Axis CNC Machining", "(Type II) anodizing", "Type II anodizing",
    # L1 dictionary
    "assemblies", "assembly", "Plastics", "plastic", "medical devices", "medical device",
    "batch deliveries", "batch delivery", "steering columns", "steering column",
    "electrical codes", "Brass", "brass", "bras", "stainless", "series", "chassis",
    "analysis", "gas", "press", "presses", "glass", "bearings", "bearing", "housings",
    # guarded fallback (OOV plurals)
    "metal stampings", "metal stamping", "baseplates", "counterbores", "helicoils",
    "abs", "ABS", "as", "hs", "continuous", "status", "Texas", "texas", "Kansas",
    # code-token guard
    "6061-T6 aluminum", "ISO9001", "AccuGrips", "AccuGrip", "iPhones", "CNCs", "OEMs",
    "Paladin", "paladins", "HVAC systems",
    # L0 letter<->digit break (NORMALIZER_VERSION 2). Each pair must share a key;
    # the ISO edition pairs must NOT — see test_letter_digit_break_* below.
    "ISO 9001", "AS9100", "AS 9100", "IATF16949", "IATF 16949", "DL-95", "DL95",
    "SLM500", "SLM 500", "DAINICHI DLX-75A", "DAINICHI DLX75A", "TS16949", "TS 16949",
    "ICC500-2014", "ICC 500-2014", "3.3mm", "3.3 mm",
    "ISO 9001:2015", "ISO9001:2015", "ISO 9001:2000",
    # L2 candidates (keyed with verb_fold=False here; the L2 corpus is below)
    "CNC milled", "CNC milling", "Polished", "Polishing", "surface finishing",
    "injection molds", "Injection Molding", "mounting brackets", "cutting", "coatings",
    # Spanish passes through
    "productos", "instalador de molduras", "soluciones completas de HVAC",
    # edges
    "", "   ", "™", "-", "&", "a", "s", "ss",
]
GOLDEN_CORPUS_L2 = [
    "CNC milled", "CNC milling", "CNC mills", "Polished", "Polishing", "polish",
    "surface finish", "surface finishing", "3D Printing", "metal stampings",
    "bearings", "bearing", "housings", "housing", "cutting", "cut", "coatings", "coating",
    "injection molds", "Injection Molding", "mounting brackets", "tested", "testing",
    "machined", "machining", "Blasted", "blast", "Labeling",
]


def _digest(pairs) -> str:
    blob = "\n".join(f"{form}\t{key}" for form, key in pairs).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()


# Recorded for NORMALIZER_VERSION == "2" with lemminflect 0.2.3.
# v1 -> v2: L0 gained the letter<->digit word break. Measured over the
# 20-subject corpus (15,208 distinct real forms, run 20260829T022413) it merges
# 17 groups and produces ZERO wrong merges.
GOLDEN_DIGESTS = {
    "2": {
        "l1": "cf2ad77200b2b4f0777ae3b260a680d2eefe1c02db4b55413f51eb52437d90d2",
        "l2": "611123babfe6affb45394d7861aabf8af3df24b5955e44a86a4de9df2e3555ee",
    }
}


def test_golden_digest_is_the_version_tripwire():
    l1 = _digest((f, normalize(f)) for f in GOLDEN_CORPUS)
    l2 = _digest((f, normalize(f, verb_fold=True)) for f in GOLDEN_CORPUS_L2)
    recorded = GOLDEN_DIGESTS.get(NORMALIZER_VERSION)
    assert recorded is not None, (
        f"no golden digest recorded for NORMALIZER_VERSION={NORMALIZER_VERSION!r}; "
        f"record l1={l1} l2={l2}"
    )
    assert (l1, l2) == (recorded["l1"], recorded["l2"]), (
        "normalizer behaviour changed without a version bump: "
        f"l1={l1} l2={l2} (recorded {recorded}). If the change is intended, bump "
        "NORMALIZER_VERSION and record the new digests under it."
    )


def test_lemmatizer_pin_is_asserted():
    assert form_normalizer.LEMMATIZER_PINNED_VERSION == "0.2.3"
    form_normalizer.assert_lemmatizer_pinned()  # the installed one matches
    real = form_normalizer.lemminflect.__version__
    try:
        form_normalizer.lemminflect.__version__ = "9.9.9"
        with pytest.raises(NormalizerEnvironmentError):
            form_normalizer.assert_lemmatizer_pinned()
    finally:
        form_normalizer.lemminflect.__version__ = real


# ---------------------------------------------------------------------------
# L0
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "form, expected",
    [
        ("Fire Rated", ["Fire", "Rated"]),
        ("fire-rated", ["fire", "rated"]),
        ("Paladin™ PW Series", ["Paladin", "PW", "Series"]),
        ("Doors & Frames", ["Doors", "and", "Frames"]),
        ("CNC/Manual Machining", ["CNC", "Manual", "Machining"]),
        ("  Stainless   Steel ", ["Stainless", "Steel"]),
        ("(Type II) anodizing,", ["Type", "II", "anodizing"]),
        ("5–axis — machining", ["5", "axis", "machining"]),
        ("", []),
        ("™ - &", ["and"]),
    ],
)
def test_l0_tokens(form, expected):
    assert l0_tokens(form) == expected


@pytest.mark.parametrize(
    "a, b",
    [
        ("Fire Rated", "fire-rated"),
        ("FIRE RATED", "fire rated"),
        ("Paladin™ PW Series", "paladin pw series"),
        ("Doors & Frames", "doors and frames"),
        ("CNC/Manual Machining", "cnc manual machining"),
        ("GRAINTECH™", "graintech"),
    ],
)
def test_l0_variants_share_a_key(a, b):
    assert normalize(a) == normalize(b)


# ---------------------------------------------------------------------------
# L1: dictionary plurals, the traps, the fallback
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "a, b",
    [
        ("assemblies", "assembly"),
        ("Plastics", "plastic"),
        ("medical devices", "medical device"),
        ("batch deliveries", "batch delivery"),
        ("steering columns", "steering column"),
        ("electrical codes", "electrical code"),
        ("presses", "press"),
        ("bearings", "bearing"),
        ("housings", "housing"),
        ("metal stampings", "metal stamping"),  # fallback: stampings is OOV
        ("baseplates", "baseplate"),
        ("counterbores", "counterbore"),
        ("HVACs", "HVAC"),  # acronym plural: not guarded, the fallback folds it
    ],
)
def test_l1_plurals_share_a_key(a, b):
    assert normalize(a) == normalize(b)


@pytest.mark.parametrize(
    "form, key",
    [
        ("Brass", "brass"),          # in the dictionary; never "bras"
        ("brass", "brass"),
        ("bras", "bra"),             # and bras really is a plural
        ("stainless", "stainless"),  # known as ADJ only: no NOUN lemma, no fallback
        ("series", "series"),
        ("chassis", "chassis"),
        ("analysis", "analysis"),
        ("gas", "gas"),
        ("glass", "glass"),
        ("status", "status"),
        ("continuous", "continuous"),  # OOV-guesser would say continuou
        ("abs", "abs"),                # too short for the fallback
        ("as", "as"),
        ("hs", "hs"),
        ("productos", "producto"),     # Spanish: fallback happens to be right
        ("soluciones", "solucione"),   # ...or harmlessly wrong: collides with nothing
    ],
)
def test_l1_traps(form, key):
    assert normalize(form) == key


# ---------------------------------------------------------------------------
# The code-token guard
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "token, guarded",
    [
        ("6061-T6", True), ("ISO9001", True), ("T6", True),
        ("AccuGrips", True), ("iPhones", True), ("PowerGrips", True),
        ("Texas", True), ("Paladin", True),     # capitalized + OOV
        ("Plastics", False),                    # capitalized but known
        ("CNCs", False), ("OEMs", False),       # acronym plural: not internal capitals
        ("CNC", False), ("HVAC", False),
        ("plastics", False), ("accugrips", False),
    ],
)
def test_code_token_guard(token, guarded):
    assert is_code_token(token) is guarded


def test_guarded_tokens_keep_their_casefolded_spelling():
    assert normalize("AccuGrips") == "accugrips"
    assert normalize("AccuGrip") == "accugrip"
    assert normalize("AccuGrips") != normalize("AccuGrip")  # by design (D10)
    # NORMALIZER_VERSION 2: the letter<->digit break splits `T6` into `t 6`, so
    # the temper is no longer ONE guarded token. Measured to be an improvement,
    # not a regression: `6061T6` now shares a key with `6061-T6` and `6061 T6`
    # (under v1 the unseparated spelling was alone), while `T6` and `T651` stay
    # distinct — a temper is a real product difference and must never merge.
    assert normalize("6061-T6 aluminum") == "6061 t 6 aluminum"
    assert normalize("6061T6") == normalize("6061-T6") == normalize("6061 T6")
    assert normalize("6061-T6") != normalize("6061-T651")
    assert normalize("CNCs") == "cncs"  # too short for the fallback; CNC stays apart
    assert normalize("HVACs") == normalize("HVAC") == "hvac"
    assert normalize("Texas") == "texas"
    assert normalize("Kansas") == "kansas"


# ---------------------------------------------------------------------------
# L2: the per-field verb fold
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "a, b",
    [
        ("CNC milled", "CNC milling"),
        ("CNC milling", "CNC mills"),
        ("Polished", "Polishing"),
        ("surface finish", "surface finishing"),
        ("tested", "testing"),
        ("machined", "machining"),
        ("Blasted", "blast"),
        ("bearings", "bearing"),  # L2 applied AFTER L1 keeps these together
        ("housings", "housing"),
        ("coatings", "coating"),
    ],
)
def test_l2_folds_share_a_key(a, b):
    assert normalize(a, verb_fold=True) == normalize(b, verb_fold=True)


def test_l2_is_off_by_default_and_is_a_dial():
    assert normalize("CNC milled") != normalize("CNC milling")
    assert normalize("CNC milled", verb_fold=True) == normalize("CNC milling", verb_fold=True)
    assert normalize("mounting brackets") == "mounting bracket"
    assert normalize("mounting brackets", verb_fold=True) == "mount bracket"


# ---------------------------------------------------------------------------
# Properties
# ---------------------------------------------------------------------------

# Lowercase letters that HAVE an uppercase form and whose upper-casing
# round-trips through casefold. Hypothesis found both exceptions on the first
# runs: dotless i (U+0131) upper-cases to I, which casefolds to i; and letters
# like U+0234 have no uppercase at all, so ``form.upper()`` is not all-caps.
# Both are properties of Unicode, not of the normalizer.
_roundtrip_letters = st.characters(whitelist_categories=("Ll",), max_codepoint=0x24F).filter(
    lambda c: c.upper() != c and c.upper().casefold() == c
)
_word = st.text(alphabet=_roundtrip_letters, min_size=1, max_size=12)
_plain_form = st.lists(_word, min_size=0, max_size=6).map(" ".join)
_any_form = st.text(max_size=60)


@settings(max_examples=300, deadline=None)
@given(_any_form)
def test_normalize_is_idempotent(form):
    """At L0+L1, for forms without a code-token-guarded token. A guarded token
    (digits, internal capital, capitalized-OOV) is returned casefolded and
    UNLEMMATIZED — its casing IS its identity (D10: AccuGrips) — and the
    casefolded key no longer carries the casing the guard read, so a second pass
    may lemmatize it (`AAAaS` → `aaaas` → `aaaa`; found by hypothesis 2026-08-21
    at 3.1). The L2 verb fold is SINGLE-PASS by design and not a fixed point
    either — ``test_verb_fold_is_single_pass_not_a_fixed_point`` pins the
    example — so the property is asserted at ``verb_fold=False`` (user decision
    2026-08-22, option c: a key is computed once per form, never re-normalized)."""
    from core.utils.form_normalizer import is_code_token, l0_tokens

    assume(not any(is_code_token(token) for token in l0_tokens(form)))
    once = normalize(form, verb_fold=False)
    assert normalize(once, verb_fold=False) == once


def test_verb_fold_is_single_pass_not_a_fixed_point():
    """The documented boundary (hypothesis, 2026-08-22): a verb lemma can itself
    be an inflection of another verb — ``ground`` is the lemma of *to ground*
    AND the participle of *to grind* — so ``Grounded`` keys as ``ground`` and a
    second pass would key that as ``grind``. Keys are computed once per form,
    so ``Grounded`` and ``Ground Steel`` simply do not merge (an under-merge,
    the accepted steady state). Pinned so a fixed-point rewrite (option a,
    which would need a NORMALIZER_VERSION bump) is a visible decision."""
    assert normalize("Grounded", verb_fold=True) == "ground"
    assert normalize("ground", verb_fold=True) == "grind"
    assert normalize("Ground Steel", verb_fold=True) == "grind steel"
    assert normalize("Grounded", verb_fold=False) == "grounded"


@settings(max_examples=300, deadline=None)
@given(_any_form, st.booleans())
def test_key_invariants(form, verb_fold):
    key = normalize(form, verb_fold=verb_fold)
    assert key == key.casefold()
    assert not re.search(r"[™®©/\-‐-―&]", key)
    assert "  " not in key and key == key.strip()


@settings(max_examples=300, deadline=None)
@given(_plain_form)
def test_all_lowercase_and_all_uppercase_spellings_share_a_key(form):
    """Casing is only a signal for camel-cased and capitalized-OOV tokens; a
    plain lowercase form and its shouted twin must land in one key."""
    assert normalize(form) == normalize(form.upper())


# ---------------------------------------------------------------------------
# L0 letter<->digit break (NORMALIZER_VERSION 2)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "spaced, joined",
    [
        ("ISO 9001", "ISO9001"),
        ("AS 9100", "AS9100"),
        ("IATF 16949", "IATF16949"),
        ("TS 16949", "TS16949"),
        ("DL-95", "DL95"),
        ("SLM 500", "SLM500"),
        ("DAINICHI DLX-75A", "DAINICHI DLX75A"),
        ("ICC 500-2014", "ICC500-2014"),
        ("3.3 mm", "3.3mm"),
    ],
)
def test_letter_digit_break_merges_separated_and_joined_designators(spaced, joined):
    """Every one of these is a real pair measured on the 20-subject corpus.

    L0 already turned a hyphen between letters and digits into a space, so
    `6061-T6` and `6061 T6` were one key while `DL-95` and `DL95` were two —
    the pipeline held that a separator is meaningless but its absence is
    meaningful. This closes that.
    """
    assert normalize(spaced) == normalize(joined)


@pytest.mark.parametrize(
    "left, right",
    [
        # base standard vs a specific edition: different claims, stay apart
        ("ISO 9001", "ISO 9001:2015"),
        # two different editions: the case that kills the ":YYYY" strip rule,
        # measured live on mathewsco.com
        ("ISO 9001:2000 certified", "ISO 9001:2015 certified"),
        # material temper is a real product difference, not noise
        ("6061-T6", "6061-T651"),
    ],
)
def test_letter_digit_break_does_not_merge_genuinely_different_things(left, right):
    assert normalize(left) != normalize(right)


def test_letter_digit_break_is_consistent_with_the_hyphen_rule():
    """The three spellings of one steelcraft hinge, differing only by unit
    spacing and a hyphen — a PRODUCT, which is why this rule is not
    certificate-specific."""
    a = normalize('standard weight .134" (3.3 mm) thick hinges')
    b = normalize('standard weight .134" (3.3mm) thick hinges')
    c = normalize('standard-weight .134" (3.3mm) thick hinges')
    assert a == b == c
