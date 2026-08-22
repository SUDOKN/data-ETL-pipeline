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
    GroupIdCollisionError,
    NORMALIZER_VERSION,
    NormalizerEnvironmentError,
    assign_group_ids,
    group_id_for_form,
    group_id_for_key,
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


# Recorded for NORMALIZER_VERSION == "1" with lemminflect 0.2.3.
GOLDEN_DIGESTS = {
    "1": {
        "l1": "4589f6813ef892847c75f380baccf1590a9ee2138f6a481f72addf92ef430138",
        "l2": "b9acf60c6c6188ee9a3182bce0bd2171b0dc8763b8590ed518a39d3c862745d0",
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
    assert normalize("6061-T6 aluminum") == "6061 t6 aluminum"
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
@given(_any_form, st.booleans())
def test_normalize_is_idempotent(form, verb_fold):
    """For forms without a code-token-guarded token. A guarded token (digits,
    internal capital, capitalized-OOV) is returned casefolded and UNLEMMATIZED —
    its casing IS its identity (D10: AccuGrips) — and the casefolded key no
    longer carries the casing the guard read, so a second pass may lemmatize it
    (`AAAaS` → `aaaas` → `aaaa`; found by hypothesis 2026-08-21 at 3.1). The key
    is computed once per form, so this is a property boundary, not a defect."""
    from core.utils.form_normalizer import is_code_token, l0_tokens

    assume(not any(is_code_token(token) for token in l0_tokens(form)))
    once = normalize(form, verb_fold=verb_fold)
    assert normalize(once, verb_fold=verb_fold) == once


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
# group_id
# ---------------------------------------------------------------------------

_GID = re.compile(r"^g[0-9a-z]{7}$")


def test_group_id_shape_determinism_and_derivation():
    gid = group_id_for_key("fire rated")
    assert _GID.match(gid)
    assert gid == group_id_for_key("fire rated") == group_id_for_form("Fire-Rated")
    assert group_id_for_key("fire rated") != group_id_for_key("fire rating")


def test_group_ids_are_not_record_ids():
    from core.utils.record_id_util import record_id_for_phrase
    # Same hash space, different prefix — the two keys can never be confused
    # even when a form is already normalized.
    assert group_id_for_key("aluminum")[0] == "g"
    assert record_id_for_phrase("aluminum")[0] == "r"


def test_assign_group_ids_orders_dedupes_and_detects_collisions(monkeypatch):
    ids = assign_group_ids(["b", "a", "b"])
    assert list(ids) == ["b", "a"]
    monkeypatch.setattr(form_normalizer, "group_id_for_key", lambda key: "g0000000")
    with pytest.raises(GroupIdCollisionError):
        assign_group_ids(["x", "y"])
