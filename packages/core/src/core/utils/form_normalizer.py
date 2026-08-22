"""The v3 surface-form normalizer and the group key it derives (PIPELINE_V3_PLAN.md
D9–D11): a pure, versioned function from a form string to the normalization key
that is the group's identity.

Grouping in v3 is a dict keyed by ``normalize(form)`` — no LLM, no union-find
(D9, D12). So this function IS the grouper, and everything about it is tuned
for the one failure that matters: a WRONG merge lands upstream of every check,
while an under-merge costs one redundant synthesis request and heals at
reconcile (D13). Every layer below errs toward under-merging.

LAYERS (D10)
------------
L0 — case/punct: casefold; strip ™ ® ©; hyphens, dashes and slashes become
     spaces; ``&`` becomes ``and``; edge punctuation is stripped off tokens;
     whitespace collapses. Always on.
L1 — plural: per-token NOUN lemma from lemminflect's DICTIONARY, never its
     rule-based OOV guesser (measured 2026-08-21: the guesser is suffix
     stripping in disguise — ``continuous``→``continuou``, ``abs``→``ab``,
     ``as``→``a``). A token the dictionary knows under no part of speech gets one
     narrowly guarded fallback: ≥5 letters, alphabetic, ends in ``s`` but not
     ``ss``/``us``/``is``/``ous`` → drop the ``s``. That fallback is what folds
     the domain plurals the dictionary lacks (``stampings``, ``counterbores``,
     ``helicoils``) and, measured over 4,157 real phrases, merged nothing it
     should not have. Always on.
L2 — verb/participle fold: per-token VERB dictionary lemma applied to the L1
     result, so ``bearings``→``bearing``→``bear`` and ``bearing``→``bear`` land
     in one key (applying it to the raw token instead splits them). A per-field
     dial for process/material fields only (``CNC milled``/``milling``,
     ``Polished``/``Polishing``); off elsewhere, where a verb fold only uglifies
     noun modifiers (``mounting bracket``→``mount bracket``).

CODE-TOKEN GUARD (D10): a token is never lemmatized when it carries a digit
(``6061-T6``, ``ISO9001``), an internal capital (``AccuGrips`` — a camel-cased
name's casing IS its identity; ``AccuGrip``/``AccuGrips`` stay separate by
design), or is capitalized and unknown to the dictionary (``Texas``,
``Paladin`` — a proper or invented name, not a word to fold).

VERSIONING (D10): ``NORMALIZER_VERSION`` names the behaviour of this module
PLUS the pinned lemmatizer dictionary. Group ids are ``hash(normalize(form))``
— not salted with the version, so an upgrade that leaves a key unchanged leaves
its id unchanged — and the golden-corpus digest test is the tripwire: change a
rule or the pin without bumping the version and the test fails. The pin is also
asserted at import, so a drifted environment fails loudly at startup rather than
regrouping silently.
"""

from __future__ import annotations

import hashlib
import re
import unicodedata
from typing import Iterable

import lemminflect
from lemminflect import getAllLemmas, getLemma

NORMALIZER_VERSION = "1"

LEMMATIZER_NAME = "lemminflect"
LEMMATIZER_PINNED_VERSION = "0.2.3"


class NormalizerEnvironmentError(RuntimeError):
    """The installed lemmatizer is not the version the normalizer was
    versioned against. Its dictionary defines the group keys, so running on a
    different version would silently regroup every run; fix the environment
    (``lemminflect==0.2.3``) or bump the pin AND ``NORMALIZER_VERSION``
    together and re-record the golden digest."""


def assert_lemmatizer_pinned() -> None:
    installed = getattr(lemminflect, "__version__", None)
    if installed != LEMMATIZER_PINNED_VERSION:
        raise NormalizerEnvironmentError(
            f"{LEMMATIZER_NAME} {installed!r} is installed; the normalizer "
            f"(NORMALIZER_VERSION={NORMALIZER_VERSION}) is pinned to "
            f"{LEMMATIZER_PINNED_VERSION!r}."
        )


assert_lemmatizer_pinned()


# --- L0 ----------------------------------------------------------------------

_TRADEMARK_SYMBOLS = re.compile(r"[™®©]")  # ™ ® ©
# ASCII hyphen, the Unicode hyphen/dash block (‐ ‑ ‒ – — ―), and slashes.
_SEPARATORS = re.compile(r"[\-‐-―/]+")
_EDGE_PUNCTUATION = "()[]{}'\",.;:!?"


def l0_tokens(form: str) -> list[str]:
    """The form's tokens after L0, casing PRESERVED (the guard reads it)."""
    text = unicodedata.normalize("NFC", form)
    text = _TRADEMARK_SYMBOLS.sub("", text)
    text = _SEPARATORS.sub(" ", text)
    text = text.replace("&", " and ")
    tokens = (token.strip(_EDGE_PUNCTUATION) for token in text.split())
    return [token for token in tokens if token]


# --- guard + lemmas ------------------------------------------------------------

_FALLBACK_MIN_LENGTH = 5
_FALLBACK_BLOCKED_ENDINGS = ("ss", "us", "is", "ous")


def _is_known_word(token_cf: str) -> bool:
    return bool(getAllLemmas(token_cf))


def _is_acronym_plural(token: str) -> bool:
    """``CNCs``, ``OEMs``, ``HVACs``: an all-caps acronym with a plural ``s`` is
    not a camel-cased name, so the internal-capital guard stands down for it."""
    return len(token) >= 3 and token.endswith("s") and token[:-1].isupper()


def is_code_token(token: str) -> bool:
    """True when the token must not be lemmatized (D10's code-token guard):
    carries a digit, has an internal capital while not being all-caps, or is
    capitalized and unknown to the dictionary."""
    if any(ch.isdigit() for ch in token):
        return True
    if _is_acronym_plural(token):
        return False  # CNCs, OEMs: neither camel-cased nor a capitalized name
    tail = token[1:]
    if tail != tail.lower() and not token.isupper():
        return True  # AccuGrips, iPhone-style internal capitals
    if token[:1].isupper() and not token.isupper() and not _is_known_word(token.casefold()):
        return True  # Texas, Paladin: capitalized and out of vocabulary
    return False


def _noun_lemma(token_cf: str) -> str | None:
    lemmas = getLemma(token_cf, upos="NOUN", lemmatize_oov=False)
    return lemmas[0] if lemmas else None


def _verb_lemma(token_cf: str) -> str | None:
    lemmas = getLemma(token_cf, upos="VERB", lemmatize_oov=False)
    return lemmas[0] if lemmas else None


def _guarded_plural_fallback(token_cf: str) -> str | None:
    """The one rule applied to dictionary-unknown tokens; see module docstring."""
    if _is_known_word(token_cf):
        return None
    if (
        len(token_cf) >= _FALLBACK_MIN_LENGTH
        and token_cf.isalpha()
        and token_cf.endswith("s")
        and not token_cf.endswith(_FALLBACK_BLOCKED_ENDINGS)
    ):
        return token_cf[:-1]
    return None


def _normalize_token(token: str, *, verb_fold: bool) -> str:
    token_cf = token.casefold()
    if is_code_token(token) or not token_cf.isalpha():
        return token_cf
    lemma = _noun_lemma(token_cf)
    if lemma is None:
        lemma = _guarded_plural_fallback(token_cf)
    if lemma is None:
        lemma = token_cf
    if verb_fold:
        verb = _verb_lemma(lemma)
        if verb is not None:
            lemma = verb
    return lemma


# --- the key ------------------------------------------------------------------


def normalize(form: str, *, verb_fold: bool = False) -> str:
    """The normalization key of *form*: L0 + L1 always, L2 when *verb_fold*.

    Pure and deterministic. ``normalize(normalize(x)) == normalize(x)`` for
    every form without a code-token-guarded token; a guarded token is returned
    casefolded and unlemmatized (its casing is its identity), and that
    casefolded spelling, fed back in, no longer carries the casing the guard
    read — so the key is a fixed point only for un-guarded forms. Keys are
    computed once per form, never re-normalized. The empty string is the key
    of a form with no tokens.
    """
    return " ".join(_normalize_token(token, verb_fold=verb_fold) for token in l0_tokens(form))


# --- the id -------------------------------------------------------------------

GROUP_ID_PREFIX = "g"
GROUP_ID_BODY_LENGTH = 7
_BASE36 = "0123456789abcdefghijklmnopqrstuvwxyz"
_ID_SPACE = 36**GROUP_ID_BODY_LENGTH


def group_id_for_key(normalized_key: str) -> str:
    """``g`` + 7 base36 characters of the key's sha256 — the downstream wire
    key for everything after aggregation (D11). Same construction and
    rationale as ``record_id_util.record_id_for_phrase``: content-derived, so
    stable across chunks and runs; random-looking, so a mangled id matches
    nothing rather than a neighbour."""
    digest = hashlib.sha256(normalized_key.encode("utf-8")).digest()
    n = int.from_bytes(digest, "big") % _ID_SPACE
    chars = []
    for _ in range(GROUP_ID_BODY_LENGTH):
        n, rem = divmod(n, 36)
        chars.append(_BASE36[rem])
    return GROUP_ID_PREFIX + "".join(reversed(chars))


def group_id_for_form(form: str, *, verb_fold: bool = False) -> str:
    return group_id_for_key(normalize(form, verb_fold=verb_fold))


class GroupIdCollisionError(ValueError):
    """Two distinct keys hashed to one group id — deterministic, so raise
    ``GROUP_ID_BODY_LENGTH`` rather than retry."""


def assign_group_ids(normalized_keys: Iterable[str]) -> dict[str, str]:
    """``key -> group_id`` over *normalized_keys*, insertion-ordered, raising on
    a collision between two distinct keys."""
    ids: dict[str, str] = {}
    key_by_id: dict[str, str] = {}
    for key in normalized_keys:
        if key in ids:
            continue
        gid = group_id_for_key(key)
        clashing = key_by_id.get(gid)
        if clashing is not None and clashing != key:
            raise GroupIdCollisionError(
                f"group id {gid!r} is shared by two distinct keys: {clashing!r} and "
                f"{key!r}; raise GROUP_ID_BODY_LENGTH."
            )
        ids[key] = gid
        key_by_id[gid] = key
    return ids
