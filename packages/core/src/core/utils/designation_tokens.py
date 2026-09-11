"""Designation-shaped tokens: the mechanical vocabulary of the synthesis
stage's under-enumeration check.

A "designation" is a token whose information lives in its FORM, not its
meaning — a model code, grade, or standard id. The check built on this module
is a CONSERVATION law, never a semantic judgment: the designations a record
OWNS must appear in its synthesis, and a synthesis that drops one gets the
stage's one retry.

WHAT A RECORD OWNS (the focal-form designation rule, 2026-09-10 — decisions
D2/D14/D16 in ``docs_local/SYNTHESIS_GROUNDING_REDESIGN_2026-09-10.md``, rule
spelled out and measured in ``docs_local/trigger_scope_survey_20260910/``).
Until 2026-09-10 the check demanded every designation-shaped token anywhere in
a record's snippets; measured on run 20260905T213127, 82.8% of those tokens
belonged to sibling entities or to nobody (16,644 demanded pairs, 1,567
records re-asked, and the re-asked population failed at 19.8% against 5.4%).
Now a record's demand is:

- **own tokens** — designation shapes inside the focal form itself
  (``UNS N06625`` → ``UNS N06625``; ``D2`` counts at two characters, a tool
  steel); these are never dropped in practice (0 of 6,381 drops measured);
- **the after-span** — designations written directly after an occurrence of
  the focal form in a snippet (``Aluminum castings in 319, 356, and A357
  alloys`` → all three). The span is at most 80 characters, never cut inside
  a token, and ENDS at the first thing that names another item: a sentence
  end, a table cell, a list item, a semicolon, a spaced colon, a SIBLING FORM
  (another record's form in the same chunk — the user's rule), a Capitalized
  word that is not a kind word, a designation whose preceding word is an
  ordinary lowercase word (that word's item), a designation that prefixes the
  next item (``2D/3D Laser Cutting``), a kind-word pair that designates the
  noun after it (``Grade 5 titanium``), the first comma when the focal form
  is itself one item of a list, or the third ordinary word;
- **the before-span** — letter+digit codes and head pairs directly before the
  focal form (``A60 galvannealed steel``, ``Grade 5 titanium``), through
  connectors, kind words and at most ONE ordinary word, stopping at commas,
  colons, parentheses, verbs of being and modals, a sibling form or a
  Capitalized word, and skipped when the focal form is the first word of a
  longer proper name. Bare numbers before the focal form are never demanded
  (70% counts, section numbers and table values by reading).

A sibling occurrence that CONTAINS the focal occurrence, or overlaps its end,
owns the span on both sides (``Steel Doors 101``, ``Aluminum 6061-T6``); a
focal form standing between two designation-shaped neighbours is one item of
a peer run and gets no span (``8620H 414020MnCr5 18NiCrMo4 En19``).

NEVER designations: a number followed by a unit (``05mm``, ``600-lb``,
``110 to 170 mph``, hardness scales), decimals (never split), thousands
groups (``70,000``), fractions, ordinals, page refs, counts in parentheses
(``SHIMADA (11)``), a small or round number before a lowercase noun
(``over 100 individual parts``) unless a head precedes it (``STC 45``), digit
sequences (``937 878 0911``), URL/path/filename continuations, and
lowercase-letter ids (``kpl01``). Slash-joined codes are demanded as their
parts (``IP65/IP67/IP68``); a slash after a code is a variant and stays
(``JTM-1050EVS/460``).

PRESENCE is checked case-insensitively at word boundaries, tolerant of an
optional space or hyphen inside a token and at every letter/digit seam
(``ISO14001`` ~ ``ISO 14001``, ``6061-T6`` ~ ``6061 T6``).

Measured (run 20260905T213127, first-pass answers): 3,323 demanded pairs
(2,695 own + 628 adjacent), 29 records fire, ≈20 retry requests; demanded
pairs 89% own by reading; fires 36% real drops (today's rule: 6%).

``designation_shaped_tokens`` keeps the raw shape tiers (letter+digit tokens,
ALL-CAPS/kind-word pairs) for the harness's nominations and for tests; it is
NOT the demand.
"""

from __future__ import annotations

import re
from functools import lru_cache
from typing import Iterable, Optional, Sequence

# --- vocabulary --------------------------------------------------------------------------------

# Capitalized kind words that head a pair (``Grade 630``, ``Section 172``).
_KIND_HEADS = frozenset({
    "Grade", "Alloy", "Series", "Model", "Type", "Class", "Mark",
    "Section", "Regulation", "Clause", "Form", "Schedule",
})
_KIND_LOWER = frozenset(w.lower() for w in _KIND_HEADS)
# Kind words in any casing: they neither name another item nor count as an
# ordinary word inside a span.
_KIND_WORDS = _KIND_LOWER | {
    "grades", "alloys", "types", "models", "classes", "marks", "sections",
    "forms", "schedules", "spec", "specs", "specification", "specifications",
    "standard", "standards", "code", "codes", "designation", "designations",
    "no", "nos", "number", "numbers", "part", "parts", "item", "items",
    "style", "styles", "version", "versions", "rev", "revision", "temper",
    "tempers", "size", "sizes", "family", "families", "range", "ranges",
    "sku", "skus", "pn", "ref",
}
_CONNECTORS = frozenset({
    "and", "or", "&", "plus", "with", "in", "of", "to", "the", "a", "an", "as",
    "such", "including", "include", "includes", "incl", "e.g", "eg", "i.e",
    "ie", "per", "from", "for", "on", "at", "by", "via", "like", "etc",
    "also", "either", "both", "all", "various", "several", "other", "others",
    "is", "are", "was", "were", "be", "being", "been", "our", "its", "their",
    "this", "these", "that", "those", "available", "offered", "certified",
    "rated", "approved", "compliant", "meets", "meeting", "under", "up",
    "through", "thru", "between", "into", "over", "than", "then", "not",
    "only", "any", "each", "every", "some", "more", "most", "which", "where",
    "when", "we", "it", "they", "you", "one", "two", "three",
})
_UNITS = frozenset({
    "mm", "cm", "m", "km", "in", "inch", "inches", "ft", "feet", "yd", "lb",
    "lbs", "kg", "g", "gr", "oz", "ton", "tons", "tonne", "tonnes", "psi",
    "ksi", "mpa", "kpa", "gpa", "bar", "hp", "kw", "w", "v", "kv", "mv", "a",
    "amp", "amps", "hz", "khz", "mhz", "ghz", "rpm", "sq", "cu", "ga",
    "gauge", "deg", "min", "mins", "hr", "hrs", "hour", "hours", "sec",
    "secs", "ms", "mil", "mils", "thou", "micron", "microns", "µm", "um",
    "nm", "pcs", "pc", "percent", "days", "day", "weeks", "week", "years",
    "year", "months", "month", "yrs", "x", "°c", "°f", "c", "f", "k",
    "pounds", "pound", "miles", "mile", "mph", "kph", "degrees", "degree",
    "gallons", "gallon", "gal", "liters", "litres", "liter", "litre", "ml", "cc",
    "units", "unit", "pieces", "piece", "boxes", "box", "people", "employees",
    "hrc", "hrb", "hb", "hv", "rc", "n/mm2", "brinell", "rockwell",
    "customers", "countries", "states", "locations", "pages", "page",
})
_UNIT_SYMBOLS = frozenset({"°", "%", "×", '"', "'", "″", "′", "±"})
_CLAUSE_VERBS = frozenset({
    "is", "are", "was", "were", "be", "been", "being", "can", "could", "will",
    "would", "may", "might", "must", "shall", "should", "does", "do", "did",
    "has", "have", "had",
})
_SPAN_CHARS = 80

_TOKEN_RE = re.compile(r"[A-Za-z0-9µ](?:[A-Za-z0-9/\-]*[A-Za-z0-9])?|[^\sA-Za-z0-9µ]")
_FILE_EXT_RE = re.compile(
    r"\.(?:jpe?g|png|gif|pdf|svg|webp|html?|php|aspx?|xlsx?|docx?|csv)(?![A-Za-z0-9])", re.I
)
_YEAR_RE = re.compile(r"^(?:19|20)\d{2}$")
_ALLCAPS_RE = re.compile(r"^[A-Z]{2,8}$")
_CAPWORD_RE = re.compile(r"^[A-Z][a-z]+$")
_DIGITS_RE = re.compile(r"^\d{2,4}$")
_LETTER_DIGIT_RE = re.compile(
    r"^(?=[A-Za-z0-9/\-]*[A-Za-z])(?=[A-Za-z0-9/\-]*\d)[A-Za-z0-9][A-Za-z0-9/\-]*$"
)
_DIGITS_UNIT_RE = re.compile(r"^(\d+(?:/\d+)?(?:\.\d+)?)-?([a-zA-Zµ°]{1,6})$")  # 05mm, 600-lb
_ORDINAL_RE = re.compile(r"^\d+(?:st|nd|rd|th)$", re.I)
_PAGE_REF_RE = re.compile(r"^p{1,2}\d{1,4}$")
_FRACTION_RE = re.compile(r"^\d+/\d+(?:st|nd|rd|th)?$", re.I)
# A word that opens a proper name: Coral, S., BLACKSTONE, CNCs.
_NAME_RE = re.compile(r"^(?:[A-Z](?:[a-z]+|\.)?|[A-Z]{9,}|[A-Z]{2,8}[a-z]+)$")

# The raw shape tiers (``designation_shaped_tokens``): a token carrying both a
# letter and a digit, and an ALL-CAPS/kind-word head followed by a digit-
# bearing tail.
_CORE_TOKEN_RE = re.compile(
    r"\b(?=[A-Za-z0-9/-]*[A-Za-z])(?=[A-Za-z0-9/-]*\d)[A-Za-z0-9][A-Za-z0-9/-]*\b"
)
_PAIR_RE = re.compile(
    r"\b([A-Z]{2,8}|[A-Z][a-z]{2,11})\s+((?=[A-Za-z0-9/-]*\d)[A-Za-z0-9][A-Za-z0-9/.-]*)\b"
)
_MIN_RAW_TOKEN_LEN = 3

Token = tuple[str, int, int]  # (text, start, end) over the snippet


# --- presence ------------------------------------------------------------------------------------


@lru_cache(maxsize=None)
def _present_re(token: str) -> re.Pattern[str]:
    """Word-boundary presence, case-insensitive; spaces and hyphens inside the
    token, and the seam between a letter run and a digit run, may be written
    with or without a space or hyphen (ISO14001 ~ ISO 14001, 6061-T6 ~ 6061 T6)."""
    parts = re.findall(r"[A-Za-z]+|\d+|[^A-Za-z0-9\s-]+", token)
    pattern = r"[\s-]?".join(re.escape(p) for p in parts)
    return re.compile(r"(?<![A-Za-z0-9])" + pattern + r"(?![A-Za-z0-9])", re.IGNORECASE)


def _present(token: str, text: str) -> bool:
    return _present_re(token).search(text) is not None


@lru_cache(maxsize=None)
def _form_re(form: str) -> re.Pattern[str]:
    """An occurrence of a form: left word boundary only, so a plural or a
    fused suffix still counts as the form (``Steels``, ``JTM-1050EVS/460``)."""
    return re.compile(r"(?<![A-Za-z0-9])" + re.escape(form), re.IGNORECASE)


@lru_cache(maxsize=None)
def _forms_re(forms: tuple[str, ...]) -> re.Pattern[str]:
    """Alternation over forms, longest first, so a longer form wins where a
    shorter one is its prefix."""
    ordered = sorted({f for f in forms if f and f.strip()}, key=len, reverse=True)
    if not ordered:
        return re.compile(r"(?!x)x")
    return re.compile(
        r"(?<![A-Za-z0-9])(?:" + "|".join(re.escape(f) for f in ordered) + r")",
        re.IGNORECASE,
    )


# --- shapes --------------------------------------------------------------------------------------


def _is_word(tok: str) -> bool:
    return bool(tok) and (tok[0].isalnum() or tok[0] == "µ")


def _is_head(word: Optional[str]) -> bool:
    return bool(word) and (bool(_ALLCAPS_RE.match(word or "")) or (word or "").lower() in _KIND_LOWER)


def _shape(tok: str, *, min_len: int) -> Optional[str]:
    """``"T1"`` letter+digit, ``"D"`` bare digits (2–4, not a year), or None."""
    if _LETTER_DIGIT_RE.match(tok):
        m = _DIGITS_UNIT_RE.match(tok)
        if m and m.group(2).lower() in _UNITS:
            return None  # 05mm, 10lb, 25hp: a measurement
        if _ORDINAL_RE.match(tok) or _PAGE_REF_RE.match(tok) or _FRACTION_RE.match(tok):
            return None  # 2nd, 21st, p277, 1/16th
        if min_len >= 3 and tok[0].islower() and not any(c.isupper() for c in tok):
            return None  # kpl01: lowercase letters + digits is page furniture, not a grade
        return "T1" if len(tok) >= min_len else None
    if _DIGITS_RE.match(tok) and not _YEAR_RE.match(tok):
        return "D"
    return None


def _measurement(tokens: Sequence[Token], i: int, text: str) -> bool:
    """A number followed by a unit, an operand of a × dimension, a decimal
    fragment, a thousands group, a digit sequence, or a count in parentheses."""
    tok, start, end = tokens[i]
    if not tok[0].isdigit():
        return False
    if tok.isdigit() and start >= 1 and end < len(text) and text[start - 1] == "(" and text[end] == ")":
        return True  # an inventory count: SHIMADA (11)
    if tok.isdigit():
        for j in (i - 1, i + 1):
            if (
                0 <= j < len(tokens)
                and tokens[j][0].isdigit()
                and text[min(end, tokens[j][2]):max(start, tokens[j][1])].isspace()
            ):
                return True  # 937 878 0911: a digit sequence, not designations
    if start >= 2 and text[start - 1] == "." and text[start - 2].isdigit():
        return True
    if end + 1 < len(text) and text[end] == "." and text[end + 1].isdigit():
        return True
    # thousands separator: 70,000 / 1,250,000
    if (
        i + 2 < len(tokens)
        and tokens[i + 1][0] == ","
        and tokens[i + 1][1] == end
        and re.match(r"\d{3}", tokens[i + 2][0])
        and tokens[i + 2][1] == tokens[i + 1][2]
    ):
        return True
    if (
        i >= 2
        and tokens[i - 1][0] == ","
        and tokens[i - 1][2] == start
        and tokens[i - 2][0].isdigit()
        and tokens[i - 2][2] == tokens[i - 1][1]
        and re.fullmatch(r"\d{3}", tok)
    ):
        return True
    if (
        i + 3 < len(tokens)
        and tokens[i + 1][0].lower() in {"to", "-", "–", "through", "thru"}
        and tokens[i + 2][0].isdigit()
    ):
        unit = tokens[i + 3][0]
        if unit in _UNIT_SYMBOLS or unit.lower() in _UNITS:
            return True  # 110 to 170 mph
    if i + 1 < len(tokens):
        nxt, _s, after = tokens[i + 1]
        if nxt in _UNIT_SYMBOLS:
            return True
        if nxt.lower() in _UNITS and (after >= len(text) or not text[after].isalnum()):
            return True
    if i >= 1 and tokens[i - 1][0] in {"×", "x", "X"}:
        return True
    return False


def _measurement_at(
    toks: Sequence[Token],
    i: int,
    snippet: str,
    backwards: bool,
    all_toks: Optional[Sequence[Token]],
    first_index: int,
) -> bool:
    """``_measurement`` for the i-th token of a walk, looked up in the whole
    snippet's forward token list when one is given (units past the span
    limit still count)."""
    if all_toks is not None:
        target = toks[i][1]
        j = first_index
        while j < len(all_toks) and all_toks[j][1] != target:
            j += 1
        if j < len(all_toks):
            return _measurement(all_toks, j, snippet)
    if not backwards:
        return _measurement(toks, i, snippet)
    forward = list(reversed(toks))
    return _measurement(forward, len(toks) - 1 - i, snippet)


def _path_continuation(snippet: str, s: int) -> bool:
    """``s`` starts a token right after '/' or '.': a URL path / domain /
    filename continuation when the segment before is purely alphabetic
    (gov/trade/remedies/301, P65Warnings.ca.gov); a slash after a code is a
    model variant (JTM-1050EVS/460) and stays."""
    if s < 2 or snippet[s - 1] not in "./" or not snippet[s - 2].isalnum():
        return False
    j = s - 2
    while j >= 0 and (snippet[j].isalnum() or snippet[j] in "-_"):
        j -= 1
    segment = snippet[j + 1:s - 1]
    return segment.isalpha()


def _neighbour_token(snippet: str, pos: int, *, before: bool) -> Optional[str]:
    """The word token adjacent to ``pos`` across whitespace only (None if
    punctuation or the snippet edge intervenes)."""
    if before:
        j = pos
        while j > 0 and snippet[j - 1] == " ":
            j -= 1
        if j == pos or j == 0:
            return None
        m = re.search(r"([A-Za-z0-9µ][A-Za-z0-9/\-]*)$", snippet[:j])
        return m.group(1) if m and m.end() == j else None
    j = pos
    while j < len(snippet) and snippet[j] == " ":
        j += 1
    if j == pos or j >= len(snippet):
        return None
    m = _TOKEN_RE.match(snippet, j)
    return m.group(0) if m and _is_word(m.group(0)) else None


# --- the walk ------------------------------------------------------------------------------------


def _walk(
    toks: Sequence[Token],
    snippet: str,
    *,
    backwards: bool,
    all_toks: Optional[Sequence[Token]] = None,
    first_index: int = 0,
    list_item: bool = False,
) -> list[str]:
    """Walk a span's tokens (already ordered in walking direction) and collect
    the designations that belong to the focal form, stopping at the first
    thing that names another item (module docstring). ``all_toks`` is the
    whole snippet's forward token list (lookahead for units and the next
    word past the span limit); ``first_index`` is the span's first token in
    it. Bare digits are demanded only walking forward."""
    found: list[str] = []
    prev_word: Optional[str] = None  # the last WORD seen, None after punctuation
    prev_is_shaped = False
    ordinary_words = 0  # ordinary words passed (limit: 1 before, 2 after)
    depth = 0  # parentheses: a comma inside them is not a list comma
    run_start = 0  # index in ``found`` where the current comma/connector run began
    for i, (tok, s, e) in enumerate(toks):
        if tok in {"|", ";", "•", "·"} or (backwards and tok in {",", ")", ":", "/"}):
            break
        if backwards and tok.lower() in _CLAUSE_VERBS:
            break  # "the P-300 can process", "UNS N09946 is approved under": a clause, not a prefix
        if tok == "(":
            depth += 1
        elif tok == ")":
            depth = max(0, depth - 1)
        if tok == "," and not backwards and list_item and depth == 0:
            break  # the focal form is one item of a list: what follows the comma is a peer
        if (
            tok == ":"
            and not backwards
            and e < len(snippet)
            and snippet[e] == " "
            and (i > 0 or (s > 0 and snippet[s - 1] == " "))
        ):
            break  # a spaced colon / fused navigation segment; a colon attached to the focal form is kept
        if tok == ".":
            nxt = toks[i + 1][0] if i + 1 < len(toks) else ""
            adjacent = i + 1 < len(toks) and (
                toks[i + 1][1] == e if not backwards else toks[i + 1][2] == s
            )
            if nxt[:1].isdigit() and adjacent and prev_word and prev_word[-1:].isdigit():
                continue  # decimal point
            if nxt and adjacent and not nxt[0].isupper() and not backwards:
                continue  # "e.g." / "no." / "co.uk"
            break
        if not _is_word(tok):
            prev_word, prev_is_shaped = None, False
            continue
        shape = _shape(tok, min_len=2 if found else 3)
        if shape is None and any(c.isdigit() for c in tok) and not tok.isalpha():
            if not backwards and _is_head(prev_word) and not prev_is_shaped:
                shape = "P"  # Grade 5, ISO 2015-style tails: the head vouches for the tail
            elif (
                backwards
                and i + 1 < len(toks)
                and _is_head(toks[i + 1][0])
                and toks[i + 1][2] < s
                and snippet[toks[i + 1][2]:s].isspace()
            ):
                shape = "P"
        if shape:
            if _measurement_at(toks, i, snippet, backwards, all_toks, first_index):
                prev_word, prev_is_shaped = tok, True
                continue
            if _FILE_EXT_RE.match(snippet, e) or _path_continuation(snippet, s):
                prev_word, prev_is_shaped = tok, True
                continue  # URL path / domain / filename continuation
            if (
                shape == "D"
                and not backwards
                and all_toks is not None
                and not (_is_head(prev_word) and not prev_is_shaped)
            ):
                k = first_index + i + 1
                nw = all_toks[k][0] if k < len(all_toks) else ""
                if (
                    nw
                    and nw[0].islower()
                    and nw.lower() not in _CONNECTORS
                    and nw.lower() not in _KIND_WORDS
                    and nw.lower() not in _UNITS
                    and (len(tok) <= 2 or int(tok) % 50 == 0)
                ):
                    prev_word, prev_is_shaped = tok, True
                    continue  # "over 100 individual parts", "40 coining presses": a count
            parts = tok.split("/") if "/" in tok else [tok]
            if len(parts) > 1 and all(_shape(p, min_len=3) for p in parts):
                found.extend(parts)  # slash-joined codes (IP65/IP67/IP68) are demanded as parts
                prev_word, prev_is_shaped = tok, True
                continue
            if backwards:
                # a head to the LEFT of this token makes a pair (Grade 5, Type 316)
                nxt_tok = toks[i + 1] if i + 1 < len(toks) else None
                if (
                    nxt_tok
                    and _is_head(nxt_tok[0])
                    and nxt_tok[2] < s
                    and snippet[nxt_tok[2]:s].isspace()
                ):
                    found.append(f"{nxt_tok[0]} {tok}")
                elif shape != "D":
                    found.append(tok)  # bare digits before the focal form are never demanded
                prev_word, prev_is_shaped = tok, True
                continue
            if all_toks is not None:
                k = first_index + i + 1
                nxt_word = all_toks[k][0] if k < len(all_toks) and _is_word(all_toks[k][0]) else None
            else:
                nxt_word = next((w for (w, _ws, _we) in toks[i + 1:i + 2] if _is_word(w)), None)
            if (
                nxt_word
                and _NAME_RE.match(nxt_word)
                and nxt_word.lower() not in _CONNECTORS
                and nxt_word.lower() not in _KIND_WORDS
            ):
                del found[run_start:]  # "2D/3D Laser", "3-axis, 4-axis and 5-axis CNCs": the next item's own
                break
            if (
                nxt_word
                and nxt_word[0].islower()
                and _is_head(prev_word)
                and not prev_is_shaped
                and (prev_word or "").lower() in _KIND_LOWER
                and nxt_word.lower() not in _CONNECTORS
                and nxt_word.lower() not in _KIND_WORDS
                and nxt_word.lower() not in _UNITS
            ):
                break  # "Grade 5 titanium": the kind-word pair designates the noun after it
            if _is_head(prev_word) and not prev_is_shaped:
                token = f"{prev_word} {tok}"
            elif (
                prev_word
                and not prev_is_shaped
                and prev_word.lower() not in _CONNECTORS
                and prev_word.lower() not in _KIND_WORDS
            ):
                break  # "widgets 4130": an ordinary word's own designation
            else:
                token = tok
            found.append(token)
            prev_word, prev_is_shaped = tok, True
            continue
        low = tok.lower()
        if low in _CONNECTORS or low in _KIND_WORDS or _ALLCAPS_RE.match(tok):
            if low in _KIND_WORDS:
                run_start = len(found)
            prev_word, prev_is_shaped = tok, False
            continue
        if _CAPWORD_RE.match(tok):
            break  # a Capitalized word names another item
        ordinary_words += 1
        if ordinary_words > (1 if backwards else 2):
            break
        run_start = len(found)  # an ordinary word ends a list run
        prev_word, prev_is_shaped = tok, False
    return found


# --- ownership -----------------------------------------------------------------------------------


def own_tokens(focal_form: str) -> set[str]:
    """Designation-shaped tokens inside the focal form itself (two-character
    minimum: ``D2`` is a tool steel); a head + year pair counts here
    (``ISO 9001:2015``'s tail is the form's own)."""
    out: set[str] = set()
    toks = [(m.group(0), m.start(), m.end()) for m in _TOKEN_RE.finditer(focal_form)]
    prev: Optional[str] = None
    for tok, _s, _e in toks:
        if not _is_word(tok):
            prev = None  # any punctuation breaks a head/tail pair (SAE - 4130)
            continue
        shape = _shape(tok, min_len=2)
        if shape is None and _DIGITS_RE.match(tok) and _YEAR_RE.match(tok) and _is_head(prev):
            shape = "D"
        if shape:
            out.add(f"{prev} {tok}" if _is_head(prev) else tok)
        prev = tok
    return out


def _sibling_occurrences(
    snippet: str, focal_form: str, sibling_forms: Sequence[str]
) -> list[tuple[int, int, str]]:
    """Occurrences of OTHER records' forms in the snippet, longest form
    winning at a position (the focal form rides in the alternation so it is
    not mistaken for a shorter sibling it contains, then dropped)."""
    if not sibling_forms:
        return []
    focal = focal_form.casefold()
    pattern = _forms_re(tuple(sorted({*sibling_forms, focal_form})))
    return [
        (m.start(), m.end(), m.group(0))
        for m in pattern.finditer(snippet)
        if m.group(0).casefold() != focal
    ]


def _owned_by_sibling(fs: int, fe: int, sib_occ: Sequence[tuple[int, int, str]]) -> Optional[str]:
    """A sibling occurrence that contains the focal occurrence, or overlaps
    its end, owns the span on both sides (the more specific record)."""
    for s, e, form in sib_occ:
        if (s, e) == (fs, fe):
            continue
        if (s <= fs and e >= fe) or (s < fe < e) or (s < fs < e):
            return form
    return None


def _span_tokens(
    snippet: str, focal_form: str, sib_occ: Sequence[tuple[int, int, str]]
) -> set[str]:
    """The designations the spans around each focal-form occurrence in one
    snippet demand (the after-span and the before-span; module docstring)."""
    demanded: set[str] = set()
    for m in _form_re(focal_form).finditer(snippet):
        fs, fe = m.start(), m.end()
        if _owned_by_sibling(fs, fe, sib_occ) is not None:
            continue
        prev_tok = _neighbour_token(snippet, fs, before=True)
        next_tok = _neighbour_token(snippet, fe, before=False)
        if prev_tok and next_tok and _shape(prev_tok, min_len=2) and _shape(next_tok, min_len=2):
            continue  # 8620H 414020MnCr5 18NiCrMo4 En19: one item of a peer run

        # --- after ---
        start, limit = fe, min(len(snippet), fe + _SPAN_CHARS)
        for s, _e, _form in sib_occ:
            if start <= s < limit:
                limit = s  # the user's rule: nothing past a sibling form is this record's
                break
        while 0 < limit < len(snippet) and snippet[limit - 1].isalnum() and snippet[limit].isalnum():
            limit += 1  # never cut inside a token (ICC 50|0)
        all_toks: list[Token] = [(t.group(0), t.start(), t.end()) for t in _TOKEN_RE.finditer(snippet)]
        # a token the focal occurrence ends inside (JTM-1050EVS/460, Steels): its
        # remainder after the occurrence is the span's first token
        for k, (_tt, ts, te) in enumerate(all_toks):
            if ts < start < te:
                rest = snippet[start:te]
                lead_punct = len(rest) - len(rest.lstrip("/-:."))
                if rest[lead_punct:]:
                    all_toks[k] = (rest[lead_punct:], start + lead_punct, te)
                else:
                    all_toks[k] = (rest, start, te)
                break
        toks = [t for t in all_toks if t[1] >= start and t[2] <= limit]
        first_index = next((k for k, t in enumerate(all_toks) if t[1] >= start), len(all_toks))
        lead = snippet[:fs].rstrip()
        trail = snippet[fe:].lstrip()
        list_item = lead.endswith((",", ";", "/")) or trail.startswith(",")
        demanded.update(
            _walk(
                toks,
                snippet,
                backwards=False,
                all_toks=all_toks,
                first_index=first_index,
                list_item=list_item,
            )
        )

        # --- before ---
        if (
            next_tok
            and (_NAME_RE.match(next_tok) or _ALLCAPS_RE.match(next_tok))
            and next_tok.lower() not in _CONNECTORS
            and next_tok.lower() not in _KIND_WORDS
        ):
            continue  # "MC-600 Machining Center": the focal form opens a longer name
        bstart = max(0, fs - _SPAN_CHARS)
        while 0 < bstart < len(snippet) and snippet[bstart - 1].isalnum() and snippet[bstart].isalnum():
            bstart -= 1
        for _s, e, _form in sib_occ:
            if e <= fs and e > bstart:
                bstart = e
        bspan = snippet[bstart:fs]
        btoks: list[Token] = [
            (t.group(0), t.start() + bstart, t.end() + bstart) for t in _TOKEN_RE.finditer(bspan)
        ]
        btoks.reverse()
        demanded.update(_walk(btoks, snippet, backwards=True))
    return demanded


# --- public API ----------------------------------------------------------------------------------


def designation_shaped_tokens(text: str) -> set[str]:
    """Every designation-shaped token in *text* regardless of owner — the raw
    shape tiers (letter+digit tokens of three or more characters, and
    ALL-CAPS/kind-word + number pairs, space-normalized). A nomination
    vocabulary for the harness and tests; NOT the demand (that is
    ``designation_tokens``)."""
    tokens: set[str] = set()
    for match in _CORE_TOKEN_RE.finditer(text):
        token = match.group(0)
        if len(token) >= _MIN_RAW_TOKEN_LEN:
            tokens.add(token)
    for match in _PAIR_RE.finditer(text):
        head, tail = match.group(1), match.group(2)
        if _YEAR_RE.match(tail) and head not in _KIND_HEADS and not head.isupper():
            continue
        if head.isupper() or head in _KIND_HEADS:
            tokens.add(f"{head} {tail}")
    return tokens


def designation_tokens(
    text: str,
    *,
    focal_form: str,
    sibling_forms: Sequence[str] = (),
) -> set[str]:
    """The designations a record whose focal form is *focal_form* OWNS in
    *text* (one snippet): the tokens inside the focal form plus the spans
    directly around each of its occurrences, cut at *sibling_forms* (the
    member forms of the chunk's OTHER records, same field). Module docstring
    has the rule."""
    demanded = own_tokens(focal_form)
    if focal_form and text:
        sib_occ = _sibling_occurrences(text, focal_form, sibling_forms)
        demanded |= _span_tokens(text, focal_form, sib_occ)
    return demanded


def _demand(
    entry_texts: Iterable[str], focal_form: str, sibling_forms: Sequence[str]
) -> set[str]:
    demanded = own_tokens(focal_form)
    for entry_text in entry_texts:
        demanded |= designation_tokens(entry_text, focal_form=focal_form, sibling_forms=sibling_forms)
    return demanded


def missing_designations(
    entry_texts: Iterable[str],
    synthesis: str,
    *,
    focal_form: str,
    sibling_forms: Sequence[str] = (),
) -> set[str]:
    """The record's OWN designations (``designation_tokens`` over every
    snippet) that *synthesis* does not carry — the conservation check.
    Presence is word-boundary, case-insensitive and spacing-tolerant. Empty
    set = the synthesis conserves every designation it owes."""
    return {
        token
        for token in _demand(entry_texts, focal_form, sibling_forms)
        if not _present(token, synthesis)
    }


def designation_coverage(
    entry_texts: Iterable[str],
    synthesis: str,
    *,
    focal_form: str,
    sibling_forms: Sequence[str] = (),
) -> tuple[int, int]:
    """(present, demanded) counts over the record's own designations — the
    comparator the under-enumeration retry uses to keep the better of two
    answers for one record."""
    demanded = _demand(entry_texts, focal_form, sibling_forms)
    present = sum(1 for token in demanded if _present(token, synthesis))
    return present, len(demanded)
