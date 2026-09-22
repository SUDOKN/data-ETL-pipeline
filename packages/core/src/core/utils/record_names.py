"""Record names (2026-09-22, user decision): a record is named by its
POSITION in the chunk, in words — ``record-one``, ``record-twenty-one``,
``record-three-hundred-twelve`` — and that name is both what the model reads
and echoes and what every result is keyed by. Numbering runs over the whole
chunk in the fold's order and continues across requests, so a request that
carries records fifty-one to seventy-five names them exactly so.

Why words and not digits: no two number words are one character apart, so a
copying slip cannot land on a neighbouring record the way ``R11`` for ``R17``
would. Why positions and not content hashes: the hashes were unreadable and
were copied wrong about 0.3% of the time (run 20260922T202843); a renumbering
after an upstream change is caught by the request digest (the payload carries
the names), which re-asks rather than misattributes.
"""

from __future__ import annotations

from typing import Optional

RECORD_NAME_PREFIX = "record-"

_ONES = [
    "zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine",
    "ten", "eleven", "twelve", "thirteen", "fourteen", "fifteen", "sixteen",
    "seventeen", "eighteen", "nineteen",
]
_TENS = ["", "", "twenty", "thirty", "forty", "fifty", "sixty", "seventy", "eighty", "ninety"]


def number_words(n: int) -> str:
    """``n`` in hyphenated English words, 1 ≤ n < 1,000,000."""
    if n < 1 or n >= 1_000_000:
        raise ValueError(f"number_words: {n} is out of the supported range 1..999999")
    parts: list[str] = []
    thousands, rest = divmod(n, 1000)
    if thousands:
        parts.append(f"{_below_thousand(thousands)}-thousand")
    if rest or not thousands:
        parts.append(_below_thousand(rest))
    return "-".join(p for p in parts if p)


def _below_thousand(n: int) -> str:
    parts: list[str] = []
    hundreds, rest = divmod(n, 100)
    if hundreds:
        parts.append(f"{_ONES[hundreds]}-hundred")
    if rest:
        if rest < 20:
            parts.append(_ONES[rest])
        else:
            tens, ones = divmod(rest, 10)
            parts.append(_TENS[tens] + (f"-{_ONES[ones]}" if ones else ""))
    return "-".join(parts)


def record_name(position: int) -> str:
    """The name of the record at 1-based ``position`` in its chunk."""
    return RECORD_NAME_PREFIX + number_words(position)


_WORD_VALUE = {w: i for i, w in enumerate(_ONES)} | {w: i * 10 for i, w in enumerate(_TENS) if w}


def record_position(name: str) -> Optional[int]:
    """The 1-based position a record name denotes, or None when the string
    is not a well-formed record name."""
    if not name.startswith(RECORD_NAME_PREFIX):
        return None
    words = name[len(RECORD_NAME_PREFIX):].split("-")
    total = 0
    current = 0
    for w in words:
        if w == "hundred":
            if current == 0:
                return None
            current *= 100
        elif w == "thousand":
            if current == 0:
                return None
            total += current * 1000
            current = 0
        elif w in _WORD_VALUE:
            current += _WORD_VALUE[w]
        else:
            return None
    value = total + current
    return value if value >= 1 and record_name(value) == name else None
