"""Is a quoted span really in the text it claims to quote?

The Step 2 grounding and screening wires (2026-09-21) carry, per option or
per accepted record, the words of the record that evidence it. The quote IS
the evidence test (V1 as simplified in round 2): a record listed with no
quote is not listed. A quote the record does not contain is NOT a drop
(user decision 2026-09-21: a model's copy can differ by a spelling or a
word beyond the normalisations below) — the grounding parser keeps the label
and marks its evidence rule ``unverified``, a trail the census reads. This is
the one check made on a quote — presence, after the normalisations a model's
copy can legitimately differ by (whitespace, case, curly quotes), and with an
ellipsis allowed to skip words between two verbatim parts. Nothing here
judges whether the quoted words evidence the label; that is the census's.
"""

from __future__ import annotations

import re

_WS = re.compile(r"\s+")
_ELLIPSIS = re.compile(r"\s*(?:\.\.\.|…)\s*")
_QUOTE_MARKS = str.maketrans({"’": "'", "‘": "'", "“": '"', "”": '"'})


def normalise_for_quote(text: str) -> str:
    return _WS.sub(" ", text.translate(_QUOTE_MARKS)).strip().casefold()


def quote_found_in_text(quote: str, *texts: str) -> bool:
    """Every ellipsis-separated part of ``quote`` occurs in the concatenation
    of ``texts`` after normalisation. An empty quote is never found."""
    haystack = normalise_for_quote("\n".join(texts))
    parts = [part for part in _ELLIPSIS.split(normalise_for_quote(quote)) if part]
    return bool(parts) and all(part in haystack for part in parts)
