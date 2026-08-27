"""Reduce a search request's wire text to its scan domain.

The wire text is the exact user-message text a search request carried (after
the nonce line). The scan domain masks what the mention contract also ignores:
URL lines, page-separator lines, and the excluded/continued-page markers —
matching occurrences against unmasked text would count phantom hits (locked
domain decision, 2026-08-21/22). Masking is length-preserving so offsets into
the masked string equal offsets into the wire text."""

from __future__ import annotations

import re
import sys
from pathlib import Path

from core.utils.floor_scan import (  # type: ignore[import-untyped]
    CONTINUED_PAGE_MARKER,
    EXCLUDED_PAGE_MARKER,
)

# Space normalization and form matching are SHARED across stage evaluations —
# see _shared/text_matching.py for why a naive matcher silently returns wrong
# answers on this corpus (NBSPs mid-phrase, whitespace runs, short-form
# substrings). Re-exported here so this module stays the search harness's one
# text-handling entry point.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from _shared.text_matching import normalize_spaces  # noqa: E402,F401

URL_LINE_RE = re.compile(r"^[ \t]*https?://\S+[ \t]*$", re.M)
SEPARATOR_LINE_RE = re.compile(r"^[ \t]*#{10,}[ \t]*$", re.M)

NONCE_PREFIX = "request nonce (ignore): "


def wire_text_from_user_message(user_message: str) -> str:
    """Strip the leading nonce line (and its blank separator) if present.
    Tolerates a nonce line at the END of the message too (the nonce moved
    there by user decision 2026-08-22 — handle both vintages)."""
    text = user_message
    if text.startswith(NONCE_PREFIX):
        cut = text.find("\n\n")
        if cut != -1:
            text = text[cut + 2 :]
    else:
        # trailing-nonce vintage: "...\n\nrequest nonce (ignore): <hex>"
        marker = "\n" + NONCE_PREFIX
        tail = text.rfind(marker)
        if tail != -1 and "\n" not in text[tail + len(marker) :].strip():
            text = text[:tail].rstrip("\n") + "\n"
    return text


def scan_domain(wire_text: str) -> str:
    """The wire text with non-content lines blanked and Unicode spaces
    normalized, length-preserving."""
    wire_text = normalize_spaces(wire_text)
    out = list(wire_text)
    patterns = [
        URL_LINE_RE,
        SEPARATOR_LINE_RE,
        re.compile(re.escape(EXCLUDED_PAGE_MARKER)),
        re.compile(re.escape(CONTINUED_PAGE_MARKER)),
    ]
    for pattern in patterns:
        for match in pattern.finditer(wire_text):
            for i in range(match.start(), match.end()):
                if out[i] != "\n":
                    out[i] = " "
    return "".join(out)
