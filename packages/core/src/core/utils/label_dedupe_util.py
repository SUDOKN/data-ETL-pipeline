"""Dedupe of near-identical labels at the final merge boundaries.

Chunks are extracted independently, so their union carries variants of one
label — "Tooling" beside "tooling", "metal stamping" beside "metal stampings".
Per-chunk stats keep the raw values for audit; only the FINAL merged results
are collapsed here. Candidates are considered in sorted order so the surviving
variant does not depend on set iteration order.
"""

from __future__ import annotations

from typing import Iterable


def _plural_insensitive_key(label: str) -> str:
    """Casefolded, whitespace-collapsed key with each word's trailing plural 's'
    stripped. Words of three letters or fewer and words ending in a double 's'
    keep their 's': "gas" and "press" are not plurals of anything here."""
    words = label.casefold().split()
    stripped = [
        word[:-1] if word.endswith("s") and not word.endswith("ss") and len(word) > 3
        else word
        for word in words
    ]
    return " ".join(stripped)


def dedupe_case_insensitive(labels: Iterable[str]) -> set[str]:
    """Collapse labels differing only in case or surrounding whitespace. Of the
    variants, the sorted-first one survives (uppercase before lowercase)."""
    by_key: dict[str, str] = {}
    for label in sorted(labels):
        by_key.setdefault(label.casefold().strip(), label)
    return set(by_key.values())


def dedupe_equivalent_keywords(keywords: Iterable[str]) -> set[str]:
    """Collapse keywords differing only by case, whitespace, or per-word plural
    's' — "metal stamping"/"metal stampings" are one keyword. The shortest
    variant survives (ties broken lexicographically), which prefers the
    singular form."""
    by_key: dict[str, str] = {}
    for keyword in sorted(keywords, key=lambda k: (len(k), k)):
        by_key.setdefault(_plural_insensitive_key(keyword), keyword)
    return set(by_key.values())
