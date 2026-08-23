"""The masked record id, and the masking of relationship records under it.

A record_id is a pure function of the phrase string alone — ``r`` plus 7
lowercase base36 characters of its sha256 — and deliberately NOT positional.
Position-derived ids (``R1``, ``R2``) reassign silently whenever an upstream
change adds or reorders a phrase, which turns the known stale-replay hazard
(custom ids ignore upstream content) into silent misattribution; a content-
derived id is immutable per phrase, so a stale replay stays merely stale. It
also means the same phrase carries the same id in every chunk, which makes
cross-chunk occurrences of one phrase joinable for free.

The id is what downstream stages show the LLM in place of the phrase as the
GROUPING KEY. This is key-masking only: the mention forms inside a record
necessarily carry the phrase's wording, and must — grounding cannot work
otherwise. What the mask prevents is the output key reading like a candidate
label, which is the confusion it exists to remove.

Random-looking rather than sequential on purpose: a model that mangles
``r4k9x2m`` almost certainly matches nothing and fails loudly at the hold,
where a model that writes ``record_12`` for ``record_13`` lands a verdict on a
real neighboring phrase, silently.
"""

from __future__ import annotations

import hashlib
from typing import Iterable

from core.models.extraction_schemas.relationship import (
    LLMPhraseRelationshipRecords,
    MaskedLLMPhraseRelationshipResults,
    MaskedPhraseRelationshipRecord,
)

RECORD_ID_PREFIX = "r"
RECORD_ID_BODY_LENGTH = 7
# v3 mention ids (the Location stage's wire key, PIPELINE_V3_PLAN.md 2026-08-22):
# same construction over the verbatim snippet text, a different prefix so the
# two id kinds can never be mistaken for one another in a dump or a request.
MENTION_ID_PREFIX = "m"

_BASE36 = "0123456789abcdefghijklmnopqrstuvwxyz"
_ID_SPACE = 36**RECORD_ID_BODY_LENGTH


class RecordIdCollisionError(ValueError):
    """Two distinct phrases hashed to the same record id.

    Deterministic, so the same pair collides on every run: the remedy is to
    raise ``RECORD_ID_BODY_LENGTH`` (a wire-format change that re-dispatches
    everything), not to retry. At 7 base36 characters the chance of any
    collision within a chunk of 300 phrases is under one in a million.
    """


def _content_id(prefix: str, text: str) -> str:
    digest = hashlib.sha256(text.encode("utf-8")).digest()
    n = int.from_bytes(digest, "big") % _ID_SPACE
    chars = []
    for _ in range(RECORD_ID_BODY_LENGTH):
        n, rem = divmod(n, 36)
        chars.append(_BASE36[rem])
    return prefix + "".join(reversed(chars))


def record_id_for_phrase(phrase: str) -> str:
    """The phrase's record id. Pure, exact-string identity: case variants are
    distinct phrases and get distinct ids (per-chunk case-dedup upstream keeps
    case-twins from ever sharing a request)."""
    return _content_id(RECORD_ID_PREFIX, phrase)


def mention_id_for_snippet(snippet: str) -> str:
    """The id a verbatim snippet rides the Location wire under. Pure, exact-
    string identity over the snippet text; the fold checks per window that two
    distinct snippets never share one (``MentionIdCollisionError``)."""
    return _content_id(MENTION_ID_PREFIX, snippet)


class MentionIdCollisionError(ValueError):
    """Two distinct snippets in one window hashed to the same mention id —
    deterministic, so the remedy is ``RECORD_ID_BODY_LENGTH``, not a retry."""


def assign_record_ids(phrases: Iterable[str]) -> dict[str, str]:
    """``phrase -> record_id`` over *phrases*, insertion-ordered.

    Raises :class:`RecordIdCollisionError` naming both phrases when two
    distinct phrases share an id — within one map an id must address exactly
    one phrase, and a silent overwrite would fuse two phrases' trails.
    """
    ids: dict[str, str] = {}
    phrase_by_id: dict[str, str] = {}
    for phrase in phrases:
        if phrase in ids:
            continue
        record_id = record_id_for_phrase(phrase)
        clashing = phrase_by_id.get(record_id)
        if clashing is not None and clashing != phrase:
            raise RecordIdCollisionError(
                f"record id {record_id!r} is shared by two distinct phrases: "
                f"{clashing!r} and {phrase!r}. The id is deterministic, so this "
                f"pair collides on every run; raise RECORD_ID_BODY_LENGTH."
            )
        ids[phrase] = record_id
        phrase_by_id[record_id] = phrase
    return ids


def mask_relationship_records(
    records: LLMPhraseRelationshipRecords,
) -> MaskedLLMPhraseRelationshipResults:
    """The stored (masked) shape of the relationship stage's parse result.

    The phrase rides inside each entry, so the id→phrase join is persisted with
    the evidence it masks and needs no side table.
    """
    ids = assign_record_ids(records.keys())
    return {
        ids[phrase]: MaskedPhraseRelationshipRecord(phrase=phrase, record=record)
        for phrase, record in records.items()
    }


def phrases_by_record_id(
    masked: MaskedLLMPhraseRelationshipResults,
) -> dict[str, str]:
    """``record_id -> phrase``, the join every id-keyed stage result reads
    through."""
    return {record_id: entry.phrase for record_id, entry in masked.items()}
