"""Writing the vocabulary candidates a run produced (Step 2, 2026-09-22).

One writer, called from the concept reconcile node: every proposal from every
stage — the grounding call, the proposal pass, descent's sibling proposals,
the leaf steps — with its records, quotes, sources and the proposal wave's
verdict. Upsert by (subject, field, run, parent, label): reconcile is
re-entrant. GUARDED like the run record (``extraction_run_service``): a lost
candidate costs review material, raising would cost the run's results.
Encoded the way Beanie encodes a ``save()`` — never ``model_dump``.
"""

from __future__ import annotations

import logging
from typing import Sequence

from beanie.odm.utils.encoder import Encoder

from core.db_models.vocabulary_candidates import VocabularyCandidate

logger = logging.getLogger(__name__)


def encode_vocabulary_candidate(candidate: VocabularyCandidate) -> dict:
    return Encoder(exclude={"_id", "id", "revision_id"}, to_db=True).encode(candidate)


async def save_vocabulary_candidates(candidates: Sequence[VocabularyCandidate]) -> int:
    """Upsert every candidate; returns how many writes succeeded."""
    written = 0
    for candidate in candidates:
        try:
            document = encode_vocabulary_candidate(candidate)
            await VocabularyCandidate.get_pymongo_collection().update_one(
                {
                    "subject_unique_id": candidate.subject_unique_id,
                    "field_name": candidate.field_name,
                    "run_timestamp": candidate.run_timestamp,
                    "parent_label": candidate.parent_label,
                    "label": candidate.label,
                },
                {"$set": document},
                upsert=True,
            )
            written += 1
        except Exception as write_error:  # noqa: BLE001 — candidates must not sink a run
            logger.error(
                f"[{candidate.subject_unique_id}] could not record the vocabulary candidate "
                f"{candidate.label!r} for '{candidate.field_name}': {write_error}",
                exc_info=True,
            )
    return written
