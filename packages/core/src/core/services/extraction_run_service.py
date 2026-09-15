"""Writing the per-run extraction record (2026-08-27).

One writer, called from the concept and keyword reconcile nodes and nowhere
else — which is what makes "every document in ``extraction_runs`` is a full
run" true by construction rather than by a stored flag.

Upsert rather than insert: reconcile is re-entrant (a resumed subject can reach
it again for the same run timestamp), and the unique index on (subject, field,
run) turns a second insert into a crash instead of the no-op it should be.

GUARDED, even though this collection is the record and the subject is the
cache. The trade is deliberate and it is not symmetric: a failed write here
costs this run's history — real, and unrecoverable — while raising would cost
the run's RESULTS, which are already computed and paid for in model spend. The
cache is what every reader hits; losing the record loses analysis, losing the
results loses the extraction. So this logs loudly and lets the run finish.

Contrast ``build_stored_fold``, which is NOT guarded: it raises only when a
window's bounds do not describe its own text, and offsets written from a fold
that cannot locate its windows resolve to the wrong passage. Storing a
confident lie is worse than failing.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Union

from beanie.odm.utils.encoder import Encoder

from core.db_models.extraction_run import ExtractionRun, FieldFamily
from core.models.extraction_results.llm_phrase_extraction_results import (
    ConceptExtractionResults,
    KeywordExtractionResults,
)
from core.models.extraction_schemas.run_provenance import RunProvenance

logger = logging.getLogger(__name__)


async def save_extraction_run(
    *,
    subject_unique_id: str,
    field_name: str,
    field_family: FieldFamily,
    run_timestamp: datetime,
    run_provenance: RunProvenance,
    results: Union[ConceptExtractionResults, KeywordExtractionResults],
) -> None:
    """Record what this run produced, keyed by (subject, field, run).

    Called BEFORE the subject is saved: a crash between the two leaves a record
    with a stale cache, which the next run repairs, rather than a cache with no
    record, which nothing can.
    """
    run = ExtractionRun(
        subject_unique_id=subject_unique_id,
        field_name=field_name,
        field_family=field_family,
        run_timestamp=run_timestamp,
        run_provenance=run_provenance,
        results=results,
    )
    try:
        document = encode_extraction_run(run)
        await ExtractionRun.get_pymongo_collection().update_one(
            {
                "subject_unique_id": subject_unique_id,
                "field_name": field_name,
                "run_timestamp": run_timestamp,
            },
            {"$set": document},
            upsert=True,
        )
    except Exception as write_error:  # noqa: BLE001 — history must not sink a run
        logger.error(
            f"[{subject_unique_id}] could not record the extraction run for "
            f"'{field_name}' at {run_timestamp.isoformat()}; the results still "
            f"reach the subject, but this run leaves no history: {write_error}",
            exc_info=True,
        )


def encode_extraction_run(run: ExtractionRun) -> dict:
    """The run as a BSON-ready document, encoded the way Beanie encodes a
    ``save()`` — NOT ``model_dump(mode="python")``.

    Found on the first full-tail run after this service landed (2026-09-14, run
    20260915T020646): ``model_dump`` keeps ``set[str]`` fields as Python sets,
    which BSON refuses ("cannot encode object: set()") — every keyword field's
    history silently failed the write — and it cannot serialize the descent
    result at all (``dict[int, set[IterativelyTaggedPhraseGroup]]``: a set of
    models dumps to a set of dicts, ``TypeError: unhashable type: 'dict'``),
    which sank every concept field's run because the dump sat outside the
    guard. Beanie's encoder turns sets into lists and models into dicts, keeps
    datetimes as datetimes (the upsert key must match the stored value), and
    is what the subject document's own ``save()`` goes through (``to_db=True``,
    as ``Document.save`` calls it), so the record and the cache carry the same
    shape and read back as the same model — pinned by the round-trip test.
    """
    # Beanie's encoder keys by database alias, so the document id is ``_id``
    # here, not ``id`` — excluding only ``id`` left ``_id: null`` in the $set,
    # which the collection's schema refuses (run 20260915T023502).
    return Encoder(exclude={"_id", "id", "revision_id"}, to_db=True).encode(run)
