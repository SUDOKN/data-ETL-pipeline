"""One completed extraction of one field, kept per run (2026-08-27).

THIS IS THE RECORD; the subject document is a CACHE of the latest run.

``Manufacturer`` holds one result per field and is ``save()``d over on every
run, so it can only ever answer "what does this manufacturer look like now".
That is the question almost every reader asks, which is why the cache exists
and why it stays complete enough to answer without a second lookup. This
collection answers "what did run X do", which nothing else can: results are a
function of the rule catalog, the ontology version, the screening logic and the
descent walk, so re-deriving an old run's results from its stored responses
with today's code produces today's answer, not that run's. The aggregation fold
inside them has the same property one stage earlier.

The invariant that makes "cache" precise, and which is checkable with a query
because the cached copy carries its own ``run_provenance.run_timestamp``::

    manufacturer.<field>  ==  the results of the ExtractionRun with the
                              greatest run_timestamp for (subject, field)

Write order follows from it: the record is written BEFORE the subject is saved,
so a crash between the two leaves a record with a stale cache — which the next
run repairs — rather than a cache with no record, which nothing can.

Phrase fields only. The single-stage fields (binary classification, addresses,
business description) have no fold, no page exclusion, and nothing else a
re-parse would not reproduce; their result models also live in the app rather
than in ``core``.

Written only by the reconcile nodes, so every document here is a full run — see
``core.models.extraction_schemas.run_provenance``.
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal, Union

from beanie import Document
from pydantic import model_validator

from core.models.extraction_results.llm_phrase_extraction_results import (
    ConceptExtractionResults,
    KeywordExtractionResults,
)
from core.models.extraction_schemas.run_provenance import RunProvenance

FieldFamily = Literal["concept", "keyword"]

_RESULTS_TYPE: dict[str, type] = {
    "concept": ConceptExtractionResults,
    "keyword": KeywordExtractionResults,
}


class ExtractionRun(Document):
    subject_unique_id: str
    # ``ExtractionFieldType.name`` — a string rather than the enum because the
    # concrete field types are app policy, exactly as in OutOfVocabLabel.
    field_name: str
    run_timestamp: datetime

    # Which arm of ``results`` this document carries. The two results models
    # have no discriminator of their own, and while they are structurally
    # exclusive today (concept stats require ``brute_search``, keyword stats
    # require ``llm_phrase_freehand_grounding``), resolving a stored union by
    # what happens to be required is a trap the first time either model
    # changes. The tag is explicit and the validator below holds it to the
    # parsed type.
    field_family: FieldFamily

    run_provenance: RunProvenance
    # The whole result: the final concepts, the per-chunk stats, and — inside
    # those stats — that chunk's ``aggregation_fold``. There is no separate
    # fold field: it would be the same object stored twice in one document.
    results: Union[ConceptExtractionResults, KeywordExtractionResults]

    @model_validator(mode="after")
    def check_family_matches_results(self) -> "ExtractionRun":
        expected = _RESULTS_TYPE[self.field_family]
        if not isinstance(self.results, expected):
            raise ValueError(
                f"field_family {self.field_family!r} expects "
                f"{expected.__name__}, got {type(self.results).__name__}"
            )
        return self

    class Settings:
        name = "extraction_runs"


"""
Indices for ExtractionRun

db.extraction_runs.createIndex(
  {
    subject_unique_id: 1,
    field_name: 1,
    run_timestamp: 1,
  },
  {
    name: "extraction_runs_subject_field_run_unique_idx",
    unique: true
  }
);
db.extraction_runs.createIndex(
  { run_timestamp: -1 },
  { name: "extraction_runs_recent_idx" }
);
"""
