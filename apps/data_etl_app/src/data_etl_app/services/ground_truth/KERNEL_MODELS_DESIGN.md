# Kernel base models — for review before K1

**2026-08-13 · branch `new-ground-truth` · status: DESIGN, nothing built.**
Written because "provenance as metadata vs provenance in the key" is easier to settle
by reading models than by reading prose. This document is the spec for steps K1–K5 in
[README.md](README.md). Nothing here is code yet.

---

## §0 · What "identity keying" means, concretely

It is not an abstraction. It is **the set of fields in a collection's unique index**,
and it decides one thing: *when you look up ground truth, what must you supply — and
if any of those values is new, do you find the old label or an empty slot?*

Your current binary collection already answers that, and its answer is the thing the
redesign objects to. From [db_models/binary_ground_truth.py](apps/data_etl_app/src/data_etl_app/db_models/binary_ground_truth.py):

```js
db.binary_ground_truths.createIndex(
  { mfg_etld1: 1,
    scraped_text_file_version_id: 1,
    "metadata.prompt_version_id": 1,     // ← this is identity keying by prompt version
    classification_type: 1 },
  { name: "binary_gt_unique_idx", unique: true })
```

`metadata.prompt_version_id` sits **inside the unique index**. That is what "keyed by
provenance" means. The consequence is mechanical, not philosophical:

| | keyed by pv (today) | pv as metadata (proposal) |
|---|---|---|
| Publish a new prompt, look up GT for anchor-mfg | **no document found** — the key no longer matches | same document found |
| Human labor to get a label back | re-annotate from scratch | none |
| "Show me only labels made under catalog ≥ v7" | already partitioned | one query: `find({"provenance.extraction_metadata...catalog_version": …})` |
| "Merge labels across two prompt versions" | **impossible** — no way to know two docs are the same text+question without re-deriving it | they were never split |

The asymmetry in the last two rows is the whole argument: **metadata keeps the option
to partition later; a key spends it now, irreversibly.**

## §1 · Your objection, taken seriously

> *There is no way to tell that the prompt edit was unrelated. In the absence of that,
> the prompt version ID and the change in it is the decision boundary.*

You are right that edits cannot be classified as related or unrelated — nothing can do
that reliably, and I would not build anything that tries. Three responses, and the
first one concedes a real part of the point.

**(a) You are right for one of the two record types.** A judgment about *the run's
behavior* — "this screening verdict was wrong", "rule SCR-2 mis-fired here" — is
genuinely about the prompt that produced it, and a pv change genuinely ends its
validity. Those records should be run-scoped, and in both the keyword and concept
designs they already are (keyword Layer B `KeywordRunAudit`, concept `candidate_ref`).
Nothing in this proposal asks you to reuse those across a prompt edit.

The other type is a judgment about *the text*: "this span proves they operate their own
presses", "this phrase names a real piece of equipment they own." A prompt edit has no
causal path to that. The human read the text, not the prompt.

**(b) Nothing ever needs to classify an edit.** This is the part I explained badly
before. Reuse is not decided by asking "was this edit relevant?" It is decided by
joining on the thing the judgment was *about*:

- New run emits phrase `"CNC press brake"`, and a human already judged that exact
  phrase against this exact text → the prior judgment applies, because the phrase and
  the text are byte-identical. The prompt that emitted it is irrelevant to whether the
  phrase names a real machine.
- New run emits `"press brake with CNC control"`, which nobody has judged → it lands in
  the delta queue and a human judges it.

No edit is ever classified. The join either matches or it doesn't, and the unmatched
remainder is exactly the work. That is the delta-queue economics both designs rest on.

**(c) A pv-keyed truth would be keyed to something this system does not retain.** Recon
finding: there is **no `run_id` or `batch_id` anywhere** in the models or db_models, and
`Manufacturer` stores one `Optional[…ExtractionResults]` slot per field, overwritten on
the next run. Previous runs are not kept. So keying truth to a run points it at
something that has already been discarded — while the metadata block, which *is* the
only run identity this codebase has ever had (it is what
`ExtractionNodeMetadata.to_custom_id_segment()` builds request identity from), is fully
captured by §3 below.

**The upshot:** you lose nothing by keeping provenance out of the key, because §3
captures it completely and a tripwire test keeps it complete. If you later decide a
catalog bump invalidated a slice of labels, that is a query and a re-adjudication — the
conservative move stays available forever. Keyed, it is taken up front and cannot be
undone.

---

## §2 · Primitives — `core/models/ground_truth/primitives.py`

Field-type agnostic by necessity: `packages/core` cannot import `data_etl_app`, where
`ConceptTypeEnum` / `KeywordTypeEnum` / `BinaryClassificationTypeEnum` live. Instruments
bind those in their own app-side Documents.

```python
from __future__ import annotations

from enum import Enum
from pydantic import BaseModel, Field, model_validator


class EvidenceSpan(BaseModel):
    """A character range in the scraped text, plus the text that was there.

    Ranges are half-open — ``[start, end)``, Python slice semantics — so
    ``text[span.start:span.end]`` is the quote and adjacent spans do not overlap.

    Offsets are into the FULL scraped text at a pinned S3 version, never into a
    chunk. That is what makes a chunking change unable to move them, and what
    lets per-chunk truth be derived for any chunking by intersection.

    ``quote`` is denormalized on purpose: it is the second witness. If the text
    a span points into ever changes underneath it, ``quote != text[start:end]``
    says so immediately, years later, without needing the original text at hand.
    """

    start: int = Field(ge=0)
    end: int = Field(gt=0)
    quote: str = Field(min_length=1)

    @model_validator(mode="after")
    def _check_shape(self) -> EvidenceSpan:
        if self.end <= self.start:
            raise ValueError(f"end must exceed start; got [{self.start}, {self.end})")
        # Text-free invariant: catches a truncated or re-wrapped quote at write
        # time, before any text is downloaded. The against-the-text check is
        # separate (K2) because it needs the snapshot.
        if self.end - self.start != len(self.quote):
            raise ValueError(
                f"span width {self.end - self.start} != quote length {len(self.quote)}"
            )
        return self


class ReviewedSpan(BaseModel):
    """A character range a human actually read.

    Recall is scored only inside these. Without them, text nobody reviewed —
    e.g. the tail dropped by ``max_chunks`` — silently counts as a miss against
    whichever side you happen to be measuring.
    """

    start: int = Field(ge=0)
    end: int = Field(gt=0)

    @model_validator(mode="after")
    def _check_shape(self) -> ReviewedSpan:
        if self.end <= self.start:
            raise ValueError(f"end must exceed start; got [{self.start}, {self.end})")
        return self


class Split(str, Enum):
    """Which subjects headline numbers may be computed from.

    ``dev`` subjects have been prompt-tuning targets and their scores are not
    reportable — anchor-mfg and steelcraft are dev by construction. Recorded on
    the document, not in someone's memory.
    """

    dev = "dev"
    test = "test"
```

## §3 · Provenance — `core/models/ground_truth/provenance.py`

The model that carries your comprehensiveness requirement.

```python
from __future__ import annotations

from datetime import datetime
from typing import Generic, TypeVar

from pydantic import BaseModel

from core.field_types import SubjectUniqueIDType
from core.models.extraction_results.llm_phrase_extraction_results import (
    BaseExtractionMetadata,
)
from infra.field_types import S3FileVersionIDType


class TextSnapshotWitness(BaseModel):
    """Pin the exact text, and be able to prove later that it has not changed.

    ``char_len`` is characters, not bytes: every offset in every span is a
    Python string index, and mixing the two silently corrupts spans on any text
    containing non-ASCII.
    """

    subject_unique_id: SubjectUniqueIDType
    scraped_text_file_version_id: S3FileVersionIDType
    sha256: str
    char_len: int


ExtractionMetadataT = TypeVar("ExtractionMetadataT", bound=BaseExtractionMetadata)


class RunProvenance(BaseModel, Generic[ExtractionMetadataT]):
    """Everything needed to reproduce the run that suggested this judgment.

    The metadata block is captured WHOLE rather than re-listed field by field.
    Re-listing is how a provenance record goes stale in silence: a field added
    to ``ExtractionNodeMetadata`` next month would simply not be here, and
    nothing would fail. Capturing the model itself means the record grows with
    the pipeline, and the K4 tripwire test fails the day it stops doing so.

    Bound to ``BaseExtractionMetadata``, so a binding supplies its instrument's
    exact type and loses nothing to a widened base:

        RunProvenance[LLMSingleStageExtractionMetadata]   # binary
        RunProvenance[KeywordExtractionMetadata]          # keyword
        RunProvenance[ConceptExtractionMetadata]          # concept

    What that captures today, transitively: per-node ``prompt_version_id`` (the
    S3 version) and ``catalog_version`` for every stage, ``llm_model``,
    ``model_params``, ``prompt_name``, each node's batching caps and round caps,
    plus ``chunk_strat`` and ``ontology_version_id`` from the base. In this
    codebase that IS run identity — there is no run_id or batch_id anywhere, and
    ``to_custom_id_segment()`` builds request identity out of exactly these
    fields.

    ``run_snapshot`` is deliberately separate from the annotator's own snapshot
    witness. They should normally be equal; when they are not, the run read a
    different version of the text than the human did, and that is a fact worth
    detecting rather than assuming away.
    """

    extraction_metadata: ExtractionMetadataT
    run_snapshot: TextSnapshotWitness
    captured_at: datetime
```

### Why generic rather than a union or a flattened copy

| Option | Problem |
|---|---|
| Flatten fields into `RunProvenance` by hand | Goes stale silently; exactly the failure the tripwire exists to prevent |
| `extraction_metadata: BaseExtractionMetadata` | Pydantic deserializes back to the *base*; the per-node prompt versions are lost on read |
| Discriminated union | Needs a literal tag added to every existing metadata class — invasive, and buys nothing over a generic |
| **Generic (proposed)** | Each Document binds one concrete type; serialization round-trips exactly; precedent already exists (`SingleStageExtractionResults` is `Generic`) |

## §4 · Authorship — `core/models/ground_truth/authorship.py`

```python
class Authorship(BaseModel):
    """Who decided, and when. Embedded in every append-only record.

    Deliberately not a ``DecisionLogEntry`` with a ``payload: dict``: a weakly
    typed payload is a place for instrument-specific meaning to hide. Each
    instrument declares its own strongly typed record and embeds this.
    """

    author_email: str
    at: datetime
```

## §5 · Worked example — how a binary GT document uses them

**Branch work (step B2), shown only to make the key concrete.** Note that
`RunProvenance` does **not** appear on the truth document at all: no model output
touches the truth-making path. It appears on task and eval-side records.

```python
class BinaryGroundTruth(Document):
    # ---- identity: exactly these three fields form the unique index ----
    mfg_etld1: SubjectUniqueIDType
    classification_type: BinaryClassificationTypeEnum
    scraped_text_file_version_id: S3FileVersionIDType

    # ---- the human result ----
    final_decision: Literal["true", "false", "unresolvable"]
    resolution: Literal["unanimous", "adjudicated", "provisional"]
    evidence: list[EvidenceSpan]
    split: Split
    snapshot: TextSnapshotWitness
    contributing_annotation_ids: list[PydanticObjectId]
```

```js
// proposal — three fields, no provenance
db.binary_ground_truths.createIndex(
  { mfg_etld1: 1, classification_type: 1, scraped_text_file_version_id: 1 },
  { name: "binary_gt_identity_idx", unique: true })
```

The eval-side record is where a run enters, and it carries provenance in full:

```python
class BinaryModelObservation(Document):
    """One run's answer, for joining against truth. Disposable with the run."""

    mfg_etld1: SubjectUniqueIDType
    classification_type: BinaryClassificationTypeEnum
    scraped_text_file_version_id: S3FileVersionIDType

    answer: bool
    confidence: int
    provenance: RunProvenance[LLMSingleStageExtractionMetadata]
```

Truth is written once per (subject, question, text). Observations accumulate, one per
run, each fully reproducible. The eval joins them on the three shared fields and slices
by anything inside `provenance` — per prompt version, per catalog version, per model.

---

## §6 · The decision this document is asking for

**Option A (proposed).** Identity = `(subject, field/question, text version)`.
Provenance complete, on run-scoped records, never in the key.

**Option B (your instinct).** Add `prompt_version_id` — and, for consistency,
`catalog_version` and the rest — to the unique index of the truth document itself.

If you want B, say so and I will build B; it is a two-line change to an index
definition and a field on a document, and the models above are otherwise unchanged.
What I want on the record before that choice is the cost: under B, the 2026-08-11 day
that bumped seven catalog versions would have orphaned every label collected before it,
and the keyword spec's measured cadence — twelve prompt edits in one week — sets how
often that happens.

A middle option exists if the pull is toward auditability rather than partitioning:
**A plus a `provenance_at_annotation` field** on the truth document, recording which
run's candidates the human was looking at when they judged. Fully auditable, fully
queryable, still one document per text — you can always ask "which labels were made
while looking at catalog v6 output?" and re-adjudicate exactly those.
