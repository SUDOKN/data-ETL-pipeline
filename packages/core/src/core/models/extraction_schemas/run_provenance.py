"""What decided a stored run, persisted beside its results (2026-08-27).

The dump has carried this block since 2026-08-13; the database had none of it.
Most of what the dump's header holds is already stored elsewhere and is not
repeated here — the per-field ``metadata`` object IS the dump's
``extraction_metadata``, and the subject document already pins the scraped
text's S3 version and token count. What was nowhere:

* ``page_exclusion`` — the pages the phrase pipelines removed BEFORE chunking.
  The S3 object is the PRE-exclusion text, so without this nobody can say what
  the run actually read. It is also load-bearing for the stored fold: replaying
  the exclusion from these SPANS reconstructs the post-exclusion text exactly,
  with no dependence on the exclusion rule version, which is what lets the fold
  store offsets instead of passages.
* ``run_timestamp`` — the run identity that names the extraction dump
  directory, and so the only join from a stored document to its dump.
* the text's ``last_modified_on``.

Not stored, deliberately: ``partial``, ``stopped_at``, ``stages_run`` and
``stages_disabled``. Stage toggles are hard-stop, so a run that reaches
reconcile had nothing disabled, and a stage-gated run writes no result to the
subject at all. Every document carrying this block is a full run by
construction — the reconcile nodes are the only writers — and a ``partial:
false`` field that could never read anything else is not provenance.

WHERE THIS GOES. Beside ``metadata`` on the results model, never inside it:
``PrefillNode.raise_if_metadata_is_stale`` compares stored metadata against the
live configuration, and a run timestamp buried in there would make every resume
read as drift.
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field

from core.utils.floor_scan import PageExclusion


class StoredDroppedPage(BaseModel):
    """One removed page. Offsets are into the ORIGINAL (pre-exclusion) text,
    matching ``floor_scan.DroppedPage`` — they are what a reader deletes to
    reconstruct what the run chunked."""

    url: str
    start: int
    end: int

    @property
    def chars(self) -> int:
        return self.end - self.start


class StoredPageExclusion(BaseModel):
    """``drop_excluded_pages`` over one subject's text, as stored.

    ``version`` is the rule version that PRODUCED the spans; it is provenance,
    not an input to replaying them. ``apply_to`` deletes the spans and needs no
    rule at all, which is the whole point.
    """

    version: str
    chars_before: int
    chars_removed: int
    chars_after: int
    pages: list[StoredDroppedPage] = Field(default_factory=list)

    @classmethod
    def of(cls, exclusion: PageExclusion) -> "StoredPageExclusion":
        return cls(
            version=exclusion.version,
            chars_before=exclusion.chars_before,
            chars_removed=exclusion.chars_removed,
            chars_after=exclusion.chars_after,
            pages=[
                StoredDroppedPage(url=page.url, start=page.start, end=page.end)
                for page in exclusion.dropped
            ],
        )

    def apply_to(self, original_text: str) -> str:
        """*original_text* with every stored page span removed — the text the
        run chunked, and the text the stored fold's offsets index into.

        Deterministic and rule-free: the spans are deleted back to front so
        earlier offsets stay valid while later ones are cut.
        """
        if len(original_text) != self.chars_before:
            raise ValueError(
                f"page exclusion was recorded over {self.chars_before} characters "
                f"but the given text has {len(original_text)}"
            )
        text = original_text
        for page in sorted(self.pages, key=lambda p: p.start, reverse=True):
            text = text[: page.start] + text[page.end :]
        if len(text) != self.chars_after:
            raise ValueError(
                f"replaying the page exclusion left {len(text)} characters where "
                f"{self.chars_after} were recorded"
            )
        return text


class RunProvenance(BaseModel):
    """The run that produced the results this sits beside."""

    run_timestamp: datetime
    scraped_text_version_id: str
    scraped_text_num_tokens: int
    scraped_text_last_modified_on: datetime
    # None means no trimming ran — the single-stage pipelines read the full
    # text. For a phrase field it is always set.
    page_exclusion: Optional[StoredPageExclusion] = None

    def post_exclusion_text(self, original_text: str) -> str:
        """The text the run's fold offsets resolve against."""
        if self.page_exclusion is None:
            return original_text
        return self.page_exclusion.apply_to(original_text)


def build_run_provenance_record(
    *,
    run_timestamp: datetime,
    scraped_text_file: object,
    page_exclusion: Optional[PageExclusion],
) -> RunProvenance:
    """The stored provenance for one field's completed run.

    Takes the scraped-text file loosely (``object``) for the same reason the
    dump's builder does: ``core`` describes the pipeline, and the concrete
    ``ScrapedMfgFile`` is an app type.
    """
    return RunProvenance(
        run_timestamp=run_timestamp,
        scraped_text_version_id=getattr(scraped_text_file, "s3_version_id"),
        scraped_text_num_tokens=getattr(scraped_text_file, "num_tokens"),
        scraped_text_last_modified_on=getattr(scraped_text_file, "last_modified_on"),
        page_exclusion=(
            StoredPageExclusion.of(page_exclusion) if page_exclusion is not None else None
        ),
    )
