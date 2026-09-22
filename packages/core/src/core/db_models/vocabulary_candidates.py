"""Proposals kept aside as candidates for expanding the vocabulary.

Step 2 of the grounding redesign (design draft §6.5, user decisions 2026-09-20
and 2026-09-22): every label the model PROPOSES — from the grounding call,
the proposal pass, a descent's sibling proposals, or the leaf step under an
accepted label that has no children — is screened in one proposal wave after
the last depth wave and stored here with the records and quotes that carry
it, its sources and its verdict, as the raw material people review when the
vocabulary grows. An accepted proposal also ships as an out-of-vocabulary
result of the run; a rejected one lives only here. Nothing here enters the
graph or is descended from.

Kept apart from ``OutOfVocabLabel`` on purpose: that collection holds labels a
person accepted in a mapping correction; this one holds what the model
proposed on a named run, with its evidence. Merging the two would make an
unreviewed proposal look like an accepted one.
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from beanie import Document
from pydantic import BaseModel, Field


class CandidateEvidence(BaseModel):
    record_id: str
    # The record's subject as the site names it, and the words of that record
    # the model quoted for the proposal — the same evidence test grounding
    # applies (an empty or absent quote drops the record before storage).
    focal_form: str
    quote: str


class VocabularyCandidate(Document):
    """One proposal of one run, for one subject and field: every proposal from
    every stage lands here with its verdict (2026-09-22 — no longer the leaf
    step's alone), keyed by (subject, field, run, parent, label)."""

    subject_unique_id: str
    field_name: str
    run_timestamp: datetime
    ontology_version_id: str
    # The accepted parent the proposal was made under (descent or leaf step),
    # by its vocabulary name — "" for the grounding call's and the proposal
    # pass's — and the proposal as the model spelled it.
    parent_label: str
    label: str
    records: list[CandidateEvidence] = Field(default_factory=list)
    # Where the proposal came from ("grounding", "proposal_pass",
    # "descent:<parent>", "leaf:<parent>"), and the proposal wave's verdict
    # (user decision 2026-09-22: every proposal is screened; an accepted one
    # also ships as an out-of-vocabulary result, a rejected one is only here).
    sources: list[str] = Field(default_factory=list)
    accepted: Optional[bool] = None
    failed_rule: Optional[str] = None
    created_at: datetime

    class Settings:
        name = "vocabulary_candidates"


"""
Indexes in MongoDB for VocabularyCandidate (one proposal per parent per run;
parent_label is "" for a proposal the grounding call or the proposal pass made):

db.vocabulary_candidates.createIndex(
  { subject_unique_id: 1, field_name: 1, run_timestamp: 1, parent_label: 1, label: 1 },
  { name: "vocabulary_candidates_unique_idx", unique: true }
)
db.vocabulary_candidates.createIndex(
  { field_name: 1, parent_label: 1 },
  { name: "vocabulary_candidates_parent_idx" }
)
"""
