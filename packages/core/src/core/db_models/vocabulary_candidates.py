"""Leaf-step proposals kept aside as candidates for expanding the vocabulary.

Step 2 of the grounding redesign (design draft §6.5, user decision 2026-09-20):
descent does not stop at a label that has no children. An accepted leaf label
gets exactly one more request in which the model may only PROPOSE a narrower
kind the record names, with the record's own words as the quote. What comes
back never enters the graph — it is not screened, not shipped as a tag and not
descended from — and is stored here, with the records and quotes that carry
it, as the raw material people review when the vocabulary grows.

Kept apart from ``OutOfVocabLabel`` on purpose: that collection holds labels a
person accepted in a mapping correction; this one holds what the model
proposed under a named parent, on a named run, with its evidence. Merging the
two would make an unreviewed proposal look like an accepted one.
"""

from __future__ import annotations

from datetime import datetime

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
    subject_unique_id: str
    field_name: str
    run_timestamp: datetime
    ontology_version_id: str
    # The accepted leaf label the proposal was made under, by its vocabulary
    # name (never an other name), and the proposal as the model spelled it.
    parent_label: str
    label: str
    records: list[CandidateEvidence] = Field(default_factory=list)
    created_at: datetime

    class Settings:
        name = "vocabulary_candidates"


"""
Indexes in MongoDB for VocabularyCandidate (one proposal per parent per run):

db.vocabulary_candidates.createIndex(
  { subject_unique_id: 1, field_name: 1, run_timestamp: 1, parent_label: 1, label: 1 },
  { name: "vocabulary_candidates_unique_idx", unique: true }
)
db.vocabulary_candidates.createIndex(
  { field_name: 1, parent_label: 1 },
  { name: "vocabulary_candidates_parent_idx" }
)
"""
