from __future__ import annotations

from pydantic import BaseModel, ConfigDict

# Stage 2, v1 — one prose description per phrase. Retired when the v2 node
# service lands (pipeline v2, PIPELINE_V2_PLAN.md phase 2.1); kept so v1 stays
# runnable until then.
LLMPhraseRelationshipResults = dict[
    str, str
]  # Describe relationship between phrases extracted and the extraction subject.


class PhraseRelationshipEntry(BaseModel):
    model_config = ConfigDict(extra="forbid")

    phrase: str
    description: str


class PhraseRelationshipResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    relationships: list[PhraseRelationshipEntry]


# --- Stage 2, v2: one RECORD per phrase -------------------------------------
#
# The relationship stage is an evidence deposition: per phrase, every distinct
# mention of it in the text, then a synthesis that may rest on nothing outside
# those mentions. Wire and stored are separate types at the parse boundary,
# as everywhere else: the wire types carry strict-mode guarantees
# (extra="forbid", all fields required) that a stored record has no need to
# express, and the stored map is keyed differently (record_id, not phrase).


class PhraseMentionEntry(BaseModel):
    """Wire shape of one mention of the phrase in the text."""

    model_config = ConfigDict(extra="forbid")

    # The occurrence's wording exactly as the text has it — the occurrence
    # matcher is variant-tolerant (casing/spacing/punctuation/inflection), so
    # the form is what actually appeared, not the canonical phrase.
    form: str
    # Page path after the domain; '/' is the homepage, 'unknown' when the given
    # text began mid-page and no URL line was visible for the occurrence.
    page: str
    # What this mention says, quoting the text verbatim, subject-anonymized.
    account: str


class PhraseRecordEntry(BaseModel):
    """Wire shape of one phrase's record. Field order is generation order for
    the model: the phrase is restated first, mentions are collected next, and
    the synthesis comes last so it can only summarize what is already on the
    page above it."""

    model_config = ConfigDict(extra="forbid")

    phrase: str
    # Empty exactly when the phrase was not found in the text — the honest
    # not-found branch. Those records skip grounding and screening downstream.
    mentions: list[PhraseMentionEntry]
    synthesis: str


class PhraseRecordsResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    records: list[PhraseRecordEntry]


class PhraseMention(BaseModel):
    """Stored form of one mention."""

    form: str
    page: str
    account: str


class PhraseRelationshipRecord(BaseModel):
    """Stored form of one phrase's record: the deposition without the phrase
    key, which lives on whichever map carries the record."""

    mentions: list[PhraseMention]
    synthesis: str


# phrase → record: the parse-time result, before masking.
LLMPhraseRelationshipRecords = dict[str, PhraseRelationshipRecord]


class MaskedPhraseRelationshipRecord(BaseModel):
    """One entry of the stored (masked) relationship result.

    The map key is the record_id — a content-derived hash of the phrase, the
    only key downstream stages show the LLM — and the phrase rides INSIDE the
    entry, so the id→phrase join is persisted with the evidence it masks.
    Masking is key-masking only: the mention forms necessarily carry the
    phrase's wording."""

    phrase: str
    record: PhraseRelationshipRecord


# record_id → {phrase, record}: the stored shape of the v2 relationship stage.
MaskedLLMPhraseRelationshipResults = dict[str, MaskedPhraseRelationshipRecord]
