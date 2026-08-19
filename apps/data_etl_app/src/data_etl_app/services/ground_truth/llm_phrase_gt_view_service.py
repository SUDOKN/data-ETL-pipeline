"""Browse-plane views over a saved ``LLMPhraseGroundTruth`` document.

The read plane is split (P3-4): the annotator browses ALL phrases with only
the relationship stage piggybacked — its plain-text description is what lets
a human recognize a phrase and decide whether to open it — then fetches one
phrase's full trail for annotation (P3.4). Views are compute, never storage
(settled semantics #5): built fresh from the document on every read, so they
always reflect the latest audits.
"""

from __future__ import annotations

import hashlib
from datetime import datetime
from typing import Iterator, Optional

from pydantic import BaseModel

from core.models.ground_truth.stage_blocks import ExtractedPhraseGT
from core.services.ground_truth.gt_fold import (
    ChunkTruth,
    PhraseTruth,
    RuleAgreement,
    compute_document_truth,
    compute_phrase_truth,
    effective_text,
    rule_agreement_rollup,
)
from data_etl_app.db_models.llm_phrase_ground_truth import LLMPhraseGroundTruth


def phrase_has_any_audits(phrase: ExtractedPhraseGT) -> bool:
    """Whether any author has touched any surface of this phrase's slice.

    The browse ``reviewed`` flag, and the exact test the P3.4
    ``next_unreviewed`` cursor applies.
    """
    if phrase.llm_relationship.audits:
        return True
    if phrase.llm_screening is not None and phrase.llm_screening.audits:
        return True
    if phrase.oov_grounding is not None and any(
        tag_gt.audits for tag_gt in phrase.oov_grounding.tags.values()
    ):
        return True
    if phrase.in_vocab_grounding is not None and any(
        node.audits
        for nodes in phrase.in_vocab_grounding.levels.values()
        for node in nodes
    ):
        return True
    return False


class PhraseBrowseRow(BaseModel):
    phrase: str
    search_round: int
    llm_relationship_text: str
    effective_relationship_text: str
    relationship_addendum: Optional[str]
    relationship_reviewed: bool
    # Any audit anywhere in this phrase's slice — relationship, screening,
    # or either grounding. What next_unreviewed skips past.
    reviewed: bool


class MissedPhraseBrowseRow(BaseModel):
    phrase: str
    author_email: str
    identified_entity: str
    at: datetime


class ChunkBrowseView(BaseModel):
    extracted_phrases: list[PhraseBrowseRow]
    missed_phrases: list[MissedPhraseBrowseRow]


class TemplateBrowseView(BaseModel):
    document_id: Optional[str]
    created: bool
    mfg_etld1: str
    field_type: str
    scraped_text_file_version_id: str
    scraped_text_sha256: str
    scraped_text_char_len: int
    identity_digest: str
    created_at: datetime
    updated_at: datetime
    chunks: dict[str, ChunkBrowseView]


def _browse_row(phrase: str, phrase_gt: ExtractedPhraseGT) -> PhraseBrowseRow:
    relationship = effective_text(
        phrase_gt.llm_relationship.llm_result, phrase_gt.llm_relationship.audits
    )
    return PhraseBrowseRow(
        phrase=phrase,
        search_round=phrase_gt.search_round,
        llm_relationship_text=phrase_gt.llm_relationship.llm_result,
        effective_relationship_text=relationship.text,
        relationship_addendum=relationship.addendum,
        relationship_reviewed=relationship.reviewed,
        reviewed=phrase_has_any_audits(phrase_gt),
    )


def build_template_browse_view(
    doc: LLMPhraseGroundTruth, created: bool
) -> TemplateBrowseView:
    chunks: dict[str, ChunkBrowseView] = {}
    for chunk_key in sorted(doc.chunks, key=lambda key: int(key.split(":")[0])):
        chunk = doc.chunks[chunk_key]
        missed_rows: list[MissedPhraseBrowseRow] = []
        for entry in chunk.missed_phrases:
            entity = entry.screening.identified_entity
            assert entity is not None  # PH-10: a missed phrase names its entity
            missed_rows.append(
                MissedPhraseBrowseRow(
                    phrase=entry.phrase,
                    author_email=entry.author_email,
                    identified_entity=entity,
                    at=entry.at,
                )
            )
        chunks[chunk_key] = ChunkBrowseView(
            extracted_phrases=[
                _browse_row(phrase, phrase_gt)
                for phrase, phrase_gt in chunk.extracted_phrases.items()
            ],
            missed_phrases=missed_rows,
        )
    return TemplateBrowseView(
        document_id=str(doc.id) if doc.id is not None else None,
        created=created,
        mfg_etld1=doc.mfg_etld1,
        field_type=doc.field_type.value,
        scraped_text_file_version_id=doc.scraped_text_file_version_id,
        scraped_text_sha256=doc.scraped_text_sha256,
        scraped_text_char_len=doc.scraped_text_char_len,
        identity_digest=doc.identity_digest,
        created_at=doc.created_at,
        updated_at=doc.updated_at,
        chunks=chunks,
    )


# --- detail plane (P3-4b) ----------------------------------------------------


class TextWitnessError(ValueError):
    """The supplied text is not the bytes this document witnessed."""


def iter_phrases_in_order(
    doc: LLMPhraseGroundTruth,
) -> Iterator[tuple[str, str, ExtractedPhraseGT]]:
    """(chunk_key, phrase, slice) in chunk-bounds order, then stored order —
    the one ordering the browse view, the cursor, and the counts all share."""
    for chunk_key in sorted(doc.chunks, key=lambda key: int(key.split(":")[0])):
        for phrase, phrase_gt in doc.chunks[chunk_key].extracted_phrases.items():
            yield chunk_key, phrase, phrase_gt


def find_next_unreviewed(doc: LLMPhraseGroundTruth) -> Optional[tuple[str, str]]:
    """The next_unreviewed cursor: the first phrase no author has touched."""
    for chunk_key, phrase, phrase_gt in iter_phrases_in_order(doc):
        if not phrase_has_any_audits(phrase_gt):
            return chunk_key, phrase
    return None


def count_unreviewed(doc: LLMPhraseGroundTruth) -> int:
    return sum(
        1
        for _, _, phrase_gt in iter_phrases_in_order(doc)
        if not phrase_has_any_audits(phrase_gt)
    )


def witnessed_chunk_text(
    doc: LLMPhraseGroundTruth, chunk_key: str, full_text: str
) -> str:
    """The chunk's text, sliced only after the witness proves the bytes.

    Serving a slice of the wrong text would put a plausible-looking excerpt
    in front of the annotator — the witness exists precisely to make that
    impossible to do silently.
    """
    digest = hashlib.sha256(full_text.encode("utf-8")).hexdigest()
    if (
        digest != doc.scraped_text_sha256
        or len(full_text) != doc.scraped_text_char_len
    ):
        raise TextWitnessError(
            f"the supplied text (sha256 {digest[:12]}…, {len(full_text)} "
            f"chars) is not the text this document witnessed (sha256 "
            f"{doc.scraped_text_sha256[:12]}…, {doc.scraped_text_char_len} "
            f"chars) — fetch S3 version "
            f"{doc.scraped_text_file_version_id!r} of the subject's text"
        )
    start, end = (int(part) for part in chunk_key.split(":"))
    if end > len(full_text):
        raise TextWitnessError(
            f"chunk bounds {chunk_key!r} exceed the witnessed text "
            f"({len(full_text)} chars) — the document is corrupt"
        )
    return full_text[start:end]


class PhraseDetailView(BaseModel):
    """One phrase's full trail plus everything needed to judge it: the chunk
    text it came from, the folded effective view, and where the sitting
    stands (``unreviewed_remaining``)."""

    document_id: Optional[str]
    mfg_etld1: str
    field_type: str
    identity_digest: str
    chunk_key: str
    chunk_text: str
    phrase: str
    search_round: int
    reviewed: bool
    unreviewed_remaining: int
    trail: ExtractedPhraseGT
    effective: PhraseTruth


def build_phrase_detail_view(
    doc: LLMPhraseGroundTruth, chunk_key: str, phrase: str, chunk_text: str
) -> PhraseDetailView:
    phrase_gt = doc.chunks[chunk_key].extracted_phrases[phrase]
    return PhraseDetailView(
        document_id=str(doc.id) if doc.id is not None else None,
        mfg_etld1=doc.mfg_etld1,
        field_type=doc.field_type.value,
        identity_digest=doc.identity_digest,
        chunk_key=chunk_key,
        chunk_text=chunk_text,
        phrase=phrase,
        search_round=phrase_gt.search_round,
        reviewed=phrase_has_any_audits(phrase_gt),
        unreviewed_remaining=count_unreviewed(doc),
        trail=phrase_gt,
        effective=compute_phrase_truth(phrase_gt),
    )


# --- truth + work-list plane (P3.7) ------------------------------------------


def _total_phrases(doc: LLMPhraseGroundTruth) -> int:
    return sum(len(chunk.extracted_phrases) for chunk in doc.chunks.values())


class DocumentListRow(BaseModel):
    """One work-list row: which documents exist, and how much is unreviewed."""

    document_id: Optional[str]
    mfg_etld1: str
    field_type: str
    identity_digest: str
    scraped_text_file_version_id: str
    created_at: datetime
    updated_at: datetime
    total_phrases: int
    unreviewed_phrases: int
    missed_phrases: int


def build_document_list_row(doc: LLMPhraseGroundTruth) -> DocumentListRow:
    return DocumentListRow(
        document_id=str(doc.id) if doc.id is not None else None,
        mfg_etld1=doc.mfg_etld1,
        field_type=doc.field_type.value,
        identity_digest=doc.identity_digest,
        scraped_text_file_version_id=doc.scraped_text_file_version_id,
        created_at=doc.created_at,
        updated_at=doc.updated_at,
        total_phrases=_total_phrases(doc),
        unreviewed_phrases=count_unreviewed(doc),
        missed_phrases=sum(
            len(chunk.missed_phrases) for chunk in doc.chunks.values()
        ),
    )


class DocumentTruthView(BaseModel):
    """The per-document computed truth (P3-3): identity summary, the fold,
    and the per-rule agreement rollup — compute, never storage."""

    document_id: Optional[str]
    mfg_etld1: str
    field_type: str
    identity_digest: str
    scraped_text_file_version_id: str
    scraped_text_sha256: str
    scraped_text_char_len: int
    created_at: datetime
    updated_at: datetime
    total_phrases: int
    unreviewed_phrases: int
    truth: dict[str, ChunkTruth]
    rule_rollup: dict[str, RuleAgreement]


def build_document_truth_view(doc: LLMPhraseGroundTruth) -> DocumentTruthView:
    return DocumentTruthView(
        document_id=str(doc.id) if doc.id is not None else None,
        mfg_etld1=doc.mfg_etld1,
        field_type=doc.field_type.value,
        identity_digest=doc.identity_digest,
        scraped_text_file_version_id=doc.scraped_text_file_version_id,
        scraped_text_sha256=doc.scraped_text_sha256,
        scraped_text_char_len=doc.scraped_text_char_len,
        created_at=doc.created_at,
        updated_at=doc.updated_at,
        total_phrases=_total_phrases(doc),
        unreviewed_phrases=count_unreviewed(doc),
        truth=compute_document_truth(doc.chunks),
        rule_rollup=rule_agreement_rollup(doc.chunks),
    )
