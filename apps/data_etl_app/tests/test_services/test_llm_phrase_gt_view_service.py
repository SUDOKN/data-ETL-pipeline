"""The browse view (P3-4a): relationship piggyback, reviewed flags, ordering.

Views are compute over the document — these tests build a real template from
the shared fixtures, decorate it with audits, and pin what the browse rows
say.
"""

from datetime import datetime

import pytest

from core.models.ground_truth.audits import AuditVerdict, TextFieldAudit
from core.models.ground_truth.stage_blocks import (
    ChunkGT,
    ExtractedPhraseGT,
    GroundingDerivation,
    HumanScreeningDerivation,
    RelationshipGT,
)
from data_etl_app.models.types_and_enums import KeywordTypeEnum
from data_etl_app.services.ground_truth.llm_phrase_gt_template_service import (
    build_llm_phrase_gt_template,
)
from data_etl_app.services.ground_truth.llm_phrase_gt_view_service import (
    TextWitnessError,
    build_document_list_row,
    build_document_truth_view,
    build_phrase_detail_view,
    build_template_browse_view,
    count_unreviewed,
    find_next_unreviewed,
    phrase_has_any_audits,
    witnessed_chunk_text,
)


def _audit(
    verdict: AuditVerdict,
    corrected: str | None = None,
    note: str | None = None,
) -> TextFieldAudit:
    return TextFieldAudit(
        type=verdict,
        corrected_text=corrected,
        note=note,
        author_email="ada@example.com",
        at=datetime(2026, 8, 16),
        source="api_survey",
    )


def _keyword_doc(make_gt_manufacturer, make_keyword_gt_results, lookup, text):
    mfg = make_gt_manufacturer(equipments=make_keyword_gt_results())
    return build_llm_phrase_gt_template(
        mfg, KeywordTypeEnum.equipments, text, catalog_lookup=lookup
    )


def test_browse_view_piggybacks_the_relationship_only(
    make_gt_manufacturer, make_keyword_gt_results, gt_catalog_lookup, gt_scraped_text
):
    doc = _keyword_doc(
        make_gt_manufacturer, make_keyword_gt_results, gt_catalog_lookup, gt_scraped_text
    )

    view = build_template_browse_view(doc, created=True)

    assert view.created is True
    assert view.document_id is None  # unsaved fixture doc has no id yet
    assert view.field_type == "equipments"
    assert view.identity_digest == doc.identity_digest
    (row,) = view.chunks["0:1000"].extracted_phrases
    assert row.phrase == "cnc mill"
    assert row.search_round == 1
    assert row.llm_relationship_text == "a machine they run"
    assert row.effective_relationship_text == "a machine they run"
    assert row.relationship_addendum is None
    assert row.relationship_note is None
    assert row.relationship_reviewed is False
    assert row.reviewed is False
    assert view.chunks["0:1000"].missed_phrases == []
    # The browse view carries no rule trees and no metadata.
    dumped = view.model_dump()
    assert "metadata" not in dumped
    assert "sections" not in str(dumped)


def test_relationship_audit_shows_through_and_flips_the_flags(
    make_gt_manufacturer, make_keyword_gt_results, gt_catalog_lookup, gt_scraped_text
):
    doc = _keyword_doc(
        make_gt_manufacturer, make_keyword_gt_results, gt_catalog_lookup, gt_scraped_text
    )
    phrase_gt = doc.chunks["0:1000"].extracted_phrases["cnc mill"]
    phrase_gt.llm_relationship.audits.append(
        _audit(
            AuditVerdict.AGREE_BUT,
            "and they mention five-axis work",
            note="the spec sheet on the page is explicit about it",
        )
    )

    (row,) = build_template_browse_view(doc, created=False).chunks[
        "0:1000"
    ].extracted_phrases
    assert row.effective_relationship_text == "a machine they run"
    assert row.relationship_addendum == "and they mention five-axis work"
    assert row.relationship_note == "the spec sheet on the page is explicit about it"
    assert row.relationship_reviewed is True
    assert row.reviewed is True


def test_disagree_replaces_the_effective_text(
    make_gt_manufacturer, make_keyword_gt_results, gt_catalog_lookup, gt_scraped_text
):
    doc = _keyword_doc(
        make_gt_manufacturer, make_keyword_gt_results, gt_catalog_lookup, gt_scraped_text
    )
    phrase_gt = doc.chunks["0:1000"].extracted_phrases["cnc mill"]
    phrase_gt.llm_relationship.audits.append(
        _audit(AuditVerdict.DISAGREE, "a machine they operate in-house")
    )

    (row,) = build_template_browse_view(doc, created=False).chunks[
        "0:1000"
    ].extracted_phrases
    assert row.effective_relationship_text == "a machine they operate in-house"
    assert row.llm_relationship_text == "a machine they run"


def test_any_surface_audit_marks_the_phrase_reviewed(
    make_gt_manufacturer, make_keyword_gt_results, gt_catalog_lookup, gt_scraped_text
):
    doc = _keyword_doc(
        make_gt_manufacturer, make_keyword_gt_results, gt_catalog_lookup, gt_scraped_text
    )
    phrase_gt = doc.chunks["0:1000"].extracted_phrases["cnc mill"]
    assert phrase_has_any_audits(phrase_gt) is False

    assert phrase_gt.llm_screening is not None
    phrase_gt.llm_screening.audits.append(
        HumanScreeningDerivation(
            identified_entity="cnc mill", audits=[], sections=[]
        )
    )
    assert phrase_has_any_audits(phrase_gt) is True

    phrase_gt.llm_screening.audits.clear()
    assert phrase_gt.oov_grounding is not None
    phrase_gt.oov_grounding.tags["CNC Milling Machine"].audits.append(
        GroundingDerivation(tag="CNC Milling Machine", sections=[])
    )
    assert phrase_has_any_audits(phrase_gt) is True


def test_chunks_are_served_in_bounds_order(
    make_gt_manufacturer, make_keyword_gt_results, gt_catalog_lookup, gt_scraped_text
):
    doc = _keyword_doc(
        make_gt_manufacturer, make_keyword_gt_results, gt_catalog_lookup, gt_scraped_text
    )
    first_chunk = doc.chunks.pop("0:1000")
    doc.chunks["1000:2000"] = ChunkGT(extracted_phrases={}, missed_phrases=[])
    doc.chunks["0:1000"] = first_chunk  # inserted after — dict order is wrong

    view = build_template_browse_view(doc, created=False)

    assert list(view.chunks) == ["0:1000", "1000:2000"]


# --- detail plane ------------------------------------------------------------


def _second_chunk_phrase() -> ChunkGT:
    return ChunkGT(
        extracted_phrases={
            "laser cutter": ExtractedPhraseGT(
                search_round=1,
                llm_relationship=RelationshipGT(llm_result="cuts sheet stock"),
            )
        },
        missed_phrases=[],
    )


def test_cursor_walks_chunk_order_and_skips_reviewed(
    make_gt_manufacturer, make_keyword_gt_results, gt_catalog_lookup, gt_scraped_text
):
    doc = _keyword_doc(
        make_gt_manufacturer, make_keyword_gt_results, gt_catalog_lookup, gt_scraped_text
    )
    doc.chunks["1000:2000"] = _second_chunk_phrase()
    assert count_unreviewed(doc) == 2
    assert find_next_unreviewed(doc) == ("0:1000", "cnc mill")

    doc.chunks["0:1000"].extracted_phrases["cnc mill"].llm_relationship.audits.append(
        _audit(AuditVerdict.AGREE)
    )
    assert count_unreviewed(doc) == 1
    assert find_next_unreviewed(doc) == ("1000:2000", "laser cutter")

    doc.chunks["1000:2000"].extracted_phrases[
        "laser cutter"
    ].llm_relationship.audits.append(_audit(AuditVerdict.AGREE))
    assert count_unreviewed(doc) == 0
    assert find_next_unreviewed(doc) is None


def test_witnessed_chunk_text_slices_only_the_proven_bytes(
    make_gt_manufacturer, make_keyword_gt_results, gt_catalog_lookup, gt_scraped_text
):
    doc = _keyword_doc(
        make_gt_manufacturer, make_keyword_gt_results, gt_catalog_lookup, gt_scraped_text
    )
    key = f"0:{len(gt_scraped_text)}"
    doc.chunks[key] = doc.chunks.pop("0:1000")

    assert witnessed_chunk_text(doc, key, gt_scraped_text) == gt_scraped_text

    with pytest.raises(TextWitnessError, match="text-v3"):
        witnessed_chunk_text(doc, key, gt_scraped_text + " tampered")


def test_bounds_beyond_the_witnessed_text_are_corrupt(
    make_gt_manufacturer, make_keyword_gt_results, gt_catalog_lookup, gt_scraped_text
):
    doc = _keyword_doc(
        make_gt_manufacturer, make_keyword_gt_results, gt_catalog_lookup, gt_scraped_text
    )
    # The fixture key "0:1000" reaches past the 60-char witnessed text.
    with pytest.raises(TextWitnessError, match="exceed the witnessed text"):
        witnessed_chunk_text(doc, "0:1000", gt_scraped_text)


def test_list_row_counts_track_review_progress(
    make_gt_manufacturer, make_keyword_gt_results, gt_catalog_lookup, gt_scraped_text
):
    doc = _keyword_doc(
        make_gt_manufacturer, make_keyword_gt_results, gt_catalog_lookup, gt_scraped_text
    )

    row = build_document_list_row(doc)
    assert row.total_phrases == 1
    assert row.unreviewed_phrases == 1
    assert row.missed_phrases == 0
    assert row.field_type == "equipments"
    assert row.identity_digest == doc.identity_digest

    doc.chunks["0:1000"].extracted_phrases["cnc mill"].llm_relationship.audits.append(
        _audit(AuditVerdict.AGREE)
    )
    assert build_document_list_row(doc).unreviewed_phrases == 0


def test_truth_view_composes_fold_rollup_and_identity(
    make_gt_manufacturer, make_keyword_gt_results, gt_catalog_lookup, gt_scraped_text
):
    doc = _keyword_doc(
        make_gt_manufacturer, make_keyword_gt_results, gt_catalog_lookup, gt_scraped_text
    )

    view = build_document_truth_view(doc)

    assert view.identity_digest == doc.identity_digest
    assert view.scraped_text_sha256 == doc.scraped_text_sha256
    assert view.total_phrases == 1 and view.unreviewed_phrases == 1
    chunk_truth = view.truth["0:1000"]
    assert chunk_truth.phrases["cnc mill"].relationship.reviewed is False
    assert view.rule_rollup == {}  # nothing reviewed yet — no agreement rows


def test_phrase_detail_view_carries_trail_effective_and_position(
    make_gt_manufacturer, make_keyword_gt_results, gt_catalog_lookup, gt_scraped_text
):
    doc = _keyword_doc(
        make_gt_manufacturer, make_keyword_gt_results, gt_catalog_lookup, gt_scraped_text
    )
    key = f"0:{len(gt_scraped_text)}"
    doc.chunks[key] = doc.chunks.pop("0:1000")

    view = build_phrase_detail_view(doc, key, "cnc mill", gt_scraped_text)

    assert view.phrase == "cnc mill"
    assert view.chunk_key == key
    assert view.chunk_text == gt_scraped_text
    assert view.search_round == 1
    assert view.reviewed is False
    assert view.unreviewed_remaining == 1
    assert view.trail.llm_screening is not None
    assert view.trail.llm_screening.llm_result.passed is True
    assert view.effective.relationship.text == "a machine they run"
    assert view.effective.screening is not None
    assert view.effective.screening.passed is True
    assert "CNC Milling Machine" in view.effective.tags
