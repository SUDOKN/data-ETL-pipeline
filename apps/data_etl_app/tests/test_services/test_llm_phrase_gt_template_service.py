"""Template assembly from live-shaped Manufacturer fixtures, against the REAL
deployed catalogs. The document's own validators (digest cache, identity
projection, family match, chunk keys) run at construction, so a template that
builds at all is already self-proving; these tests pin what it contains.

The fixture builders live in the app tests root conftest (promoted at P3.2 —
the document-service and route tests share them)."""

import hashlib

import pytest

from data_etl_app.models.types_and_enums import ConceptTypeEnum, KeywordTypeEnum
from data_etl_app.services.ground_truth.llm_phrase_gt_template_service import (
    TemplateAssemblyError,
    build_llm_phrase_gt_template,
)


def test_keyword_template_builds_and_self_proves(
    make_gt_manufacturer, make_keyword_gt_results, gt_catalog_lookup, gt_scraped_text
):
    mfg = make_gt_manufacturer(equipments=make_keyword_gt_results())
    doc = build_llm_phrase_gt_template(
        mfg, KeywordTypeEnum.equipments, gt_scraped_text, catalog_lookup=gt_catalog_lookup
    )

    assert doc.mfg_etld1 == "steelcraft.com"
    assert doc.scraped_text_file_version_id == "text-v3"
    assert (
        doc.scraped_text_sha256
        == hashlib.sha256(gt_scraped_text.encode()).hexdigest()
    )
    assert doc.scraped_text_char_len == len(gt_scraped_text)

    phrase = doc.chunks["0:1000"].extracted_phrases["cnc mill"]
    assert phrase.llm_screening is not None and phrase.oov_grounding is not None
    assert phrase.search_round == 1
    assert phrase.llm_screening.llm_result.passed is True
    assert phrase.llm_screening.audits == []
    assert "CNC Milling Machine" in phrase.oov_grounding.tags
    assert phrase.in_vocab_grounding is None
    assert doc.chunks["0:1000"].missed_phrases == []


def test_concept_template_builds_with_the_descent(
    make_gt_manufacturer, make_concept_gt_results, gt_catalog_lookup, gt_scraped_text
):
    mfg = make_gt_manufacturer(industries=make_concept_gt_results())
    doc = build_llm_phrase_gt_template(
        mfg, ConceptTypeEnum.industries, gt_scraped_text, catalog_lookup=gt_catalog_lookup
    )

    phrase = doc.chunks["0:1000"].extracted_phrases["aerospace parts"]
    assert phrase.oov_grounding is not None and phrase.in_vocab_grounding is not None
    assert "Aerospace" in phrase.oov_grounding.tags
    node = phrase.in_vocab_grounding.levels[1][0]
    assert node.group_id == "Aerospace" and node.parent_group_id is None
    assert any(rule.reported for s in node.sections for rule in s.applied_rules)


def test_metadata_is_a_deep_copy_not_a_shared_reference(
    make_gt_manufacturer, make_keyword_gt_results, gt_catalog_lookup, gt_scraped_text
):
    results = make_keyword_gt_results()
    mfg = make_gt_manufacturer(equipments=results)
    doc = build_llm_phrase_gt_template(
        mfg, KeywordTypeEnum.equipments, gt_scraped_text, catalog_lookup=gt_catalog_lookup
    )
    assert doc.metadata == results.metadata
    assert doc.metadata is not results.metadata
    assert (
        doc.metadata.llm_phrase_search
        is not results.metadata.llm_phrase_search
    )


def test_field_without_stored_results_is_refused(
    make_gt_manufacturer, gt_catalog_lookup, gt_scraped_text
):
    with pytest.raises(TemplateAssemblyError, match="nothing to audit"):
        build_llm_phrase_gt_template(
            make_gt_manufacturer(),
            KeywordTypeEnum.equipments,
            gt_scraped_text,
            catalog_lookup=gt_catalog_lookup,
        )


def test_catalog_version_drift_hard_fails_with_both_versions_named(
    make_gt_manufacturer, make_keyword_gt_results, gt_catalog_lookup, gt_scraped_text
):
    results = make_keyword_gt_results()
    results.metadata.llm_phrase_relationship_screening.catalog_version = "stale.0"
    mfg = make_gt_manufacturer(equipments=results)
    with pytest.raises(TemplateAssemblyError, match="'stale.0'.*re-run extraction"):
        build_llm_phrase_gt_template(
            mfg, KeywordTypeEnum.equipments, gt_scraped_text, catalog_lookup=gt_catalog_lookup
        )
