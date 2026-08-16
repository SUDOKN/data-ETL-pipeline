"""Template assembly from live-shaped Manufacturer fixtures, against the REAL
deployed catalogs. The document's own validators (digest cache, identity
projection, family match, chunk keys) run at construction, so a template that
builds at all is already self-proving; these tests pin what it contains."""

import hashlib
from datetime import datetime
from typing import Any

import pytest

from llm_providers.models.llm_model import GPT_4o_mini
from llm_providers.models.open_ai.gpt_model_params import GPTModelParams

from core.models.chunking_strat import ChunkingStrategy
from core.models.extraction_results.concept_extraction_results import (
    BatchedInitialGroundingNodeMetadata,
    ConceptExtractionMetadata,
    ConceptExtractionResults,
    ConceptExtractionStats,
    ConceptsFound,
)
from core.models.extraction_results.keyword_extraction_results import (
    BatchedFreehandGroundingNodeMetadata,
    KeywordExtractionMetadata,
    KeywordExtractionResults,
    KeywordExtractionStats,
)
from core.models.extraction_results.llm_phrase_extraction_results import (
    BatchedRelationshipNodeMetadata,
    BatchedScreeningNodeMetadata,
    ExtractionNodeMetadata,
    RecursiveSearchNodeMetadata,
)
from core.models.extraction_schemas.applied_rule import AppliedRule
from core.models.extraction_schemas.iterative_tagging import (
    IterativelyTaggedPhraseGroup,
)
from core.models.extraction_schemas.screening import ScreeningVerdict
from core.models.rule_catalog import (
    STAGE_FREEHAND_GROUNDING,
    STAGE_INITIAL_GROUNDING,
    STAGE_RECURSIVE_GROUNDING,
    STAGE_RELATIONSHIP_SCREENING,
    RuleCatalog,
)
from core.services.applied_rule_validation import passed_implied_by
from data_etl_app.db_models.manufacturer import Manufacturer
from data_etl_app.models.types_and_enums import ConceptTypeEnum, KeywordTypeEnum
from data_etl_app.services.ground_truth.llm_phrase_gt_template_service import (
    TemplateAssemblyError,
    build_llm_phrase_gt_template,
)
from data_etl_app.services.prompt_assembly_service import build_rule_catalog_lookup

_LOOKUP = build_rule_catalog_lookup()


def _cat(stage: str, field: str) -> RuleCatalog:
    catalog = _LOOKUP(stage, field)
    assert catalog is not None, f"no deployed catalog for ({stage}, {field})"
    return catalog


_TEXT = "Steelcraft runs CNC mills and serves the aerospace industry."

_COMMON: dict[str, Any] = dict(
    llm_model=GPT_4o_mini,
    model_params=GPTModelParams.with_defaults(),
    prompt_name="some_prompt",
    prompt_version_id="s3-version-1",
    created_at=datetime(2026, 8, 15),
)


def _always_rows(catalog: RuleCatalog) -> list[AppliedRule]:
    return [
        AppliedRule(
            rule_id=rule.id,
            outcome=(
                "satisfied"
                if rule.kind == "condition"
                else catalog.outcome_vocab[rule.kind][0]
            ),
            explanation="cited from the relationship summary",
        )
        for rule in catalog.walk_rules()
        if rule.report_when == "always"
    ]


def _verdict(catalog: RuleCatalog, entity: str) -> ScreeningVerdict:
    rows = _always_rows(catalog)
    return ScreeningVerdict(
        passed=passed_implied_by(catalog, rows),
        identified_entity=entity,
        applied_rules=rows,
    )


def _base_metadata_nodes(screening_catalog: RuleCatalog) -> dict[str, Any]:
    return dict(
        created_at=datetime(2026, 8, 15),
        chunk_strat=ChunkingStrategy(
            overlap=0.1, max_chunks=5, max_tokens_per_chunk=1000
        ),
        ontology_version_id="onto-v7",
        llm_phrase_search=ExtractionNodeMetadata(**_COMMON),
        llm_phrase_recursive_search=RecursiveSearchNodeMetadata(
            **_COMMON, max_rounds=3
        ),
        llm_phrase_relationship=BatchedRelationshipNodeMetadata(
            **_COMMON, max_phrases_per_request=50
        ),
        llm_phrase_relationship_screening=BatchedScreeningNodeMetadata(
            **{**_COMMON, "catalog_version": screening_catalog.catalog_version},
            max_pairs_per_request=15,
        ),
    )


def _manufacturer(**field_results) -> Manufacturer:
    fields: dict = dict(
        etld1="steelcraft.com",
        etld1_accessible_at="steelcraft.com",
        scraped_text_file_num_tokens=42,
        scraped_text_file_version_id="text-v3",
        batches=[],
        name=None,
        founded_in=None,
        email_addresses=None,
        num_employees=None,
        business_statuses=None,
        primary_naics=None,
        secondary_naics=None,
        is_manufacturer=None,
        is_contract_manufacturer=None,
        is_product_manufacturer=None,
        addresses=None,
        business_desc=None,
        products=None,
        contract_products=None,
        equipments=None,
        conformity_attestations=None,
        industries=None,
        process_caps=None,
        material_caps=None,
    )
    fields.update(field_results)
    return Manufacturer(**fields)


def _keyword_results() -> KeywordExtractionResults:
    screening = _cat(STAGE_RELATIONSHIP_SCREENING, "equipments")
    freehand = _cat(STAGE_FREEHAND_GROUNDING, "equipments")
    return KeywordExtractionResults(
        metadata=KeywordExtractionMetadata(
            **_base_metadata_nodes(screening),
            llm_phrase_freehand_grounding=BatchedFreehandGroundingNodeMetadata(
                **{**_COMMON, "catalog_version": freehand.catalog_version},
                max_pairs_per_request=50,
            ),
        ),
        results={"cnc mill"},
        chunk_stats={
            "0:1000": KeywordExtractionStats(
                results={"cnc mill"},
                llm_phrase_search={0: set(), 1: {"cnc mill"}},
                llm_phrase_relationship={
                    0: {},
                    1: {"cnc mill": "a machine they run"},
                },
                llm_phrase_screening={
                    0: {},
                    1: {"cnc mill": _verdict(screening, "cnc mill")},
                },
                llm_phrase_freehand_grounding={
                    0: {},
                    1: {
                        "cnc mill": {
                            "CNC Milling Machine": _always_rows(freehand)
                        }
                    },
                },
            )
        },
    )


def _concept_results() -> ConceptExtractionResults:
    screening = _cat(STAGE_RELATIONSHIP_SCREENING, "industries")
    initial = _cat(STAGE_INITIAL_GROUNDING, "industries")
    recursive = _cat(STAGE_RECURSIVE_GROUNDING, "industries")
    return ConceptExtractionResults(
        metadata=ConceptExtractionMetadata(
            **_base_metadata_nodes(screening),
            llm_phrase_initial_grounding=BatchedInitialGroundingNodeMetadata(
                **{**_COMMON, "catalog_version": initial.catalog_version},
                max_pairs_per_request=15,
            ),
            llm_phrase_recursive_grounding=ExtractionNodeMetadata(
                **{**_COMMON, "catalog_version": recursive.catalog_version},
            ),
        ),
        results=ConceptsFound(in_vocab={"Aerospace"}, out_of_vocab=set()),
        chunked_extraction_stats={
            "0:1000": ConceptExtractionStats(
                results=ConceptsFound(in_vocab={"Aerospace"}, out_of_vocab=set()),
                brute_search=set(),
                llm_phrase_search={0: set(), 1: {"aerospace parts"}},
                llm_phrase_relationship={
                    0: {},
                    1: {"aerospace parts": "an industry they serve"},
                },
                llm_phrase_screening={
                    0: {},
                    1: {"aerospace parts": _verdict(screening, "aerospace parts")},
                },
                llm_phrase_initial_grounding={
                    0: {},
                    1: {"aerospace parts": {"Aerospace": _always_rows(initial)}},
                },
                llm_phrase_recursive_grounding={
                    1: {
                        IterativelyTaggedPhraseGroup(
                            parent_group_id=None,
                            group_id="Aerospace",
                            direct_phrases_to_og_tag_w_rules={
                                "aerospace parts": {
                                    "Aerospace": _always_rows(recursive)
                                }
                            },
                            iterative_phrases_to_og_tag_w_rules={},
                        )
                    }
                },
            )
        },
    )


def test_keyword_template_builds_and_self_proves():
    mfg = _manufacturer(equipments=_keyword_results())
    doc = build_llm_phrase_gt_template(
        mfg, KeywordTypeEnum.equipments, _TEXT, catalog_lookup=_LOOKUP
    )

    assert doc.mfg_etld1 == "steelcraft.com"
    assert doc.scraped_text_file_version_id == "text-v3"
    assert doc.scraped_text_sha256 == hashlib.sha256(_TEXT.encode()).hexdigest()
    assert doc.scraped_text_char_len == len(_TEXT)

    phrase = doc.chunks["0:1000"].extracted_phrases["cnc mill"]
    assert phrase.llm_screening is not None and phrase.oov_grounding is not None
    assert phrase.search_round == 1
    assert phrase.llm_screening.llm_result.passed is True
    assert phrase.llm_screening.audits == []
    assert "CNC Milling Machine" in phrase.oov_grounding.tags
    assert phrase.in_vocab_grounding is None
    assert doc.chunks["0:1000"].missed_phrases == []


def test_concept_template_builds_with_the_descent():
    mfg = _manufacturer(industries=_concept_results())
    doc = build_llm_phrase_gt_template(
        mfg, ConceptTypeEnum.industries, _TEXT, catalog_lookup=_LOOKUP
    )

    phrase = doc.chunks["0:1000"].extracted_phrases["aerospace parts"]
    assert phrase.oov_grounding is not None and phrase.in_vocab_grounding is not None
    assert "Aerospace" in phrase.oov_grounding.tags
    node = phrase.in_vocab_grounding.levels[1][0]
    assert node.group_id == "Aerospace" and node.parent_group_id is None
    assert any(rule.reported for s in node.sections for rule in s.applied_rules)


def test_metadata_is_a_deep_copy_not_a_shared_reference():
    results = _keyword_results()
    mfg = _manufacturer(equipments=results)
    doc = build_llm_phrase_gt_template(
        mfg, KeywordTypeEnum.equipments, _TEXT, catalog_lookup=_LOOKUP
    )
    assert doc.metadata == results.metadata
    assert doc.metadata is not results.metadata
    assert (
        doc.metadata.llm_phrase_search
        is not results.metadata.llm_phrase_search
    )


def test_field_without_stored_results_is_refused():
    with pytest.raises(TemplateAssemblyError, match="nothing to audit"):
        build_llm_phrase_gt_template(
            _manufacturer(), KeywordTypeEnum.equipments, _TEXT, catalog_lookup=_LOOKUP
        )


def test_catalog_version_drift_hard_fails_with_both_versions_named():
    results = _keyword_results()
    results.metadata.llm_phrase_relationship_screening.catalog_version = "stale.0"
    mfg = _manufacturer(equipments=results)
    with pytest.raises(TemplateAssemblyError, match="'stale.0'.*re-run extraction"):
        build_llm_phrase_gt_template(
            mfg, KeywordTypeEnum.equipments, _TEXT, catalog_lookup=_LOOKUP
        )
