from datetime import datetime
from typing import Any, Callable

import pytest
from beanie.odm.settings.document import DocumentSettings
from pure_utils.env_util import load_env

from data_etl_app.dependencies.env import ONTOLOGY_SCRIPT_ENV

# Entrypoint for the app test session: loads the root .env once for all tests.
load_env(ONTOLOGY_SCRIPT_ENV)

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
from core.services.ground_truth.catalog_template_inflation import (
    inflate_applied_rules,
)
from data_etl_app.db_models.manufacturer import Manufacturer
from data_etl_app.services.prompt_assembly_service import build_rule_catalog_lookup


@pytest.fixture(autouse=True, scope="session")
def offline_document_settings():
    """Offline Beanie settings for any test constructing a Document.

    Beanie 2.0's ``init_beanie`` unconditionally runs ``buildInfo`` against a
    live server, but constructing and validating a Document only needs
    ``cls._document_settings`` populated — exactly what the synchronous
    ``Initializer.init_settings`` step does. This replicates that one step so
    document contracts (validators, round-trips) are testable without MongoDB;
    anything needing real collection I/O belongs in an ``integration`` test.
    """
    from data_etl_app.db_models import APP_DOCUMENT_MODELS

    for model in APP_DOCUMENT_MODELS:
        settings_class = getattr(model, "Settings")
        settings_vars = {
            attr: getattr(settings_class, attr)
            for attr in dir(settings_class)
            if not attr.startswith("__")
        }
        model._document_settings = DocumentSettings(**settings_vars)


@pytest.fixture
def sample_fixture():
    # This is a sample fixture that can be used in tests
    return {"key": "value"}


# --- shared phrase-GT fixtures ----------------------------------------------
# Live-shaped Manufacturer results against the REAL deployed catalogs. Promoted
# here from test_llm_phrase_gt_template_service.py at P3.2: importlib import
# mode blocks sibling test-module imports, and the P3 service/route tests need
# the same builders. Factories return fresh objects per call — tests mutate.

_GT_TEXT = "Steelcraft runs CNC mills and serves the aerospace industry."

_GT_COMMON: dict[str, Any] = dict(
    llm_model=GPT_4o_mini,
    model_params=GPTModelParams.with_defaults(),
    prompt_name="some_prompt",
    prompt_version_id="s3-version-1",
    created_at=datetime(2026, 8, 15),
)


def _gt_always_rows(catalog: RuleCatalog) -> list[AppliedRule]:
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


def _gt_verdict(catalog: RuleCatalog, entity: str) -> ScreeningVerdict:
    rows = _gt_always_rows(catalog)
    return ScreeningVerdict(
        passed=passed_implied_by(catalog, rows),
        identified_entity=entity,
        applied_rules=rows,
    )


def _gt_base_metadata_nodes(screening_catalog: RuleCatalog) -> dict[str, Any]:
    return dict(
        created_at=datetime(2026, 8, 15),
        chunk_strat=ChunkingStrategy(
            overlap=0.1, max_chunks=5, max_tokens_per_chunk=1000
        ),
        ontology_version_id="onto-v7",
        llm_phrase_search=ExtractionNodeMetadata(**_GT_COMMON),
        llm_phrase_recursive_search=RecursiveSearchNodeMetadata(
            **_GT_COMMON, max_rounds=3
        ),
        llm_phrase_relationship=BatchedRelationshipNodeMetadata(
            **_GT_COMMON, max_phrases_per_request=50
        ),
        llm_phrase_relationship_screening=BatchedScreeningNodeMetadata(
            **{**_GT_COMMON, "catalog_version": screening_catalog.catalog_version},
            max_pairs_per_request=15,
        ),
    )


@pytest.fixture(scope="session")
def gt_catalog_lookup():
    return build_rule_catalog_lookup()


def _gt_required_catalog(lookup, stage: str, field: str) -> RuleCatalog:
    catalog = lookup(stage, field)
    assert catalog is not None, f"no deployed catalog for ({stage}, {field})"
    return catalog


@pytest.fixture(scope="session")
def gt_scraped_text() -> str:
    return _GT_TEXT


@pytest.fixture
def gt_fresh_happy():
    """A complete, satisfied fresh derivation against a REAL catalog:
    conditions satisfied, one ordered branch chosen, quality filled with the
    vocab's first value (fresh completeness needs it; the happy path ignores
    quality), guards keeping their synthesized silence."""

    def _fill(catalog: RuleCatalog):
        sections = inflate_applied_rules(catalog, [], synthesize_all_on_empty=True)
        for section in sections:
            chosen_done = False
            for rule in section.applied_rules:
                if rule.kind == "condition":
                    rule.outcome = "satisfied"
                elif rule.kind == "preference":
                    if section.combinator == "ordered" and not chosen_done:
                        rule.outcome = "chosen"
                        chosen_done = True
                    else:
                        continue
                elif rule.kind == "quality":
                    rule.outcome = catalog.outcome_vocab["quality"][0]
                else:
                    continue
                rule.explanation = "human derivation"
        return sections

    return _fill


@pytest.fixture
def make_gt_manufacturer() -> Callable[..., Manufacturer]:
    def _make(**field_results) -> Manufacturer:
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

    return _make


@pytest.fixture
def make_keyword_gt_results(
    gt_catalog_lookup,
) -> Callable[[], KeywordExtractionResults]:
    def _make() -> KeywordExtractionResults:
        screening = _gt_required_catalog(
            gt_catalog_lookup, STAGE_RELATIONSHIP_SCREENING, "equipments"
        )
        freehand = _gt_required_catalog(
            gt_catalog_lookup, STAGE_FREEHAND_GROUNDING, "equipments"
        )
        return KeywordExtractionResults(
            metadata=KeywordExtractionMetadata(
                **_gt_base_metadata_nodes(screening),
                llm_phrase_freehand_grounding=BatchedFreehandGroundingNodeMetadata(
                    **{**_GT_COMMON, "catalog_version": freehand.catalog_version},
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
                        1: {"cnc mill": _gt_verdict(screening, "cnc mill")},
                    },
                    llm_phrase_freehand_grounding={
                        0: {},
                        1: {
                            "cnc mill": {
                                "CNC Milling Machine": _gt_always_rows(freehand)
                            }
                        },
                    },
                )
            },
        )

    return _make


@pytest.fixture
def make_concept_gt_results(
    gt_catalog_lookup,
) -> Callable[[], ConceptExtractionResults]:
    def _make() -> ConceptExtractionResults:
        screening = _gt_required_catalog(
            gt_catalog_lookup, STAGE_RELATIONSHIP_SCREENING, "industries"
        )
        initial = _gt_required_catalog(
            gt_catalog_lookup, STAGE_INITIAL_GROUNDING, "industries"
        )
        recursive = _gt_required_catalog(
            gt_catalog_lookup, STAGE_RECURSIVE_GROUNDING, "industries"
        )
        return ConceptExtractionResults(
            metadata=ConceptExtractionMetadata(
                **_gt_base_metadata_nodes(screening),
                llm_phrase_initial_grounding=BatchedInitialGroundingNodeMetadata(
                    **{**_GT_COMMON, "catalog_version": initial.catalog_version},
                    max_pairs_per_request=15,
                ),
                llm_phrase_recursive_grounding=ExtractionNodeMetadata(
                    **{**_GT_COMMON, "catalog_version": recursive.catalog_version},
                ),
            ),
            results=ConceptsFound(in_vocab={"Aerospace"}, out_of_vocab=set()),
            chunked_extraction_stats={
                "0:1000": ConceptExtractionStats(
                    results=ConceptsFound(
                        in_vocab={"Aerospace"}, out_of_vocab=set()
                    ),
                    brute_search=set(),
                    llm_phrase_search={0: set(), 1: {"aerospace parts"}},
                    llm_phrase_relationship={
                        0: {},
                        1: {"aerospace parts": "an industry they serve"},
                    },
                    llm_phrase_screening={
                        0: {},
                        1: {
                            "aerospace parts": _gt_verdict(
                                screening, "aerospace parts"
                            )
                        },
                    },
                    llm_phrase_initial_grounding={
                        0: {},
                        1: {
                            "aerospace parts": {
                                "Aerospace": _gt_always_rows(initial)
                            }
                        },
                    },
                    llm_phrase_recursive_grounding={
                        1: {
                            IterativelyTaggedPhraseGroup(
                                parent_group_id=None,
                                group_id="Aerospace",
                                direct_phrases_to_og_tag_w_rules={
                                    "aerospace parts": {
                                        "Aerospace": _gt_always_rows(recursive)
                                    }
                                },
                                iterative_phrases_to_og_tag_w_rules={},
                            )
                        }
                    },
                )
            },
        )

    return _make
