"""The (stage, field_type) → catalog resolution the parse sites depend on.

The v1 parse-path tests this file used to carry died with the v1 wire shapes at
the pipeline-v2 flip; the v2 parse contracts are pinned in
test_grounding_v2_service / test_screening_v2_service (synthetic catalogs) and
test_v2_wire_schema (the deployed catalogs).
"""

import pytest

from core.models.rule_catalog import (
    STAGE_FREEHAND_GROUNDING,
    STAGE_INITIAL_GROUNDING,
    STAGE_OOV_GROUNDING,
    STAGE_RECURSIVE_GROUNDING,
    STAGE_RELATIONSHIP_SCREENING,
)
from core.services.rule_catalog_registry import (
    get_rule_catalog,
    set_rule_catalog_lookup,
)

from data_etl_app.models.types_and_enums import (
    BinaryClassificationTypeEnum,
    ConceptTypeEnum,
    KeywordTypeEnum,
)
from data_etl_app.services.prompt_assembly_service import build_rule_catalog_lookup


@pytest.fixture(autouse=True)
def _registry():
    set_rule_catalog_lookup(build_rule_catalog_lookup())
    yield
    set_rule_catalog_lookup(None)


def test_every_stage_and_field_type_resolves_to_a_catalog():
    """Every (stage, field) pair a v2 pipeline can ask for has exactly one
    deployed catalog: concepts run four catalog stages, keywords two."""
    for concept_type in ConceptTypeEnum:
        for stage in (
            STAGE_RELATIONSHIP_SCREENING,
            STAGE_INITIAL_GROUNDING,
            STAGE_OOV_GROUNDING,
            STAGE_RECURSIVE_GROUNDING,
        ):
            assert get_rule_catalog(stage, concept_type.value)

    for keyword_type in KeywordTypeEnum:
        assert get_rule_catalog(STAGE_RELATIONSHIP_SCREENING, keyword_type.value)
        assert get_rule_catalog(STAGE_FREEHAND_GROUNDING, keyword_type.value)

    for binary_type in BinaryClassificationTypeEnum:
        assert get_rule_catalog("binary_classification", binary_type.value)


def test_the_merged_product_grounding_catalog_serves_both_product_fields():
    """Attribution left grounding at the flip, so the contract/pure split
    collapsed back into one shared catalog (the pre-split arrangement catalog
    decision #8 was designed around). The screening catalogs stay split — the
    arrangement IS the relationship question."""
    products = get_rule_catalog(STAGE_FREEHAND_GROUNDING, "products")
    contract = get_rule_catalog(STAGE_FREEHAND_GROUNDING, "contract_products")
    assert products is contract
    assert get_rule_catalog(
        STAGE_RELATIONSHIP_SCREENING, "products"
    ) is not get_rule_catalog(STAGE_RELATIONSHIP_SCREENING, "contract_products")


def test_unregistered_catalog_fails_loudly_rather_than_skipping_validation():
    set_rule_catalog_lookup(None)
    with pytest.raises(RuntimeError, match="rule catalog lookup"):
        get_rule_catalog(STAGE_RELATIONSHIP_SCREENING, "industries")
