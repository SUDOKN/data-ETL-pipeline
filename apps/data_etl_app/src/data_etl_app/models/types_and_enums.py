from enum import Enum

from data_etl_app.utils.ontology_uri_util import (
    conformity_attestation_base_uri,
    industry_base_uri,
    material_cap_base_uri,
    process_cap_base_uri,
)


class BasicFieldTypeEnum(str, Enum):
    addresses = "addresses"
    business_desc = "business_desc"


class KeywordTypeEnum(str, Enum):
    products = "products"
    contract_products = "contract_products"
    equipments = "equipments"


class ConceptTypeEnum(str, Enum):
    """Satisfies `core.models.field_types.ConceptFieldType`."""

    industries = "industries"
    conformity_attestations = "conformity_attestations"
    material_caps = "material_caps"
    process_caps = "process_caps"

    @property
    def base_uri(self) -> str:
        return _CONCEPT_BASE_URI_RESOLVERS[self]()

    @property
    def recursive_grounding_placeholders(self) -> tuple[str, str]:
        return _RECURSIVE_GROUNDING_PLACEHOLDERS[self]


# Resolved lazily so importing this module never reads the environment.
_CONCEPT_BASE_URI_RESOLVERS = {
    ConceptTypeEnum.industries: industry_base_uri,
    ConceptTypeEnum.conformity_attestations: conformity_attestation_base_uri,
    ConceptTypeEnum.material_caps: material_cap_base_uri,
    ConceptTypeEnum.process_caps: process_cap_base_uri,
}

_RECURSIVE_GROUNDING_PLACEHOLDERS = {
    ConceptTypeEnum.industries: ("{{parent_industry}}", "{{types_of_parent_industry}}"),
    ConceptTypeEnum.conformity_attestations: (
        "{{parent_conformity_attestation}}",
        "{{types_of_parent_conformity_attestation}}",
    ),
    ConceptTypeEnum.material_caps: (
        "{{parent_material}}",
        "{{types_of_parent_material}}",
    ),
    ConceptTypeEnum.process_caps: (
        "{{parent_process_cap}}",
        "{{types_of_process_cap}}",
    ),
}


class BinaryClassificationTypeEnum(str, Enum):
    is_manufacturer = "is_manufacturer"
    is_product_manufacturer = "is_product_manufacturer"
    is_contract_manufacturer = "is_contract_manufacturer"


class GroundTruthSource(Enum):
    USER_FORM = "user_form"
    API_SURVEY = "api_survey"
