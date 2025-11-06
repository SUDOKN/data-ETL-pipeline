from enum import Enum


class KeywordTypeEnum(str, Enum):
    products = "products"


class ConceptTypeEnum(str, Enum):
    industries = "industries"
    certificates = "certificates"
    material_caps = "material_caps"
    process_caps = "process_caps"


class BasicFieldTypeEnum(str, Enum):
    addresses = "addresses"
    business_desc = "business_desc"


class BinaryClassificationTypeEnum(str, Enum):
    is_manufacturer = "is_manufacturer"
    is_product_manufacturer = "is_product_manufacturer"
    is_contract_manufacturer = "is_contract_manufacturer"


GenericFieldTypeEnum = (
    KeywordTypeEnum
    | ConceptTypeEnum
    | BasicFieldTypeEnum
    | BinaryClassificationTypeEnum
)


class GroundTruthSource(Enum):
    USER_FORM = "user_form"
    API_SURVEY = "api_survey"
