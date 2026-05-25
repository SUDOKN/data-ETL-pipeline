from enum import Enum
from typing import TypeVar


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


LLMExtractedFieldTypeEnum = (
    KeywordTypeEnum
    | ConceptTypeEnum
    | BasicFieldTypeEnum
    | BinaryClassificationTypeEnum
)

SingleStageFieldTypeEnum = BasicFieldTypeEnum | BinaryClassificationTypeEnum

# Define a type variable that must be a LLMExtractedFieldTypeEnum
LLMExtractedFieldTypeVar = TypeVar(
    "LLMExtractedFieldTypeVar", bound=LLMExtractedFieldTypeEnum
)

SingleStageFieldTypeVar = TypeVar(
    "SingleStageFieldTypeVar", bound=SingleStageFieldTypeEnum
)


class GroundTruthSource(Enum):
    USER_FORM = "user_form"
    API_SURVEY = "api_survey"
