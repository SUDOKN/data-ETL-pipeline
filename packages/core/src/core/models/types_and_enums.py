from enum import Enum
from typing import TypeVar


class KeywordTypeEnum(str, Enum):
    products = "products"
    contract_products = "contract_products"
    equipments = "equipments"


class ConceptTypeEnum(str, Enum):
    industries = "industries"
    certificates = "certificates"
    material_caps = "material_caps"
    process_caps = "process_caps"


class BinaryClassificationTypeEnum(str, Enum):
    is_manufacturer = "is_manufacturer"
    is_product_manufacturer = "is_product_manufacturer"
    is_contract_manufacturer = "is_contract_manufacturer"


# str stands in for app-declared basic field types (e.g. data_etl_app.BasicFieldTypeEnum)
LLMExtractedFieldTypeEnum = (
    KeywordTypeEnum | ConceptTypeEnum | str | BinaryClassificationTypeEnum
)

SingleStageFieldTypeEnum = str | BinaryClassificationTypeEnum

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
