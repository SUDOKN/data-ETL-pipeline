from enum import Enum
from typing import TypeVar

from core.models.db.gpt_batch_request import GPTBatchRequest
from data_etl_app.models.pipeline_nodes.llm_extraction_node import (
    LLMExtractionNode,
)
from open_ai_key_app.models.field_types import GPTBatchRequestCustomID

PipelineContext = dict[
    type[LLMExtractionNode], dict[GPTBatchRequestCustomID, GPTBatchRequest]
]


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
