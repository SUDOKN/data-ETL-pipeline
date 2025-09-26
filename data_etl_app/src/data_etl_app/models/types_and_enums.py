from enum import Enum


class KeywordTypeEnum(str, Enum):
    products = "products"


class ConceptTypeEnum(str, Enum):
    industries = "industries"
    certificates = "certificates"
    material_caps = "material_caps"
    process_caps = "process_caps"


class GenericFieldTypeEnum(str, Enum):
    # Union of both
    products = KeywordTypeEnum.products
    industries = ConceptTypeEnum.industries
    certificates = ConceptTypeEnum.certificates
    material_caps = ConceptTypeEnum.material_caps
    process_caps = ConceptTypeEnum.process_caps


class BinaryClassificationTypeEnum(str, Enum):
    is_manufacturer = "is_manufacturer"
    is_product_manufacturer = "is_product_manufacturer"
    is_contract_manufacturer = "is_contract_manufacturer"


class GroundTruthSource(Enum):
    USER_FORM = "user_form"
    API_SURVEY = "api_survey"
