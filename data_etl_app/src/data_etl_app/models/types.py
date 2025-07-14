from enum import Enum


class ConceptTypeEnum(str, Enum):
    industries = "industries"
    certificates = "certificates"
    material_caps = "material_caps"
    process_caps = "process_caps"


class BinaryClassificationTypeEnum(str, Enum):
    is_manufacturer = "is_manufacturer"
    is_product_manufacturer = "is_product_manufacturer"
    is_contract_manufacturer = "is_contract_manufacturer"
