from typing import Optional

from data_etl_app.models.pipeline_nodes.extraction.business_desc_extraction_node import (
    BusinessDescExtractionNode,
)
from data_etl_app.models.pipeline_nodes.extraction.concept_mapping_node import (
    ConceptMappingNode,
)
from data_etl_app.models.pipeline_nodes.extraction.concept_search_node import (
    ConceptSearchNode,
)
from data_etl_app.models.pipeline_nodes.extraction.extraction_node import ExtractionNode
from data_etl_app.models.pipeline_nodes.classification.binary_classification_node import (
    BinaryClassificationNode,
)
from data_etl_app.models.pipeline_nodes.extraction.address_extraction_node import (
    AddressExtractionNode,
)
from data_etl_app.models.pipeline_nodes.extraction.keyword_search_node import (
    KeywordSearchNode,
)
from data_etl_app.models.pipeline_nodes.reconcile.address_reconcile_node import (
    AddressReconcileNode,
)
from data_etl_app.models.pipeline_nodes.reconcile.binary_reconcile_node import (
    BinaryReconcileNode,
)
from data_etl_app.models.pipeline_nodes.reconcile.business_desc_reconcile_node import (
    BusinessDescReconcileNode,
)
from data_etl_app.models.pipeline_nodes.reconcile.concept_reconcile_node import (
    ConceptReconcileNode,
)
from data_etl_app.models.pipeline_nodes.reconcile.keyword_reconcile_node import (
    KeywordReconcileNode,
)
from data_etl_app.models.types_and_enums import (
    BasicFieldTypeEnum,
    BinaryClassificationTypeEnum,
    ConceptTypeEnum,
    GenericFieldTypeEnum,
    KeywordTypeEnum,
)


class ExtractionPipelineFactory:
    """Creates extraction phase pipelines for each field"""

    @staticmethod
    def create_pipelines() -> dict[GenericFieldTypeEnum, ExtractionNode]:
        """
        Returns a dict mapping field names to their phase pipelines.
        Each pipeline is the head of a chain of phases.
        """
        return {
            # Single-phase extractions
            BinaryClassificationTypeEnum.is_manufacturer: BinaryClassificationNode(
                binary_field_type=BinaryClassificationTypeEnum.is_manufacturer,
                next_node=BinaryReconcileNode(
                    binary_field_type=BinaryClassificationTypeEnum.is_manufacturer
                ),
            ),
            BinaryClassificationTypeEnum.is_contract_manufacturer: BinaryClassificationNode(
                binary_field_type=BinaryClassificationTypeEnum.is_contract_manufacturer,
                next_node=BinaryReconcileNode(
                    binary_field_type=BinaryClassificationTypeEnum.is_contract_manufacturer
                ),
            ),
            BinaryClassificationTypeEnum.is_product_manufacturer: BinaryClassificationNode(
                binary_field_type=BinaryClassificationTypeEnum.is_product_manufacturer,
                next_node=BinaryReconcileNode(
                    binary_field_type=BinaryClassificationTypeEnum.is_product_manufacturer
                ),
            ),
            BasicFieldTypeEnum.addresses: AddressExtractionNode(
                field_type=BasicFieldTypeEnum.addresses,
                next_node=AddressReconcileNode(field_type=BasicFieldTypeEnum.addresses),
            ),
            BasicFieldTypeEnum.business_desc: BusinessDescExtractionNode(
                field_type=BasicFieldTypeEnum.business_desc,
                next_node=BusinessDescReconcileNode(
                    field_type=BasicFieldTypeEnum.business_desc
                ),
            ),
            KeywordTypeEnum.products: KeywordSearchNode(
                field_type=KeywordTypeEnum.products,
                next_node=KeywordReconcileNode(
                    field_type=KeywordTypeEnum.products,
                ),
            ),
            # Two-phase extractions (search -> mapping)
            ConceptTypeEnum.certificates: ConceptSearchNode(
                concept_type=ConceptTypeEnum.certificates,
                next_node=ConceptMappingNode(
                    concept_type=ConceptTypeEnum.certificates,
                    next_node=ConceptReconcileNode(
                        concept_type=ConceptTypeEnum.certificates
                    ),
                ),
            ),
            ConceptTypeEnum.industries: ConceptSearchNode(
                concept_type=ConceptTypeEnum.industries,
                next_node=ConceptMappingNode(
                    concept_type=ConceptTypeEnum.industries,
                    next_node=ConceptReconcileNode(
                        concept_type=ConceptTypeEnum.industries
                    ),
                ),
            ),
            ConceptTypeEnum.process_caps: ConceptSearchNode(
                concept_type=ConceptTypeEnum.process_caps,
                next_node=ConceptMappingNode(
                    concept_type=ConceptTypeEnum.process_caps,
                    next_node=ConceptReconcileNode(
                        concept_type=ConceptTypeEnum.process_caps
                    ),
                ),
            ),
            ConceptTypeEnum.material_caps: ConceptSearchNode(
                concept_type=ConceptTypeEnum.material_caps,
                next_node=ConceptMappingNode(
                    concept_type=ConceptTypeEnum.material_caps,
                    next_node=ConceptReconcileNode(
                        concept_type=ConceptTypeEnum.material_caps
                    ),
                ),
            ),
        }
