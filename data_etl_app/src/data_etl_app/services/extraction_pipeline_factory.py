from core.models.prompt import Prompt

from data_etl_app.models.chunking_strat import (
    ChunkingStrategy,
    CERTIFICATE_CHUNKING_STRAT,
    INDUSTRY_CHUNKING_STRAT,
    MATERIAL_CAP_CHUNKING_STRAT,
    PROCESS_CAP_CHUNKING_STRAT,
    PRODUCT_CHUNKING_STRAT,
    ChunkingStrategy,
    get_binary_classification_chunking_strat,
)
from data_etl_app.models.pipeline_nodes import (
    PrefillNode,
    AddressPrefillNode,
    AddressExtractionNode,
    AddressReconcileNode,
    BusinessDescExtractionNode,
    BusinessDescPrefillNode,
    BusinessDescReconcileNode,
    BinaryClassificationNode,
    BinaryClassificationPrefillNode,
    BinaryReconcileNode,
    ConceptEvidenceNode,
    ConceptExtractionPrefillNode,
    ConceptMappingNode,
    ConceptReconcileNode,
    ConceptSearchNode,
    KeywordExtractionPrefillNode,
    KeywordReconcileNode,
    KeywordSearchNode,
)
from data_etl_app.models.skos_concept import Concept
from data_etl_app.models.types_and_enums import (
    BasicFieldTypeEnum,
    BinaryClassificationTypeEnum,
    ConceptTypeEnum,
    LLMExtractedFieldTypeEnum,
    KeywordTypeEnum,
)
from data_etl_app.services.knowledge.ontology_service import OntologyService
from data_etl_app.services.knowledge.prompt_service import PromptService


class ExtractionPipelineFactory:
    """Creates extraction phase pipelines for each field"""

    @staticmethod
    def create_concept_extraction_pipeline(
        concept_type: ConceptTypeEnum,
        chunk_strategy: ChunkingStrategy,
        search_prompt: Prompt,
        evidence_prompt: Prompt,
        mapping_prompt: Prompt,
        ontology_version_id: str,
        known_concepts: set[Concept],
    ) -> ConceptExtractionPrefillNode:
        return ConceptExtractionPrefillNode(
            field_type=concept_type,
            chunk_strategy=chunk_strategy,
            search_prompt=search_prompt,
            evidence_prompt=evidence_prompt,
            mapping_prompt=mapping_prompt,
            ontology_version_id=ontology_version_id,
            known_concepts=known_concepts,
            next_node=ConceptSearchNode(
                concept_type=concept_type,
                search_prompt=search_prompt,
                next_node=ConceptEvidenceNode(
                    concept_type=concept_type,
                    evidence_prompt=evidence_prompt,
                    next_node=ConceptMappingNode(
                        concept_type=concept_type,
                        mapping_prompt=mapping_prompt,
                        known_concepts=known_concepts,
                        next_node=ConceptReconcileNode(
                            concept_type=concept_type, known_concepts=known_concepts
                        ),
                    ),
                ),
            ),
        )

    @staticmethod
    def create_binary_classification_pipeline(
        binary_field_type: BinaryClassificationTypeEnum,
        prompt: Prompt,
    ) -> BinaryClassificationPrefillNode:
        return BinaryClassificationPrefillNode(
            binary_field_type=binary_field_type,
            chunk_strategy=get_binary_classification_chunking_strat(prompt=prompt),
            prompt=prompt,
            next_node=BinaryClassificationNode(
                binary_field_type=binary_field_type,
                classification_prompt=prompt,
                next_node=BinaryReconcileNode(
                    binary_field_type=binary_field_type,
                ),
            ),
        )

    @staticmethod
    def create_pipelines(
        prompt_service: PromptService,
        ontology_service: OntologyService,
    ) -> dict[LLMExtractedFieldTypeEnum, PrefillNode]:
        """
        Returns a dict mapping field names to their phase pipelines.
        Each pipeline is the head of a chain of phases.
        """

        return {
            # Single-stage extractions
            # BasicFieldTypeEnum.addresses: AddressPrefillNode(
            #     chunk_strategy=get_single_shot_chunking_strat(
            #         gpt_model=llm_model,
            #         prompt=prompt_service.extract_any_address_prompt,
            #     ),
            #     prompt=prompt_service.extract_any_address_prompt,
            #     next_node=AddressExtractionNode(
            #         extract_prompt=prompt_service.extract_any_address_prompt,
            #         next_node=AddressReconcileNode(),
            #     ),
            # ),
            # BasicFieldTypeEnum.business_desc: BusinessDescPrefillNode(
            #     chunk_strategy=get_single_shot_chunking_strat(
            #         gpt_model=llm_model,
            #         prompt=prompt_service.find_business_desc_prompt,
            #     ),
            #     prompt=prompt_service.find_business_desc_prompt,
            #     next_node=BusinessDescExtractionNode(
            #         extract_prompt=prompt_service.find_business_desc_prompt,
            #         next_node=BusinessDescReconcileNode(),
            #     ),
            # ),
            # KeywordTypeEnum.products: KeywordExtractionPrefillNode(
            #     field_type=KeywordTypeEnum.products,
            #     chunk_strategy=PRODUCT_CHUNKING_STRAT,
            #     search_prompt=prompt_service.extract_any_product_prompt,
            #     next_node=KeywordSearchNode(
            #         field_type=KeywordTypeEnum.products,
            #         search_prompt=prompt_service.extract_any_product_prompt,
            #         next_node=KeywordReconcileNode(
            #             field_type=KeywordTypeEnum.products,
            #         ),
            #     ),
            # ),
            # Three-stage extractions (search -> evidence -> mapping)
            ConceptTypeEnum.certificates: ExtractionPipelineFactory.create_concept_extraction_pipeline(
                concept_type=ConceptTypeEnum.certificates,
                chunk_strategy=CERTIFICATE_CHUNKING_STRAT,
                search_prompt=prompt_service.extract_any_certificate_prompt,
                evidence_prompt=prompt_service.certificate_evidence_prompt,
                mapping_prompt=prompt_service.unknown_to_known_certificate_prompt,
                ontology_version_id=ontology_service.version_id,
                known_concepts=ontology_service.certificates[1],
            ),
            ConceptTypeEnum.industries: ExtractionPipelineFactory.create_concept_extraction_pipeline(
                concept_type=ConceptTypeEnum.industries,
                chunk_strategy=INDUSTRY_CHUNKING_STRAT,
                search_prompt=prompt_service.extract_any_industry_prompt,
                evidence_prompt=prompt_service.industry_evidence_prompt,
                mapping_prompt=prompt_service.unknown_to_known_industry_prompt,
                ontology_version_id=ontology_service.version_id,
                known_concepts=ontology_service.industries[1],
            ),
            ConceptTypeEnum.process_caps: ExtractionPipelineFactory.create_concept_extraction_pipeline(
                concept_type=ConceptTypeEnum.process_caps,
                chunk_strategy=PROCESS_CAP_CHUNKING_STRAT,
                search_prompt=prompt_service.extract_any_process_cap_prompt,
                evidence_prompt=prompt_service.process_cap_evidence_prompt,
                mapping_prompt=prompt_service.unknown_to_known_process_cap_prompt,
                ontology_version_id=ontology_service.version_id,
                known_concepts=ontology_service.process_caps[1],
            ),
            ConceptTypeEnum.material_caps: ExtractionPipelineFactory.create_concept_extraction_pipeline(
                concept_type=ConceptTypeEnum.material_caps,
                chunk_strategy=MATERIAL_CAP_CHUNKING_STRAT,
                search_prompt=prompt_service.extract_any_material_cap_prompt,
                evidence_prompt=prompt_service.material_cap_evidence_prompt,
                mapping_prompt=prompt_service.unknown_to_known_material_cap_prompt,
                ontology_version_id=ontology_service.version_id,
                known_concepts=ontology_service.material_caps[1],
            ),
        }
