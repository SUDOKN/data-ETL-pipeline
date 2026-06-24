from core.models.prompt import Prompt

from data_etl_app.models.chunking_strat import (
    ChunkingStrategy,
    CERTIFICATE_CHUNKING_STRAT,
    INDUSTRY_CHUNKING_STRAT,
    MATERIAL_CAP_CHUNKING_STRAT,
    PROCESS_CAP_CHUNKING_STRAT,
    PRODUCT_CHUNKING_STRAT,
    ChunkingStrategy,
    get_basic_field_chunking_strat,
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
    ConceptDistillationNode,
    ConceptExtractionPrefillNode,
    ConceptMappingNode,
    ConceptReconcileNode,
    ConceptSearchNode,
    KeywordExtractionPrefillNode,
    KeywordDistillationNode,
    KeywordReconcileNode,
    KeywordSearchNode,
)
from data_etl_app.models.pipeline_nodes.single_stage_extraction_prefill_node import (
    SingleStageExtractionPrefillNode,
)
from data_etl_app.models.skos_concept import Concept
from data_etl_app.models.types_and_enums import (
    BasicFieldTypeEnum,
    BinaryClassificationTypeEnum,
    ConceptTypeEnum,
    LLMExtractedFieldTypeEnum,
    KeywordTypeEnum,
)
from data_etl_app.models.ontology import Ontology
from data_etl_app.services.knowledge.prompt_service import PromptService


class ExtractionPipelineFactory:
    """Creates extraction phase pipelines for each field"""

    @staticmethod
    def create_concept_extraction_pipeline(
        concept_type: ConceptTypeEnum,
        chunk_strategy: ChunkingStrategy,
        search_prompt: Prompt,
        distillation_prompt: Prompt,
        mapping_prompt: Prompt,
        ontology_version_id: str,
        known_concepts: set[Concept],
    ) -> ConceptExtractionPrefillNode:
        return ConceptExtractionPrefillNode(
            field_type=concept_type,
            chunk_strategy=chunk_strategy,
            search_prompt=search_prompt,
            distillation_prompt=distillation_prompt,
            mapping_prompt=mapping_prompt,
            ontology_version_id=ontology_version_id,
            known_concepts=known_concepts,
            next_node=ConceptSearchNode(
                concept_type=concept_type,
                search_prompt=search_prompt,
                next_node=ConceptDistillationNode(
                    concept_type=concept_type,
                    distillation_prompt=distillation_prompt,
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
    def create_business_desc_pipeline(
        prompt: Prompt,
    ) -> BusinessDescPrefillNode:
        return BusinessDescPrefillNode(
            chunk_strategy=get_basic_field_chunking_strat(prompt=prompt),
            prompt=prompt,
            next_node=BusinessDescExtractionNode(
                extract_prompt=prompt,
                next_node=BusinessDescReconcileNode(),
            ),
        )

    @staticmethod
    def create_pipelines(
        prompt_service: PromptService,
        ontology: Ontology,
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
            KeywordTypeEnum.products: KeywordExtractionPrefillNode(
                field_type=KeywordTypeEnum.products,
                chunk_strategy=PRODUCT_CHUNKING_STRAT,
                search_prompt=prompt_service.extract_any_product_prompt,
                distillation_prompt=prompt_service.product_distillation_prompt,
                next_node=KeywordSearchNode(
                    field_type=KeywordTypeEnum.products,
                    search_prompt=prompt_service.extract_any_product_prompt,
                    next_node=KeywordDistillationNode(
                        field_type=KeywordTypeEnum.products,
                        distillation_prompt=prompt_service.product_distillation_prompt,
                        next_node=KeywordReconcileNode(
                            field_type=KeywordTypeEnum.products,
                        ),
                    ),
                ),
            ),
            # Three-stage extractions (search -> distillation -> mapping)
            ConceptTypeEnum.certificates: ExtractionPipelineFactory.create_concept_extraction_pipeline(
                concept_type=ConceptTypeEnum.certificates,
                chunk_strategy=CERTIFICATE_CHUNKING_STRAT,
                search_prompt=prompt_service.extract_any_certificate_prompt,
                distillation_prompt=prompt_service.certificate_distillation_prompt,
                mapping_prompt=prompt_service.unknown_to_known_certificate_prompt,
                ontology_version_id=ontology.version_id,
                known_concepts=ontology.certificates,
            ),
            ConceptTypeEnum.industries: ExtractionPipelineFactory.create_concept_extraction_pipeline(
                concept_type=ConceptTypeEnum.industries,
                chunk_strategy=INDUSTRY_CHUNKING_STRAT,
                search_prompt=prompt_service.extract_any_industry_prompt,
                distillation_prompt=prompt_service.industry_distillation_prompt,
                mapping_prompt=prompt_service.unknown_to_known_industry_prompt,
                ontology_version_id=ontology.version_id,
                known_concepts=ontology.industries,
            ),
            ConceptTypeEnum.process_caps: ExtractionPipelineFactory.create_concept_extraction_pipeline(
                concept_type=ConceptTypeEnum.process_caps,
                chunk_strategy=PROCESS_CAP_CHUNKING_STRAT,
                search_prompt=prompt_service.extract_any_process_cap_prompt,
                distillation_prompt=prompt_service.process_cap_distillation_prompt,
                mapping_prompt=prompt_service.unknown_to_known_process_cap_prompt,
                ontology_version_id=ontology.version_id,
                known_concepts=ontology.process_caps,
            ),
            ConceptTypeEnum.material_caps: ExtractionPipelineFactory.create_concept_extraction_pipeline(
                concept_type=ConceptTypeEnum.material_caps,
                chunk_strategy=MATERIAL_CAP_CHUNKING_STRAT,
                search_prompt=prompt_service.extract_any_material_cap_prompt,
                distillation_prompt=prompt_service.material_cap_distillation_prompt,
                mapping_prompt=prompt_service.unknown_to_known_material_cap_prompt,
                ontology_version_id=ontology.version_id,
                known_concepts=ontology.material_caps,
            ),
        }
