from datetime import datetime

from core.models.llm_model import LLM_Model
from core.models.extraction_results.llm_phrase_extraction_results import (
    ExtractionNodeMetadata,
    RecursiveSearchNodeMetadata,
)
from core.models.file_objects.prompt import Prompt
from core.models.extraction_results.single_stage_extraction_results import (
    LLMSingleStageExtractionMetadata,
)
from data_etl_app.models.chunking_strat import (
    ChunkingStrategy,
    CERTIFICATE_CHUNKING_STRAT,
    INDUSTRY_CHUNKING_STRAT,
    MATERIAL_CAP_CHUNKING_STRAT,
    PROCESS_CAP_CHUNKING_STRAT,
    PRODUCT_CHUNKING_STRAT,
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
    ConceptRelationshipNode,
    ConceptRelationshipScreeningNode,
    ConceptInitialGroundingNode,
    ConceptRecursiveGroundingNode,
    ConceptExtractionPrefillNode,
    ConceptReconcileNode,
    ConceptPhraseSearchNode,
    ConceptRecursiveSearchNode,
    KeywordExtractionPrefillNode,
    KeywordFreehandGroundingNode,
    KeywordRelationshipNode,
    KeywordRelationshipScreeningNode,
    KeywordReconcileNode,
    KeywordPhraseSearchNode,
    KeywordRecursiveSearchNode,
)
from data_etl_app.models.skos_concept import Concept
from data_etl_app.models.types_and_enums import (
    BinaryClassificationTypeEnum,
    ConceptTypeEnum,
    LLMExtractedFieldTypeEnum,
    KeywordTypeEnum,
    BasicFieldTypeEnum,
)
from data_etl_app.models.ontology import Ontology
from open_ai_key_app.models.gpt_model_params import GPTModelParams

from data_etl_app.services.knowledge.prompt_service import PromptService


class ExtractionPipelineFactory:
    """Creates extraction phase pipelines for each field"""

    DEFAULT_RECURSIVE_SEARCH_MAX_ROUNDS = 1

    @staticmethod
    def _metadata(
        prompt: Prompt,
        llm_model: LLM_Model,
        model_params: GPTModelParams,
        created_at: datetime,
    ) -> ExtractionNodeMetadata:
        return ExtractionNodeMetadata(
            llm_model=llm_model,
            model_params=model_params,
            prompt_name=prompt.name,
            prompt_version_id=prompt.s3_version_id,
            created_at=created_at,
        )

    @staticmethod
    def _recursive_search_metadata(
        prompt: Prompt,
        llm_model: LLM_Model,
        model_params: GPTModelParams,
        created_at: datetime,
        max_rounds: int,
    ) -> RecursiveSearchNodeMetadata:
        return RecursiveSearchNodeMetadata(
            llm_model=llm_model,
            model_params=model_params,
            prompt_name=prompt.name,
            prompt_version_id=prompt.s3_version_id,
            created_at=created_at,
            max_rounds=max_rounds,
        )

    @staticmethod
    def _single_stage_metadata(
        prompt: Prompt,
        llm_model: LLM_Model,
        model_params: GPTModelParams,
        created_at: datetime,
        chunk_strategy: ChunkingStrategy,
        ontology: Ontology,
    ) -> LLMSingleStageExtractionMetadata:
        return LLMSingleStageExtractionMetadata(
            single_stage=ExtractionPipelineFactory._metadata(
                prompt, llm_model, model_params, created_at
            ),
            created_at=created_at,
            chunk_strat=chunk_strategy,
            ontology_version_id=ontology.s3_version_id,
        )

    @staticmethod
    def create_concept_extraction_pipeline(
        concept_type: ConceptTypeEnum,
        chunk_strategy: ChunkingStrategy,
        ontology: Ontology,
        search_prompt: Prompt,
        recursive_search_prompt: Prompt,
        phrase_relationship_prompt: Prompt,
        phrase_relationship_screening_prompt: Prompt,
        phrase_initial_grounding_prompt: Prompt,
        phrase_recursive_grounding_prompt: Prompt,
        known_concepts: set[Concept],
        llm_model: LLM_Model,
        model_params: GPTModelParams,
        created_at: datetime,
        max_recursive_search_rounds: int = DEFAULT_RECURSIVE_SEARCH_MAX_ROUNDS,
    ) -> ConceptExtractionPrefillNode:
        return ConceptExtractionPrefillNode(
            field_type=concept_type,
            chunk_strategy=chunk_strategy,
            ontology=ontology,
            llm_phrase_search_metadata=ExtractionPipelineFactory._metadata(
                search_prompt, llm_model, model_params, created_at
            ),
            llm_phrase_recursive_search_metadata=ExtractionPipelineFactory._recursive_search_metadata(
                recursive_search_prompt,
                llm_model,
                model_params,
                created_at,
                max_recursive_search_rounds,
            ),
            llm_phrase_relationship_metadata=ExtractionPipelineFactory._metadata(
                phrase_relationship_prompt, llm_model, model_params, created_at
            ),
            llm_phrase_relationship_screening_metadata=ExtractionPipelineFactory._metadata(
                phrase_relationship_screening_prompt,
                llm_model,
                model_params,
                created_at,
            ),
            llm_phrase_initial_grounding_metadata=ExtractionPipelineFactory._metadata(
                phrase_initial_grounding_prompt, llm_model, model_params, created_at
            ),
            llm_phrase_recursive_grounding_metadata=ExtractionPipelineFactory._metadata(
                phrase_recursive_grounding_prompt, llm_model, model_params, created_at
            ),
            next_node=ConceptPhraseSearchNode(
                concept_type=concept_type,
                search_prompt=search_prompt,
                next_node=ConceptRecursiveSearchNode(
                    concept_type=concept_type,
                    second_search_prompt=recursive_search_prompt,
                    next_node=ConceptRelationshipNode(
                        concept_type=concept_type,
                        phrase_relationship_prompt=phrase_relationship_prompt,
                        next_node=ConceptRelationshipScreeningNode(
                            concept_type=concept_type,
                            phrase_relationship_screening_prompt=phrase_relationship_screening_prompt,
                            next_node=ConceptInitialGroundingNode(
                                concept_type=concept_type,
                                phrase_initial_grounding_prompt=phrase_initial_grounding_prompt,
                                known_concepts=known_concepts,
                                next_node=ConceptRecursiveGroundingNode(
                                    concept_type=concept_type,
                                    phrase_recursive_grounding_prompt=phrase_recursive_grounding_prompt,
                                    known_concepts=known_concepts,
                                    next_node=ConceptReconcileNode(
                                        concept_type=concept_type,
                                        known_concepts=known_concepts,
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
            ),
        )

    @staticmethod
    def create_binary_classification_pipeline(
        binary_field_type: BinaryClassificationTypeEnum,
        prompt: Prompt,
        llm_model: LLM_Model,
        model_params: GPTModelParams,
        ontology: Ontology,
        created_at: datetime,
    ) -> BinaryClassificationPrefillNode:
        chunk_strategy = get_binary_classification_chunking_strat(prompt=prompt)
        return BinaryClassificationPrefillNode(
            binary_field_type=binary_field_type,
            chunk_strategy=chunk_strategy,
            prompt=prompt,
            extraction_metadata=ExtractionPipelineFactory._single_stage_metadata(
                prompt=prompt,
                llm_model=llm_model,
                model_params=model_params,
                chunk_strategy=chunk_strategy,
                ontology=ontology,
                created_at=created_at,
            ),
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
        llm_model: LLM_Model,
        model_params: GPTModelParams,
        ontology: Ontology,
        created_at: datetime,
    ) -> BusinessDescPrefillNode:
        chunk_strategy = get_basic_field_chunking_strat(prompt=prompt)
        return BusinessDescPrefillNode(
            chunk_strategy=chunk_strategy,
            prompt=prompt,
            business_desc_extraction_metadata=ExtractionPipelineFactory._single_stage_metadata(
                prompt=prompt,
                llm_model=llm_model,
                model_params=model_params,
                created_at=created_at,
                chunk_strategy=chunk_strategy,
                ontology=ontology,
            ),
            next_node=BusinessDescExtractionNode(
                extract_prompt=prompt,
                next_node=BusinessDescReconcileNode(),
            ),
        )

    @staticmethod
    def create_address_pipeline(
        prompt: Prompt,
        llm_model: LLM_Model,
        model_params: GPTModelParams,
        ontology: Ontology,
        created_at: datetime,
    ) -> AddressPrefillNode:
        chunk_strategy = get_basic_field_chunking_strat(prompt=prompt)
        return AddressPrefillNode(
            chunk_strategy=chunk_strategy,
            prompt=prompt,
            address_extraction_metadata=ExtractionPipelineFactory._single_stage_metadata(
                prompt=prompt,
                llm_model=llm_model,
                model_params=model_params,
                created_at=created_at,
                chunk_strategy=chunk_strategy,
                ontology=ontology,
            ),
            next_node=AddressExtractionNode(
                extract_prompt=prompt,
                next_node=AddressReconcileNode(),
            ),
        )

    @staticmethod
    def create_keyword_extraction_pipeline(
        keyword_type: KeywordTypeEnum,
        chunk_strategy: ChunkingStrategy,
        ontology_version_id: str,
        search_prompt: Prompt,
        recursive_search_prompt: Prompt,
        phrase_relationship_prompt: Prompt,
        phrase_relationship_screening_prompt: Prompt,
        phrase_freehand_grounding_prompt: Prompt,
        llm_model: LLM_Model,
        model_params: GPTModelParams,
        created_at: datetime,
        max_recursive_search_rounds: int = DEFAULT_RECURSIVE_SEARCH_MAX_ROUNDS,
    ) -> KeywordExtractionPrefillNode:
        return KeywordExtractionPrefillNode(
            field_type=keyword_type,
            chunk_strategy=chunk_strategy,
            ontology_version_id=ontology_version_id,
            llm_phrase_search_metadata=ExtractionPipelineFactory._metadata(
                search_prompt, llm_model, model_params, created_at
            ),
            llm_phrase_recursive_search_metadata=ExtractionPipelineFactory._recursive_search_metadata(
                recursive_search_prompt,
                llm_model,
                model_params,
                created_at,
                max_recursive_search_rounds,
            ),
            llm_phrase_relationship_metadata=ExtractionPipelineFactory._metadata(
                phrase_relationship_prompt, llm_model, model_params, created_at
            ),
            llm_phrase_relationship_screening_metadata=ExtractionPipelineFactory._metadata(
                phrase_relationship_screening_prompt,
                llm_model,
                model_params,
                created_at,
            ),
            llm_phrase_freehand_grounding_metadata=ExtractionPipelineFactory._metadata(
                phrase_freehand_grounding_prompt, llm_model, model_params, created_at
            ),
            next_node=KeywordPhraseSearchNode(
                field_type=keyword_type,
                search_prompt=search_prompt,
                next_node=KeywordRecursiveSearchNode(
                    field_type=keyword_type,
                    second_search_prompt=recursive_search_prompt,
                    next_node=KeywordRelationshipNode(
                        field_type=keyword_type,
                        phrase_relationship_prompt=phrase_relationship_prompt,
                        next_node=KeywordRelationshipScreeningNode(
                            field_type=keyword_type,
                            phrase_relationship_screening_prompt=phrase_relationship_screening_prompt,
                            next_node=KeywordFreehandGroundingNode(
                                field_type=keyword_type,
                                phrase_freehand_grounding_prompt=phrase_freehand_grounding_prompt,
                                next_node=KeywordReconcileNode(
                                    field_type=keyword_type,
                                ),
                            ),
                        ),
                    ),
                ),
            ),
        )

    @staticmethod
    def create_pipelines(
        prompt_service: PromptService,
        ontology: Ontology,
        llm_model: LLM_Model,
        model_params: GPTModelParams,
        created_at: datetime,
    ) -> dict[LLMExtractedFieldTypeEnum, PrefillNode]:
        """
        Returns a dict mapping field names to their phase pipelines.
        Each pipeline is the head of a chain of phases.
        """

        return {
            # Single-stage extractions
            BasicFieldTypeEnum.addresses: ExtractionPipelineFactory.create_address_pipeline(
                prompt=prompt_service.extract_any_address_prompt,
                llm_model=llm_model,
                ontology=ontology,
                created_at=created_at,
                model_params=model_params,
            ),
            KeywordTypeEnum.products: ExtractionPipelineFactory.create_keyword_extraction_pipeline(
                keyword_type=KeywordTypeEnum.products,
                chunk_strategy=PRODUCT_CHUNKING_STRAT,
                search_prompt=prompt_service.product_phrase_search_prompt,
                recursive_search_prompt=prompt_service.product_phrase_recursive_search_prompt,
                phrase_relationship_prompt=prompt_service.product_phrase_relationship_prompt,
                phrase_relationship_screening_prompt=prompt_service.product_phrase_relationship_screening_prompt,
                phrase_freehand_grounding_prompt=prompt_service.product_phrase_freehand_grounding_prompt,
                ontology_version_id=ontology.s3_version_id,
                llm_model=llm_model,
                model_params=model_params,
                created_at=created_at,
            ),
            # Three-stage extractions (search -> phrase_relationship -> mapping)
            ConceptTypeEnum.certificates: ExtractionPipelineFactory.create_concept_extraction_pipeline(
                concept_type=ConceptTypeEnum.certificates,
                chunk_strategy=CERTIFICATE_CHUNKING_STRAT,
                ontology=ontology,
                search_prompt=prompt_service.certificate_phrase_search_prompt,
                recursive_search_prompt=prompt_service.certificate_phrase_recursive_search_prompt,
                phrase_relationship_prompt=prompt_service.certificate_phrase_relationship_prompt,
                phrase_relationship_screening_prompt=prompt_service.certificate_phrase_relationship_screening_prompt,
                phrase_initial_grounding_prompt=prompt_service.certificate_phrase_initial_grounding_prompt,
                phrase_recursive_grounding_prompt=prompt_service.certificate_phrase_recursive_grounding_prompt,
                known_concepts=ontology.certificates,
                llm_model=llm_model,
                model_params=model_params,
                created_at=created_at,
            ),
            ConceptTypeEnum.industries: ExtractionPipelineFactory.create_concept_extraction_pipeline(
                concept_type=ConceptTypeEnum.industries,
                chunk_strategy=INDUSTRY_CHUNKING_STRAT,
                ontology=ontology,
                search_prompt=prompt_service.industry_phrase_search_prompt,
                recursive_search_prompt=prompt_service.industry_phrase_recursive_search_prompt,
                phrase_relationship_prompt=prompt_service.industry_phrase_relationship_prompt,
                phrase_relationship_screening_prompt=prompt_service.industry_phrase_relationship_screening_prompt,
                phrase_initial_grounding_prompt=prompt_service.industry_phrase_initial_grounding_prompt,
                phrase_recursive_grounding_prompt=prompt_service.industry_phrase_recursive_grounding_prompt,
                known_concepts=ontology.industries,
                llm_model=llm_model,
                model_params=model_params,
                created_at=created_at,
            ),
            ConceptTypeEnum.process_caps: ExtractionPipelineFactory.create_concept_extraction_pipeline(
                concept_type=ConceptTypeEnum.process_caps,
                chunk_strategy=PROCESS_CAP_CHUNKING_STRAT,
                ontology=ontology,
                search_prompt=prompt_service.process_cap_phrase_search_prompt,
                recursive_search_prompt=prompt_service.process_cap_phrase_recursive_search_prompt,
                phrase_relationship_prompt=prompt_service.process_cap_phrase_relationship_prompt,
                phrase_relationship_screening_prompt=prompt_service.process_cap_phrase_relationship_screening_prompt,
                phrase_initial_grounding_prompt=prompt_service.process_cap_phrase_initial_grounding_prompt,
                phrase_recursive_grounding_prompt=prompt_service.process_cap_phrase_recursive_grounding_prompt,
                known_concepts=ontology.process_caps,
                llm_model=llm_model,
                model_params=model_params,
                created_at=created_at,
            ),
            ConceptTypeEnum.material_caps: ExtractionPipelineFactory.create_concept_extraction_pipeline(
                concept_type=ConceptTypeEnum.material_caps,
                chunk_strategy=MATERIAL_CAP_CHUNKING_STRAT,
                ontology=ontology,
                search_prompt=prompt_service.material_cap_phrase_search_prompt,
                recursive_search_prompt=prompt_service.material_cap_phrase_recursive_search_prompt,
                phrase_relationship_prompt=prompt_service.material_cap_phrase_relationship_prompt,
                phrase_relationship_screening_prompt=prompt_service.material_cap_phrase_relationship_screening_prompt,
                phrase_initial_grounding_prompt=prompt_service.material_cap_phrase_initial_grounding_prompt,
                phrase_recursive_grounding_prompt=prompt_service.material_cap_phrase_recursive_grounding_prompt,
                known_concepts=ontology.material_caps,
                llm_model=llm_model,
                model_params=model_params,
                created_at=created_at,
            ),
        }
