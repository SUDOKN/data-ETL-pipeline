from datetime import datetime

from llm_providers.models.llm_model import LLM_Model
from core.models.extraction_results.llm_phrase_extraction_results import (
    ExtractionNodeMetadata,
    RecursiveSearchNodeMetadata,
    BatchedRelationshipNodeMetadata,
    BatchedScreeningNodeMetadata,
)
from core.models.extraction_results.concept_extraction_results import (
    BatchedInitialGroundingNodeMetadata,
)
from llm_providers.models.file_objects.prompt import Prompt
from core.models.extraction_results.single_stage_extraction_results import (
    LLMSingleStageExtractionMetadata,
)
from core.models.chunking_strat import (
    ChunkingStrategy,
    CERTIFICATE_CHUNKING_STRAT,
    EQUIPMENT_CHUNKING_STRAT,
    INDUSTRY_CHUNKING_STRAT,
    MATERIAL_CAP_CHUNKING_STRAT,
    PROCESS_CAP_CHUNKING_STRAT,
    PRODUCT_CHUNKING_STRAT,
    get_basic_field_chunking_strat,
    get_binary_classification_chunking_strat,
)
from data_etl_app.models.pipeline_nodes import (
    AddressPrefillNode,
    AddressExtractionNode,
    AddressReconcileNode,
    BusinessDescExtractionNode,
    BusinessDescPrefillNode,
    BusinessDescReconcileNode,
    ContractProductPhraseSearchNode,
    ContractProductRecursiveSearchNode,
    ContractProductRelationshipNode,
    ContractProductRelationshipScreeningNode,
    ContractProductFreehandGroundingNode,
    ContractProductReconcileNode,
    PureProductPhraseSearchNode,
    PureProductRecursiveSearchNode,
    PureProductRelationshipNode,
    PureProductRelationshipScreeningNode,
    PureProductFreehandGroundingNode,
    PureProductReconcileNode,
    EquipmentPhraseSearchNode,
    EquipmentRecursiveSearchNode,
    EquipmentRelationshipNode,
    EquipmentRelationshipScreeningNode,
    EquipmentFreehandGroundingNode,
    EquipmentReconcileNode,
)
from core.models.pipeline_nodes import (
    PrefillNode,
    BinaryClassificationNode,
    BinaryClassificationPrefillNode,
    BinaryReconcileNode,
    ConceptRelationshipNode,
    ConceptRelationshipScreeningNode,
    ConceptInitialGroundingNode,
    ConceptIterativeGroundingNode,
    ConceptExtractionPrefillNode,
    ConceptReconcileNode,
    ConceptPhraseSearchNode,
    ConceptRecursiveSearchNode,
    KeywordExtractionPrefillNode,
)
from core.models.skos_concept import Concept
from core.models.field_types import ExtractionFieldType
from data_etl_app.models.types_and_enums import (
    BinaryClassificationTypeEnum,
    ConceptTypeEnum,
    KeywordTypeEnum,
)
from data_etl_app.models.types_and_enums import (
    BasicFieldTypeEnum,
)
from core.models.ontology import Ontology
from llm_providers.models.open_ai.gpt_model_params import (
    GPTModelParams,
)

from data_etl_app.services.prompt_service import PromptService
from data_etl_app.services.prompt_assembly_service import (
    build_rule_catalog_lookup,
)
from core.services.rule_catalog_registry import set_rule_catalog_lookup


class ExtractionPipelineFactory:
    """Creates extraction phase pipelines for each field"""

    DEFAULT_RECURSIVE_SEARCH_MAX_ROUNDS = 1
    DEFAULT_RELATIONSHIP_MAX_PHRASES_PER_REQUEST = 50
    DEFAULT_SCREENING_MAX_PAIRS_PER_REQUEST = 15
    DEFAULT_INITIAL_GROUNDING_MAX_PAIRS_PER_REQUEST = 15

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
            catalog_version=prompt.catalog_version,
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
            catalog_version=prompt.catalog_version,
            created_at=created_at,
            max_rounds=max_rounds,
        )

    @staticmethod
    def _batched_relationship_metadata(
        prompt: Prompt,
        llm_model: LLM_Model,
        model_params: GPTModelParams,
        created_at: datetime,
        max_phrases_per_request: int,
    ) -> BatchedRelationshipNodeMetadata:
        return BatchedRelationshipNodeMetadata(
            llm_model=llm_model,
            model_params=model_params,
            prompt_name=prompt.name,
            prompt_version_id=prompt.s3_version_id,
            catalog_version=prompt.catalog_version,
            created_at=created_at,
            max_phrases_per_request=max_phrases_per_request,
        )

    @staticmethod
    def _batched_screening_metadata(
        prompt: Prompt,
        llm_model: LLM_Model,
        model_params: GPTModelParams,
        created_at: datetime,
        max_pairs_per_request: int,
    ) -> BatchedScreeningNodeMetadata:
        return BatchedScreeningNodeMetadata(
            llm_model=llm_model,
            model_params=model_params,
            prompt_name=prompt.name,
            prompt_version_id=prompt.s3_version_id,
            catalog_version=prompt.catalog_version,
            created_at=created_at,
            max_pairs_per_request=max_pairs_per_request,
        )

    @staticmethod
    def _batched_initial_grounding_metadata(
        prompt: Prompt,
        llm_model: LLM_Model,
        model_params: GPTModelParams,
        created_at: datetime,
        max_pairs_per_request: int,
    ) -> BatchedInitialGroundingNodeMetadata:
        return BatchedInitialGroundingNodeMetadata(
            llm_model=llm_model,
            model_params=model_params,
            prompt_name=prompt.name,
            prompt_version_id=prompt.s3_version_id,
            catalog_version=prompt.catalog_version,
            created_at=created_at,
            max_pairs_per_request=max_pairs_per_request,
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
        max_relationship_phrases_per_request: int = DEFAULT_RELATIONSHIP_MAX_PHRASES_PER_REQUEST,
        max_screening_pairs_per_request: int = DEFAULT_SCREENING_MAX_PAIRS_PER_REQUEST,
        max_initial_grounding_pairs_per_request: int = DEFAULT_INITIAL_GROUNDING_MAX_PAIRS_PER_REQUEST,
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
            llm_phrase_relationship_metadata=ExtractionPipelineFactory._batched_relationship_metadata(
                phrase_relationship_prompt,
                llm_model,
                model_params,
                created_at,
                max_relationship_phrases_per_request,
            ),
            llm_phrase_relationship_screening_metadata=ExtractionPipelineFactory._batched_screening_metadata(
                phrase_relationship_screening_prompt,
                llm_model,
                model_params,
                created_at,
                max_screening_pairs_per_request,
            ),
            llm_phrase_initial_grounding_metadata=ExtractionPipelineFactory._batched_initial_grounding_metadata(
                phrase_initial_grounding_prompt,
                llm_model,
                model_params,
                created_at,
                max_initial_grounding_pairs_per_request,
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
                                next_node=ConceptIterativeGroundingNode(
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
    def create_contract_product_extraction_pipeline(
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
        max_relationship_phrases_per_request: int = DEFAULT_RELATIONSHIP_MAX_PHRASES_PER_REQUEST,
        max_screening_pairs_per_request: int = DEFAULT_SCREENING_MAX_PAIRS_PER_REQUEST,
    ) -> KeywordExtractionPrefillNode:
        """Builds the contract-manufacturing product-extraction pipeline.

        The search / recursive-search / relationship phases are SHARED with the
        pure-product pipeline (see the Contract* node ``get_request_custom_id``
        overrides, which force the same custom_id as the ``products`` pipeline for
        those 3 phases so the LLM is only called once). Only screening (contract-
        specific prompt), freehand grounding, and reconcile are independently
        tracked under ``KeywordTypeEnum.contract_products``.
        """
        keyword_type = KeywordTypeEnum.contract_products
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
            llm_phrase_relationship_metadata=ExtractionPipelineFactory._batched_relationship_metadata(
                phrase_relationship_prompt,
                llm_model,
                model_params,
                created_at,
                max_relationship_phrases_per_request,
            ),
            llm_phrase_relationship_screening_metadata=ExtractionPipelineFactory._batched_screening_metadata(
                phrase_relationship_screening_prompt,
                llm_model,
                model_params,
                created_at,
                max_screening_pairs_per_request,
            ),
            llm_phrase_freehand_grounding_metadata=ExtractionPipelineFactory._metadata(
                phrase_freehand_grounding_prompt, llm_model, model_params, created_at
            ),
            next_node=ContractProductPhraseSearchNode(
                field_type=keyword_type,
                search_prompt=search_prompt,
                next_node=ContractProductRecursiveSearchNode(
                    field_type=keyword_type,
                    second_search_prompt=recursive_search_prompt,
                    next_node=ContractProductRelationshipNode(
                        field_type=keyword_type,
                        phrase_relationship_prompt=phrase_relationship_prompt,
                        next_node=ContractProductRelationshipScreeningNode(
                            field_type=keyword_type,
                            phrase_relationship_screening_prompt=phrase_relationship_screening_prompt,
                            next_node=ContractProductFreehandGroundingNode(
                                field_type=keyword_type,
                                phrase_freehand_grounding_prompt=phrase_freehand_grounding_prompt,
                                next_node=ContractProductReconcileNode(
                                    field_type=keyword_type,
                                ),
                            ),
                        ),
                    ),
                ),
            ),
        )

    @staticmethod
    def create_equipment_extraction_pipeline(
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
        max_relationship_phrases_per_request: int = DEFAULT_RELATIONSHIP_MAX_PHRASES_PER_REQUEST,
        max_screening_pairs_per_request: int = DEFAULT_SCREENING_MAX_PAIRS_PER_REQUEST,
    ) -> KeywordExtractionPrefillNode:
        """Builds the equipment (machinery/tools a manufacturer operates, owns,
        uses, or otherwise has access to) extraction pipeline.

        Single-track, open free-text vocabulary (like ``products``) — no
        pure/contract-style split is needed for equipment.
        """
        keyword_type = KeywordTypeEnum.equipments
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
            llm_phrase_relationship_metadata=ExtractionPipelineFactory._batched_relationship_metadata(
                phrase_relationship_prompt,
                llm_model,
                model_params,
                created_at,
                max_relationship_phrases_per_request,
            ),
            llm_phrase_relationship_screening_metadata=ExtractionPipelineFactory._batched_screening_metadata(
                phrase_relationship_screening_prompt,
                llm_model,
                model_params,
                created_at,
                max_screening_pairs_per_request,
            ),
            llm_phrase_freehand_grounding_metadata=ExtractionPipelineFactory._metadata(
                phrase_freehand_grounding_prompt, llm_model, model_params, created_at
            ),
            next_node=EquipmentPhraseSearchNode(
                field_type=keyword_type,
                search_prompt=search_prompt,
                next_node=EquipmentRecursiveSearchNode(
                    field_type=keyword_type,
                    second_search_prompt=recursive_search_prompt,
                    next_node=EquipmentRelationshipNode(
                        field_type=keyword_type,
                        phrase_relationship_prompt=phrase_relationship_prompt,
                        next_node=EquipmentRelationshipScreeningNode(
                            field_type=keyword_type,
                            phrase_relationship_screening_prompt=phrase_relationship_screening_prompt,
                            next_node=EquipmentFreehandGroundingNode(
                                field_type=keyword_type,
                                phrase_freehand_grounding_prompt=phrase_freehand_grounding_prompt,
                                next_node=EquipmentReconcileNode(
                                    field_type=keyword_type,
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
        # Binary classification parses against a catalog too, and this pipeline is
        # built standalone (is_manufacturer in the orchestrator) rather than only
        # through create_pipelines, so it registers the lookup itself.
        set_rule_catalog_lookup(build_rule_catalog_lookup())
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
    def create_pure_product_extraction_pipeline(
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
        max_relationship_phrases_per_request: int = DEFAULT_RELATIONSHIP_MAX_PHRASES_PER_REQUEST,
        max_screening_pairs_per_request: int = DEFAULT_SCREENING_MAX_PAIRS_PER_REQUEST,
    ) -> KeywordExtractionPrefillNode:
        """Builds the pure-product (own/sell own products) extraction pipeline."""
        keyword_type = KeywordTypeEnum.products
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
            llm_phrase_relationship_metadata=ExtractionPipelineFactory._batched_relationship_metadata(
                phrase_relationship_prompt,
                llm_model,
                model_params,
                created_at,
                max_relationship_phrases_per_request,
            ),
            llm_phrase_relationship_screening_metadata=ExtractionPipelineFactory._batched_screening_metadata(
                phrase_relationship_screening_prompt,
                llm_model,
                model_params,
                created_at,
                max_screening_pairs_per_request,
            ),
            llm_phrase_freehand_grounding_metadata=ExtractionPipelineFactory._metadata(
                phrase_freehand_grounding_prompt, llm_model, model_params, created_at
            ),
            next_node=PureProductPhraseSearchNode(
                field_type=keyword_type,
                search_prompt=search_prompt,
                next_node=PureProductRecursiveSearchNode(
                    field_type=keyword_type,
                    second_search_prompt=recursive_search_prompt,
                    next_node=PureProductRelationshipNode(
                        field_type=keyword_type,
                        phrase_relationship_prompt=phrase_relationship_prompt,
                        next_node=PureProductRelationshipScreeningNode(
                            field_type=keyword_type,
                            phrase_relationship_screening_prompt=phrase_relationship_screening_prompt,
                            next_node=PureProductFreehandGroundingNode(
                                field_type=keyword_type,
                                phrase_freehand_grounding_prompt=phrase_freehand_grounding_prompt,
                                next_node=PureProductReconcileNode(
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
    ) -> dict[ExtractionFieldType, PrefillNode]:
        """
        Returns a dict mapping field names to their phase pipelines.
        Each pipeline is the head of a chain of phases.
        """
        # Rule catalogs live in this app but are read by the parse functions in
        # `core`, which cannot import from here. Registering at pipeline
        # construction covers every path that goes on to parse a response.
        set_rule_catalog_lookup(build_rule_catalog_lookup())

        return {
            # Single-stage extractions
            BasicFieldTypeEnum.addresses: ExtractionPipelineFactory.create_address_pipeline(
                prompt=prompt_service.extract_any_address_prompt,
                llm_model=llm_model,
                ontology=ontology,
                created_at=created_at,
                model_params=model_params,
            ),
            BinaryClassificationTypeEnum.is_product_manufacturer: ExtractionPipelineFactory.create_binary_classification_pipeline(
                binary_field_type=BinaryClassificationTypeEnum.is_product_manufacturer,
                prompt=prompt_service.is_product_manufacturer_prompt,
                llm_model=llm_model,
                ontology=ontology,
                created_at=created_at,
                model_params=model_params,
            ),
            BinaryClassificationTypeEnum.is_contract_manufacturer: ExtractionPipelineFactory.create_binary_classification_pipeline(
                binary_field_type=BinaryClassificationTypeEnum.is_contract_manufacturer,
                prompt=prompt_service.is_contract_manufacturer_prompt,
                llm_model=llm_model,
                ontology=ontology,
                created_at=created_at,
                model_params=model_params,
            ),
            KeywordTypeEnum.products: ExtractionPipelineFactory.create_pure_product_extraction_pipeline(
                chunk_strategy=PRODUCT_CHUNKING_STRAT,
                search_prompt=prompt_service.product_phrase_search_prompt,
                recursive_search_prompt=prompt_service.product_phrase_recursive_search_prompt,
                phrase_relationship_prompt=prompt_service.product_phrase_relationship_prompt,
                phrase_relationship_screening_prompt=prompt_service.product_phrase_screening_pure_product_prompt,
                phrase_freehand_grounding_prompt=prompt_service.product_phrase_freehand_grounding_pure_product_prompt,
                ontology_version_id=ontology.s3_version_id,
                llm_model=llm_model,
                model_params=model_params,
                created_at=created_at,
            ),
            KeywordTypeEnum.contract_products: ExtractionPipelineFactory.create_contract_product_extraction_pipeline(
                chunk_strategy=PRODUCT_CHUNKING_STRAT,
                ontology_version_id=ontology.s3_version_id,
                search_prompt=prompt_service.product_phrase_search_prompt,
                recursive_search_prompt=prompt_service.product_phrase_recursive_search_prompt,
                phrase_relationship_prompt=prompt_service.product_phrase_relationship_prompt,
                phrase_relationship_screening_prompt=prompt_service.product_phrase_screening_contract_prompt,
                phrase_freehand_grounding_prompt=prompt_service.product_phrase_freehand_grounding_contract_prompt,
                llm_model=llm_model,
                model_params=model_params,
                created_at=created_at,
            ),
            KeywordTypeEnum.equipments: ExtractionPipelineFactory.create_equipment_extraction_pipeline(
                chunk_strategy=EQUIPMENT_CHUNKING_STRAT,
                ontology_version_id=ontology.s3_version_id,
                search_prompt=prompt_service.equipment_phrase_search_prompt,
                recursive_search_prompt=prompt_service.equipment_phrase_recursive_search_prompt,
                phrase_relationship_prompt=prompt_service.equipment_phrase_relationship_prompt,
                phrase_relationship_screening_prompt=prompt_service.equipment_phrase_relationship_screening_prompt,
                phrase_freehand_grounding_prompt=prompt_service.equipment_phrase_freehand_grounding_prompt,
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
                known_concepts=ontology.get_concepts_flat(ConceptTypeEnum.certificates),
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
                known_concepts=ontology.get_concepts_flat(ConceptTypeEnum.industries),
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
                known_concepts=ontology.get_concepts_flat(ConceptTypeEnum.process_caps),
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
                known_concepts=ontology.get_concepts_flat(
                    ConceptTypeEnum.material_caps
                ),
                llm_model=llm_model,
                model_params=model_params,
                created_at=created_at,
            ),
        }
