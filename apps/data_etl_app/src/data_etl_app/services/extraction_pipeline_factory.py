from datetime import datetime
from typing import Optional

from llm_providers.models.llm_model import LLM_Model
from core.models.extraction_results.extraction_node_metadata import (
    AggregationFoldMetadata,
    BatchedMentionCollectionNodeMetadata,
    BatchedSynthesisNodeMetadata,
    ExtractionNodeMetadata,
    RecursiveSearchNodeMetadata,
    BatchedScreeningNodeMetadata,
)
from core.models.extraction_results.concept_extraction_results import (
    BatchedInitialGroundingNodeMetadata,
)
from core.models.extraction_results.keyword_extraction_results import (
    BatchedFreehandGroundingNodeMetadata,
)
from llm_providers.models.file_objects.prompt import Prompt
from core.models.extraction_results.single_stage_extraction_results import (
    LLMSingleStageExtractionMetadata,
)
from core.models.chunking_strat import (
    ChunkingStrategy,
    CONFORMITY_ATTESTATION_CHUNKING_STRAT,
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
    ContractProductMentionCollectionNode,
    ContractProductSynthesisNode,
    ContractProductRelationshipScreeningNode,
    ContractProductFreehandGroundingNode,
    ContractProductReconcileNode,
    PureProductPhraseSearchNode,
    PureProductRecursiveSearchNode,
    PureProductMentionCollectionNode,
    PureProductSynthesisNode,
    PureProductRelationshipScreeningNode,
    PureProductFreehandGroundingNode,
    PureProductReconcileNode,
    EquipmentPhraseSearchNode,
    EquipmentRecursiveSearchNode,
    EquipmentMentionCollectionNode,
    EquipmentSynthesisNode,
    EquipmentRelationshipScreeningNode,
    EquipmentFreehandGroundingNode,
    EquipmentReconcileNode,
)
from core.models.pipeline_nodes import (
    PrefillNode,
    BinaryClassificationNode,
    BinaryClassificationPrefillNode,
    BinaryReconcileNode,
    ConceptMentionCollectionNode,
    ConceptSynthesisNode,
    ConceptRelationshipScreeningNode,
    ConceptInitialGroundingNode,
    ConceptOovGroundingNode,
    ConceptIterativeGroundingNode,
    ConceptExtractionPrefillNode,
    ConceptReconcileNode,
    ConceptPhraseSearchNode,
    ConceptRecursiveSearchNode,
    KeywordExtractionPrefillNode,
)
from core.models.skos_concept import Concept
from core.utils.form_normalizer import NORMALIZER_VERSION
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

    # 0 since 2026-08-22 (user decision, v3 D3 verdict): the recursive round
    # measured on run 20260822T061410 returned 65% verbatim phrases (22% occur
    # nowhere in the window), ran on empty lists and dumped page menus, and in
    # its intended form-completion role carried ~10% of the occurrences the
    # first search already carried. max_rounds=0 makes the node a no-op
    # pass-through (the keyword pipelines already ran that way).
    DEFAULT_RECURSIVE_SEARCH_MAX_ROUNDS = 0
    # Keyword (open-vocabulary) pipelines skip recursive search by default;
    # max_rounds=0 makes the recursive-search node a no-op pass-through.
    DEFAULT_KEYWORD_RECURSIVE_SEARCH_MAX_ROUNDS = 0

    SEARCH_MAX_COMPLETION_TOKENS = 4000
    # Both search stages answer with a short JSON array of phrases: across the
    # 2026-08-16 alecmfg run the largest first-search response was 657 output
    # tokens (mean 229) and the largest recursive round 2,734 (mean 107), while
    # the caller's cap was 20,000. That headroom is what a repetition loop fills
    # — one recursive round spent all 20,000 tokens repeating a single phrase
    # 2,453 times and truncated mid-string — so the search stages carry their
    # own cap instead of inheriting the pipeline's.

    # MENTION COLLECTION (v3, PIPELINE_V3_PLAN.md D4–D7, as amended 2026-08-22):
    # the unit is MENTIONS — code collects a 5k sub-window's mentions, and the
    # window's distinct snippets go to the LLM for location in groups of this
    # size (measured 2026-08-22 on run 20260822T195947: median 23 distinct
    # snippets per window, p90 81, max 125 → most windows fit one request).
    DEFAULT_MENTION_COLLECTION_MAX_MENTIONS_PER_REQUEST = 50
    # v3 mention collection (user knob, 2026-08-22): the collector's snippet clip
    # radius in sentence units each side; 0 = the sentence-within-line clip.
    DEFAULT_MENTION_COLLECTION_SNIPPET_RADIUS = 0
    # v3 synthesis (PIPELINE_V3_PLAN.md D15/D16, Phase 3.2): the soft entry cap
    # per request — records packed in bundle order, never split — and the
    # location A/B arm (True = entries carry the Location stage's description;
    # False = snippet only). Both are request identity.
    DEFAULT_SYNTHESIS_MAX_ENTRIES_PER_REQUEST = 50
    DEFAULT_SYNTHESIS_INCLUDE_LOCATION = True
    # AGGREGATION FOLD (v3 D10): the L2 verb/participle fold is a per-field
    # dial — on for the two fields whose phrases are process-flavoured
    # (`CNC milled`/`CNC milling`, `Polished`/`Polishing`), off everywhere else,
    # where a verb fold only uglifies noun modifiers.
    VERB_FOLD_FIELDS: frozenset = frozenset(
        {ConceptTypeEnum.material_caps, ConceptTypeEnum.process_caps}
    )
    # v3 D21 (2026-08-27, user decision on measurement): a group whose surface
    # form is nothing but a coordination of sibling groups that already hold
    # every one of its mentions is not synthesized, grounded or screened. Only
    # sound now that mentions are decentralized (D8 reversed) — measured on run
    # 194457, 16 of 3,309 groups collapse and none of them loses evidence.
    DEFAULT_COLLAPSE_COMPOUNDS = True
    DEFAULT_SCREENING_MAX_PAIRS_PER_REQUEST = 25

    # GROUNDING (v2: the unit is RECORDS per request)
    DEFAULT_INITIAL_GROUNDING_MAX_PAIRS_PER_REQUEST = 25
    DEFAULT_OOV_GROUNDING_MAX_PAIRS_PER_REQUEST = 25
    DEFAULT_FREEHAND_GROUNDING_MAX_PAIRS_PER_REQUEST = 25

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
    def _search_metadata(
        prompt: Prompt,
        llm_model: LLM_Model,
        model_params: GPTModelParams,
        created_at: datetime,
    ) -> ExtractionNodeMetadata:
        """``_metadata`` under the search stages' own output cap."""
        return ExtractionPipelineFactory._metadata(
            prompt,
            llm_model,
            model_params.with_max_completion_tokens(
                ExtractionPipelineFactory.SEARCH_MAX_COMPLETION_TOKENS
            ),
            created_at,
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
            model_params=model_params.with_max_completion_tokens(
                ExtractionPipelineFactory.SEARCH_MAX_COMPLETION_TOKENS
            ),
            prompt_name=prompt.name,
            prompt_version_id=prompt.s3_version_id,
            catalog_version=prompt.catalog_version,
            created_at=created_at,
            max_rounds=max_rounds,
        )

    @staticmethod
    def _batched_mention_collection_metadata(
        prompt: Prompt,
        llm_model: LLM_Model,
        model_params: GPTModelParams,
        created_at: datetime,
        max_mentions_per_request: int,
        snippet_radius: int = DEFAULT_MENTION_COLLECTION_SNIPPET_RADIUS,
    ) -> BatchedMentionCollectionNodeMetadata:
        return BatchedMentionCollectionNodeMetadata(
            llm_model=llm_model,
            model_params=model_params,
            prompt_name=prompt.name,
            prompt_version_id=prompt.s3_version_id,
            catalog_version=prompt.catalog_version,
            created_at=created_at,
            max_mentions_per_request=max_mentions_per_request,
            snippet_radius=snippet_radius,
        )

    @staticmethod
    def _batched_synthesis_metadata(
        prompt: Prompt,
        llm_model: LLM_Model,
        model_params: GPTModelParams,
        created_at: datetime,
        max_entries_per_request: int,
        include_location: bool,
    ) -> BatchedSynthesisNodeMetadata:
        return BatchedSynthesisNodeMetadata(
            llm_model=llm_model,
            model_params=model_params,
            prompt_name=prompt.name,
            prompt_version_id=prompt.s3_version_id,
            catalog_version=prompt.catalog_version,
            created_at=created_at,
            max_entries_per_request=max_entries_per_request,
            include_location=include_location,
        )

    @staticmethod
    def _aggregation_fold_metadata(
        field_type: ExtractionFieldType,
    ) -> AggregationFoldMetadata:
        """The fold's run identity: the normalizer version the code ships,
        this field's verb-fold dial, and the compound-collapse dial (D21)."""
        return AggregationFoldMetadata(
            normalizer_version=NORMALIZER_VERSION,
            verb_fold=field_type in ExtractionPipelineFactory.VERB_FOLD_FIELDS,
            collapse_compounds=ExtractionPipelineFactory.DEFAULT_COLLAPSE_COMPOUNDS,
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
    def _batched_freehand_grounding_metadata(
        prompt: Prompt,
        llm_model: LLM_Model,
        model_params: GPTModelParams,
        created_at: datetime,
        max_pairs_per_request: int,
    ) -> BatchedFreehandGroundingNodeMetadata:
        return BatchedFreehandGroundingNodeMetadata(
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
        phrase_mention_collection_prompt: Prompt,
        phrase_relationship_screening_prompt: Prompt,
        phrase_initial_grounding_prompt: Prompt,
        phrase_recursive_grounding_prompt: Prompt,
        known_concepts: set[Concept],
        llm_model: LLM_Model,
        model_params: GPTModelParams,
        created_at: datetime,
        # None = the OOV discovery pass is off for this run (run config carried
        # as metadata identity, fork F6).
        phrase_oov_grounding_prompt: Optional[Prompt] = None,
        # v3 3.2: Optional only so older construction sites still compile; the
        # chain below always needs it (create_pipelines passes it).
        phrase_synthesis_prompt: Optional[Prompt] = None,
        max_recursive_search_rounds: int = DEFAULT_RECURSIVE_SEARCH_MAX_ROUNDS,
        max_mention_collection_mentions_per_request: int = DEFAULT_MENTION_COLLECTION_MAX_MENTIONS_PER_REQUEST,
        mention_collection_snippet_radius: int = DEFAULT_MENTION_COLLECTION_SNIPPET_RADIUS,
        max_synthesis_entries_per_request: int = DEFAULT_SYNTHESIS_MAX_ENTRIES_PER_REQUEST,
        synthesis_include_location: bool = DEFAULT_SYNTHESIS_INCLUDE_LOCATION,
        max_screening_pairs_per_request: int = DEFAULT_SCREENING_MAX_PAIRS_PER_REQUEST,
        max_initial_grounding_pairs_per_request: int = DEFAULT_INITIAL_GROUNDING_MAX_PAIRS_PER_REQUEST,
        max_oov_grounding_pairs_per_request: int = DEFAULT_OOV_GROUNDING_MAX_PAIRS_PER_REQUEST,
    ) -> ConceptExtractionPrefillNode:
        synthesis_prompt = ExtractionPipelineFactory._require_synthesis_prompt(
            phrase_synthesis_prompt, concept_type
        )
        return ConceptExtractionPrefillNode(
            field_type=concept_type,
            chunk_strategy=chunk_strategy,
            ontology=ontology,
            llm_phrase_search_metadata=ExtractionPipelineFactory._search_metadata(
                search_prompt, llm_model, model_params, created_at
            ),
            llm_phrase_recursive_search_metadata=ExtractionPipelineFactory._recursive_search_metadata(
                recursive_search_prompt,
                llm_model,
                model_params,
                created_at,
                max_recursive_search_rounds,
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
            llm_phrase_oov_grounding_metadata=(
                ExtractionPipelineFactory._batched_initial_grounding_metadata(
                    phrase_oov_grounding_prompt,
                    llm_model,
                    model_params,
                    created_at,
                    max_oov_grounding_pairs_per_request,
                )
                if phrase_oov_grounding_prompt is not None
                else None
            ),
            llm_phrase_recursive_grounding_metadata=ExtractionPipelineFactory._metadata(
                phrase_recursive_grounding_prompt, llm_model, model_params, created_at
            ),
            # v2 chain: grounding ENUMERATES first (in-vocab, then the optional
            # OOV discovery pass), consolidated screening vets every candidate,
            # and recursive descent deepens the survivors.
            llm_phrase_mention_collection_metadata=ExtractionPipelineFactory._batched_mention_collection_metadata(
                phrase_mention_collection_prompt,
                llm_model,
                model_params,
                created_at,
                max_mention_collection_mentions_per_request,
                mention_collection_snippet_radius,
            ),
            aggregation_fold_metadata=ExtractionPipelineFactory._aggregation_fold_metadata(
                concept_type
            ),
            llm_phrase_synthesis_metadata=ExtractionPipelineFactory._batched_synthesis_metadata(
                synthesis_prompt,
                llm_model,
                model_params,
                created_at,
                max_synthesis_entries_per_request,
                synthesis_include_location,
            ),
            next_node=ConceptPhraseSearchNode(
                concept_type=concept_type,
                search_prompt=search_prompt,
                next_node=ConceptRecursiveSearchNode(
                    concept_type=concept_type,
                    second_search_prompt=recursive_search_prompt,
                    # v3 (3.1): mention collection replaced relationship; the
                    # aggregation fold runs at its parse. v3 (3.2): synthesis
                    # writes one description per group. v3 (3.3, D16): the
                    # grounding → screening → descent tail below consumes the
                    # synthesis stage's per-group records keyed group_id.
                    next_node=ConceptMentionCollectionNode(
                        concept_type=concept_type,
                        phrase_mention_collection_prompt=phrase_mention_collection_prompt,
                        next_node=ConceptSynthesisNode(
                            concept_type=concept_type,
                            phrase_synthesis_prompt=synthesis_prompt,
                            next_node=ConceptInitialGroundingNode(
                                concept_type=concept_type,
                                phrase_initial_grounding_prompt=phrase_initial_grounding_prompt,
                                known_concepts=known_concepts,
                                next_node=ConceptOovGroundingNode(
                                    concept_type=concept_type,
                                    phrase_oov_grounding_prompt=phrase_oov_grounding_prompt,
                                    known_concepts=known_concepts,
                                    next_node=ConceptRelationshipScreeningNode(
                                        concept_type=concept_type,
                                        phrase_relationship_screening_prompt=phrase_relationship_screening_prompt,
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
                ),
            ),
        )

    @staticmethod
    def _require_synthesis_prompt(
        prompt: Optional[Prompt], field_type: ExtractionFieldType
    ) -> Prompt:
        """The v3 chain always carries synthesis; None is a wiring error at the
        construction site, not a run state."""
        if prompt is None:
            raise ValueError(
                f"phrase_synthesis_prompt is required to build the {field_type.name} "
                f"pipeline (v3 3.2): pass PromptService.<field>_phrase_synthesis_prompt."
            )
        return prompt

    @staticmethod
    def create_contract_product_extraction_pipeline(
        chunk_strategy: ChunkingStrategy,
        ontology_version_id: str,
        search_prompt: Prompt,
        recursive_search_prompt: Prompt,
        phrase_mention_collection_prompt: Prompt,
        phrase_relationship_screening_prompt: Prompt,
        phrase_freehand_grounding_prompt: Prompt,
        llm_model: LLM_Model,
        model_params: GPTModelParams,
        created_at: datetime,
        # v3 3.2: Optional only so older construction sites still compile; the
        # chain always needs it (create_pipelines passes it).
        phrase_synthesis_prompt: Optional[Prompt] = None,
        max_recursive_search_rounds: int = DEFAULT_KEYWORD_RECURSIVE_SEARCH_MAX_ROUNDS,
        max_mention_collection_mentions_per_request: int = DEFAULT_MENTION_COLLECTION_MAX_MENTIONS_PER_REQUEST,
        mention_collection_snippet_radius: int = DEFAULT_MENTION_COLLECTION_SNIPPET_RADIUS,
        max_synthesis_entries_per_request: int = DEFAULT_SYNTHESIS_MAX_ENTRIES_PER_REQUEST,
        synthesis_include_location: bool = DEFAULT_SYNTHESIS_INCLUDE_LOCATION,
        max_screening_pairs_per_request: int = DEFAULT_SCREENING_MAX_PAIRS_PER_REQUEST,
        max_freehand_grounding_pairs_per_request: int = DEFAULT_FREEHAND_GROUNDING_MAX_PAIRS_PER_REQUEST,
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
            llm_phrase_search_metadata=ExtractionPipelineFactory._search_metadata(
                search_prompt, llm_model, model_params, created_at
            ),
            llm_phrase_recursive_search_metadata=ExtractionPipelineFactory._recursive_search_metadata(
                recursive_search_prompt,
                llm_model,
                model_params,
                created_at,
                max_recursive_search_rounds,
            ),
            llm_phrase_relationship_screening_metadata=ExtractionPipelineFactory._batched_screening_metadata(
                phrase_relationship_screening_prompt,
                llm_model,
                model_params,
                created_at,
                max_screening_pairs_per_request,
            ),
            llm_phrase_freehand_grounding_metadata=ExtractionPipelineFactory._batched_freehand_grounding_metadata(
                phrase_freehand_grounding_prompt,
                llm_model,
                model_params,
                created_at,
                max_freehand_grounding_pairs_per_request,
            ),
            llm_phrase_mention_collection_metadata=ExtractionPipelineFactory._batched_mention_collection_metadata(
                phrase_mention_collection_prompt,
                llm_model,
                model_params,
                created_at,
                max_mention_collection_mentions_per_request,
                mention_collection_snippet_radius,
            ),
            aggregation_fold_metadata=ExtractionPipelineFactory._aggregation_fold_metadata(
                keyword_type
            ),
            llm_phrase_synthesis_metadata=ExtractionPipelineFactory._batched_synthesis_metadata(
                ExtractionPipelineFactory._require_synthesis_prompt(
                    phrase_synthesis_prompt, keyword_type
                ),
                llm_model,
                model_params,
                created_at,
                max_synthesis_entries_per_request,
                synthesis_include_location,
            ),
            next_node=ContractProductPhraseSearchNode(
                field_type=keyword_type,
                search_prompt=search_prompt,
                next_node=ContractProductRecursiveSearchNode(
                    field_type=keyword_type,
                    second_search_prompt=recursive_search_prompt,
                    # v3 (3.1): mention collection replaced relationship; v3 (3.2):
                    # synthesis follows it. v3 (3.3, D16): the grounding →
                    # screening tail below consumes the synthesis stage's
                    # per-group records keyed group_id.
                    next_node=ContractProductMentionCollectionNode(
                        field_type=keyword_type,
                        phrase_mention_collection_prompt=phrase_mention_collection_prompt,
                        next_node=ContractProductSynthesisNode(
                            field_type=keyword_type,
                            phrase_synthesis_prompt=ExtractionPipelineFactory._require_synthesis_prompt(
                                phrase_synthesis_prompt, keyword_type
                            ),
                            next_node=ContractProductFreehandGroundingNode(
                                field_type=keyword_type,
                                phrase_freehand_grounding_prompt=phrase_freehand_grounding_prompt,
                                next_node=ContractProductRelationshipScreeningNode(
                                    field_type=keyword_type,
                                    phrase_relationship_screening_prompt=phrase_relationship_screening_prompt,
                                    next_node=ContractProductReconcileNode(
                                        field_type=keyword_type,
                                    ),
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
        phrase_mention_collection_prompt: Prompt,
        phrase_relationship_screening_prompt: Prompt,
        phrase_freehand_grounding_prompt: Prompt,
        llm_model: LLM_Model,
        model_params: GPTModelParams,
        created_at: datetime,
        # v3 3.2: Optional only so older construction sites still compile; the
        # chain always needs it (create_pipelines passes it).
        phrase_synthesis_prompt: Optional[Prompt] = None,
        max_recursive_search_rounds: int = DEFAULT_KEYWORD_RECURSIVE_SEARCH_MAX_ROUNDS,
        max_mention_collection_mentions_per_request: int = DEFAULT_MENTION_COLLECTION_MAX_MENTIONS_PER_REQUEST,
        mention_collection_snippet_radius: int = DEFAULT_MENTION_COLLECTION_SNIPPET_RADIUS,
        max_synthesis_entries_per_request: int = DEFAULT_SYNTHESIS_MAX_ENTRIES_PER_REQUEST,
        synthesis_include_location: bool = DEFAULT_SYNTHESIS_INCLUDE_LOCATION,
        max_screening_pairs_per_request: int = DEFAULT_SCREENING_MAX_PAIRS_PER_REQUEST,
        max_freehand_grounding_pairs_per_request: int = DEFAULT_FREEHAND_GROUNDING_MAX_PAIRS_PER_REQUEST,
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
            llm_phrase_search_metadata=ExtractionPipelineFactory._search_metadata(
                search_prompt, llm_model, model_params, created_at
            ),
            llm_phrase_recursive_search_metadata=ExtractionPipelineFactory._recursive_search_metadata(
                recursive_search_prompt,
                llm_model,
                model_params,
                created_at,
                max_recursive_search_rounds,
            ),
            llm_phrase_relationship_screening_metadata=ExtractionPipelineFactory._batched_screening_metadata(
                phrase_relationship_screening_prompt,
                llm_model,
                model_params,
                created_at,
                max_screening_pairs_per_request,
            ),
            llm_phrase_freehand_grounding_metadata=ExtractionPipelineFactory._batched_freehand_grounding_metadata(
                phrase_freehand_grounding_prompt,
                llm_model,
                model_params,
                created_at,
                max_freehand_grounding_pairs_per_request,
            ),
            llm_phrase_mention_collection_metadata=ExtractionPipelineFactory._batched_mention_collection_metadata(
                phrase_mention_collection_prompt,
                llm_model,
                model_params,
                created_at,
                max_mention_collection_mentions_per_request,
                mention_collection_snippet_radius,
            ),
            aggregation_fold_metadata=ExtractionPipelineFactory._aggregation_fold_metadata(
                keyword_type
            ),
            llm_phrase_synthesis_metadata=ExtractionPipelineFactory._batched_synthesis_metadata(
                ExtractionPipelineFactory._require_synthesis_prompt(
                    phrase_synthesis_prompt, keyword_type
                ),
                llm_model,
                model_params,
                created_at,
                max_synthesis_entries_per_request,
                synthesis_include_location,
            ),
            next_node=EquipmentPhraseSearchNode(
                field_type=keyword_type,
                search_prompt=search_prompt,
                next_node=EquipmentRecursiveSearchNode(
                    field_type=keyword_type,
                    second_search_prompt=recursive_search_prompt,
                    # v3 (3.1): mention collection replaced relationship; v3 (3.2):
                    # synthesis follows it. v3 (3.3, D16): the grounding →
                    # screening tail below consumes the synthesis stage's
                    # per-group records keyed group_id.
                    next_node=EquipmentMentionCollectionNode(
                        field_type=keyword_type,
                        phrase_mention_collection_prompt=phrase_mention_collection_prompt,
                        next_node=EquipmentSynthesisNode(
                            field_type=keyword_type,
                            phrase_synthesis_prompt=ExtractionPipelineFactory._require_synthesis_prompt(
                                phrase_synthesis_prompt, keyword_type
                            ),
                            next_node=EquipmentFreehandGroundingNode(
                                field_type=keyword_type,
                                phrase_freehand_grounding_prompt=phrase_freehand_grounding_prompt,
                                next_node=EquipmentRelationshipScreeningNode(
                                    field_type=keyword_type,
                                    phrase_relationship_screening_prompt=phrase_relationship_screening_prompt,
                                    next_node=EquipmentReconcileNode(
                                        field_type=keyword_type,
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
        phrase_mention_collection_prompt: Prompt,
        phrase_relationship_screening_prompt: Prompt,
        phrase_freehand_grounding_prompt: Prompt,
        llm_model: LLM_Model,
        model_params: GPTModelParams,
        created_at: datetime,
        # v3 3.2: Optional only so older construction sites still compile; the
        # chain always needs it (create_pipelines passes it).
        phrase_synthesis_prompt: Optional[Prompt] = None,
        max_recursive_search_rounds: int = DEFAULT_KEYWORD_RECURSIVE_SEARCH_MAX_ROUNDS,
        max_mention_collection_mentions_per_request: int = DEFAULT_MENTION_COLLECTION_MAX_MENTIONS_PER_REQUEST,
        mention_collection_snippet_radius: int = DEFAULT_MENTION_COLLECTION_SNIPPET_RADIUS,
        max_synthesis_entries_per_request: int = DEFAULT_SYNTHESIS_MAX_ENTRIES_PER_REQUEST,
        synthesis_include_location: bool = DEFAULT_SYNTHESIS_INCLUDE_LOCATION,
        max_screening_pairs_per_request: int = DEFAULT_SCREENING_MAX_PAIRS_PER_REQUEST,
        max_freehand_grounding_pairs_per_request: int = DEFAULT_FREEHAND_GROUNDING_MAX_PAIRS_PER_REQUEST,
    ) -> KeywordExtractionPrefillNode:
        """Builds the pure-product (own/sell own products) extraction pipeline."""
        keyword_type = KeywordTypeEnum.products
        return KeywordExtractionPrefillNode(
            field_type=keyword_type,
            chunk_strategy=chunk_strategy,
            ontology_version_id=ontology_version_id,
            llm_phrase_search_metadata=ExtractionPipelineFactory._search_metadata(
                search_prompt, llm_model, model_params, created_at
            ),
            llm_phrase_recursive_search_metadata=ExtractionPipelineFactory._recursive_search_metadata(
                recursive_search_prompt,
                llm_model,
                model_params,
                created_at,
                max_recursive_search_rounds,
            ),
            llm_phrase_relationship_screening_metadata=ExtractionPipelineFactory._batched_screening_metadata(
                phrase_relationship_screening_prompt,
                llm_model,
                model_params,
                created_at,
                max_screening_pairs_per_request,
            ),
            llm_phrase_freehand_grounding_metadata=ExtractionPipelineFactory._batched_freehand_grounding_metadata(
                phrase_freehand_grounding_prompt,
                llm_model,
                model_params,
                created_at,
                max_freehand_grounding_pairs_per_request,
            ),
            llm_phrase_mention_collection_metadata=ExtractionPipelineFactory._batched_mention_collection_metadata(
                phrase_mention_collection_prompt,
                llm_model,
                model_params,
                created_at,
                max_mention_collection_mentions_per_request,
                mention_collection_snippet_radius,
            ),
            aggregation_fold_metadata=ExtractionPipelineFactory._aggregation_fold_metadata(
                keyword_type
            ),
            llm_phrase_synthesis_metadata=ExtractionPipelineFactory._batched_synthesis_metadata(
                ExtractionPipelineFactory._require_synthesis_prompt(
                    phrase_synthesis_prompt, keyword_type
                ),
                llm_model,
                model_params,
                created_at,
                max_synthesis_entries_per_request,
                synthesis_include_location,
            ),
            next_node=PureProductPhraseSearchNode(
                field_type=keyword_type,
                search_prompt=search_prompt,
                next_node=PureProductRecursiveSearchNode(
                    field_type=keyword_type,
                    second_search_prompt=recursive_search_prompt,
                    # v3 (3.1): mention collection replaced relationship; v3 (3.2):
                    # synthesis follows it. v3 (3.3, D16): the grounding →
                    # screening tail below consumes the synthesis stage's
                    # per-group records keyed group_id.
                    next_node=PureProductMentionCollectionNode(
                        field_type=keyword_type,
                        phrase_mention_collection_prompt=phrase_mention_collection_prompt,
                        next_node=PureProductSynthesisNode(
                            field_type=keyword_type,
                            phrase_synthesis_prompt=ExtractionPipelineFactory._require_synthesis_prompt(
                                phrase_synthesis_prompt, keyword_type
                            ),
                            next_node=PureProductFreehandGroundingNode(
                                field_type=keyword_type,
                                phrase_freehand_grounding_prompt=phrase_freehand_grounding_prompt,
                                next_node=PureProductRelationshipScreeningNode(
                                    field_type=keyword_type,
                                    phrase_relationship_screening_prompt=phrase_relationship_screening_prompt,
                                    next_node=PureProductReconcileNode(
                                        field_type=keyword_type,
                                    ),
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
        chunk_strategy_overrides: (
            dict[ExtractionFieldType, ChunkingStrategy] | None
        ) = None,
        oov_grounding_enabled: bool = True,
        synthesis_include_location: bool = DEFAULT_SYNTHESIS_INCLUDE_LOCATION,
        mention_collection_snippet_radius: int = DEFAULT_MENTION_COLLECTION_SNIPPET_RADIUS,
        max_synthesis_entries_per_request: int = DEFAULT_SYNTHESIS_MAX_ENTRIES_PER_REQUEST,
    ) -> dict[ExtractionFieldType, PrefillNode]:
        """
        Returns a dict mapping field names to their phase pipelines.
        Each pipeline is the head of a chain of phases.

        ``chunk_strategy_overrides`` replaces the module-level default chunking
        strategy per field — an experimentation knob (chunk size / search_divisor
        sweeps from the notebook) that leaves the defaults in source untouched.
        Fields not in the mapping keep their defaults.

        ``oov_grounding_enabled`` is the OOV discovery pass's RUN CONFIG (fork
        F6): off means the concept metadata carries no oov node — a distinct
        run identity — and the pass embeds zero requests. Never a StageToggle.

        v3 knobs (user decisions 2026-08-22), all run identity, all applied to
        every phrase pipeline alike: ``synthesis_include_location`` picks the
        synthesis A/B arm (entries with or without the Location stage's
        description); ``mention_collection_snippet_radius`` widens the
        collector's snippet clip (0 = sentence within line);
        ``max_synthesis_entries_per_request`` is the soft packing cap.
        """
        # Rule catalogs live in this app but are read by the parse functions in
        # `core`, which cannot import from here. Registering at pipeline
        # construction covers every path that goes on to parse a response.
        set_rule_catalog_lookup(build_rule_catalog_lookup())

        overrides = chunk_strategy_overrides or {}

        def chunk_strat_for(
            field_type: ExtractionFieldType, default: ChunkingStrategy
        ) -> ChunkingStrategy:
            return overrides.get(field_type, default)

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
                chunk_strategy=chunk_strat_for(
                    KeywordTypeEnum.products, PRODUCT_CHUNKING_STRAT
                ),
                search_prompt=prompt_service.product_phrase_search_prompt,
                recursive_search_prompt=prompt_service.product_phrase_recursive_search_prompt,
                phrase_mention_collection_prompt=prompt_service.product_phrase_mention_collection_prompt,
                phrase_synthesis_prompt=prompt_service.product_phrase_synthesis_prompt,
                synthesis_include_location=synthesis_include_location,
                mention_collection_snippet_radius=mention_collection_snippet_radius,
                max_synthesis_entries_per_request=max_synthesis_entries_per_request,
                phrase_relationship_screening_prompt=prompt_service.product_phrase_screening_pure_product_prompt,
                phrase_freehand_grounding_prompt=prompt_service.product_phrase_freehand_grounding_prompt,
                ontology_version_id=ontology.s3_version_id,
                llm_model=llm_model,
                model_params=model_params,
                created_at=created_at,
            ),
            KeywordTypeEnum.contract_products: ExtractionPipelineFactory.create_contract_product_extraction_pipeline(
                chunk_strategy=chunk_strat_for(
                    KeywordTypeEnum.contract_products, PRODUCT_CHUNKING_STRAT
                ),
                ontology_version_id=ontology.s3_version_id,
                search_prompt=prompt_service.product_phrase_search_prompt,
                recursive_search_prompt=prompt_service.product_phrase_recursive_search_prompt,
                phrase_mention_collection_prompt=prompt_service.product_phrase_mention_collection_prompt,
                phrase_synthesis_prompt=prompt_service.product_phrase_synthesis_prompt,
                synthesis_include_location=synthesis_include_location,
                mention_collection_snippet_radius=mention_collection_snippet_radius,
                max_synthesis_entries_per_request=max_synthesis_entries_per_request,
                phrase_relationship_screening_prompt=prompt_service.product_phrase_screening_contract_prompt,
                phrase_freehand_grounding_prompt=prompt_service.product_phrase_freehand_grounding_prompt,
                llm_model=llm_model,
                model_params=model_params,
                created_at=created_at,
            ),
            KeywordTypeEnum.equipments: ExtractionPipelineFactory.create_equipment_extraction_pipeline(
                chunk_strategy=chunk_strat_for(
                    KeywordTypeEnum.equipments, EQUIPMENT_CHUNKING_STRAT
                ),
                ontology_version_id=ontology.s3_version_id,
                search_prompt=prompt_service.equipment_phrase_search_prompt,
                recursive_search_prompt=prompt_service.equipment_phrase_recursive_search_prompt,
                phrase_mention_collection_prompt=prompt_service.equipment_phrase_mention_collection_prompt,
                phrase_synthesis_prompt=prompt_service.equipment_phrase_synthesis_prompt,
                synthesis_include_location=synthesis_include_location,
                mention_collection_snippet_radius=mention_collection_snippet_radius,
                max_synthesis_entries_per_request=max_synthesis_entries_per_request,
                phrase_relationship_screening_prompt=prompt_service.equipment_phrase_relationship_screening_prompt,
                phrase_freehand_grounding_prompt=prompt_service.equipment_phrase_freehand_grounding_prompt,
                llm_model=llm_model,
                model_params=model_params,
                created_at=created_at,
            ),
            # Three-stage extractions (search -> phrase_relationship -> mapping)
            ConceptTypeEnum.conformity_attestations: ExtractionPipelineFactory.create_concept_extraction_pipeline(
                concept_type=ConceptTypeEnum.conformity_attestations,
                chunk_strategy=chunk_strat_for(
                    ConceptTypeEnum.conformity_attestations,
                    CONFORMITY_ATTESTATION_CHUNKING_STRAT,
                ),
                ontology=ontology,
                search_prompt=prompt_service.conformity_attestation_phrase_search_prompt,
                recursive_search_prompt=prompt_service.conformity_attestation_phrase_recursive_search_prompt,
                phrase_mention_collection_prompt=prompt_service.conformity_attestation_phrase_mention_collection_prompt,
                phrase_synthesis_prompt=prompt_service.conformity_attestation_phrase_synthesis_prompt,
                synthesis_include_location=synthesis_include_location,
                mention_collection_snippet_radius=mention_collection_snippet_radius,
                max_synthesis_entries_per_request=max_synthesis_entries_per_request,
                phrase_relationship_screening_prompt=prompt_service.conformity_attestation_phrase_relationship_screening_prompt,
                phrase_initial_grounding_prompt=prompt_service.conformity_attestation_phrase_initial_grounding_prompt,
                phrase_oov_grounding_prompt=(
                    prompt_service.conformity_attestation_phrase_oov_grounding_prompt
                    if oov_grounding_enabled
                    else None
                ),
                phrase_recursive_grounding_prompt=prompt_service.conformity_attestation_phrase_recursive_grounding_prompt,
                known_concepts=ontology.get_concepts_flat(
                    ConceptTypeEnum.conformity_attestations
                ),
                llm_model=llm_model,
                model_params=model_params,
                created_at=created_at,
            ),
            ConceptTypeEnum.industries: ExtractionPipelineFactory.create_concept_extraction_pipeline(
                concept_type=ConceptTypeEnum.industries,
                chunk_strategy=chunk_strat_for(
                    ConceptTypeEnum.industries, INDUSTRY_CHUNKING_STRAT
                ),
                ontology=ontology,
                search_prompt=prompt_service.industry_phrase_search_prompt,
                recursive_search_prompt=prompt_service.industry_phrase_recursive_search_prompt,
                phrase_mention_collection_prompt=prompt_service.industry_phrase_mention_collection_prompt,
                phrase_synthesis_prompt=prompt_service.industry_phrase_synthesis_prompt,
                synthesis_include_location=synthesis_include_location,
                mention_collection_snippet_radius=mention_collection_snippet_radius,
                max_synthesis_entries_per_request=max_synthesis_entries_per_request,
                phrase_relationship_screening_prompt=prompt_service.industry_phrase_relationship_screening_prompt,
                phrase_initial_grounding_prompt=prompt_service.industry_phrase_initial_grounding_prompt,
                phrase_oov_grounding_prompt=(
                    prompt_service.industry_phrase_oov_grounding_prompt
                    if oov_grounding_enabled
                    else None
                ),
                phrase_recursive_grounding_prompt=prompt_service.industry_phrase_recursive_grounding_prompt,
                known_concepts=ontology.get_concepts_flat(ConceptTypeEnum.industries),
                llm_model=llm_model,
                model_params=model_params,
                created_at=created_at,
            ),
            ConceptTypeEnum.process_caps: ExtractionPipelineFactory.create_concept_extraction_pipeline(
                concept_type=ConceptTypeEnum.process_caps,
                chunk_strategy=chunk_strat_for(
                    ConceptTypeEnum.process_caps, PROCESS_CAP_CHUNKING_STRAT
                ),
                ontology=ontology,
                search_prompt=prompt_service.process_cap_phrase_search_prompt,
                recursive_search_prompt=prompt_service.process_cap_phrase_recursive_search_prompt,
                phrase_mention_collection_prompt=prompt_service.process_cap_phrase_mention_collection_prompt,
                phrase_synthesis_prompt=prompt_service.process_cap_phrase_synthesis_prompt,
                synthesis_include_location=synthesis_include_location,
                mention_collection_snippet_radius=mention_collection_snippet_radius,
                max_synthesis_entries_per_request=max_synthesis_entries_per_request,
                phrase_relationship_screening_prompt=prompt_service.process_cap_phrase_relationship_screening_prompt,
                phrase_initial_grounding_prompt=prompt_service.process_cap_phrase_initial_grounding_prompt,
                phrase_oov_grounding_prompt=(
                    prompt_service.process_cap_phrase_oov_grounding_prompt
                    if oov_grounding_enabled
                    else None
                ),
                phrase_recursive_grounding_prompt=prompt_service.process_cap_phrase_recursive_grounding_prompt,
                known_concepts=ontology.get_concepts_flat(ConceptTypeEnum.process_caps),
                llm_model=llm_model,
                model_params=model_params,
                created_at=created_at,
            ),
            ConceptTypeEnum.material_caps: ExtractionPipelineFactory.create_concept_extraction_pipeline(
                concept_type=ConceptTypeEnum.material_caps,
                chunk_strategy=chunk_strat_for(
                    ConceptTypeEnum.material_caps, MATERIAL_CAP_CHUNKING_STRAT
                ),
                ontology=ontology,
                search_prompt=prompt_service.material_cap_phrase_search_prompt,
                recursive_search_prompt=prompt_service.material_cap_phrase_recursive_search_prompt,
                phrase_mention_collection_prompt=prompt_service.material_cap_phrase_mention_collection_prompt,
                phrase_synthesis_prompt=prompt_service.material_cap_phrase_synthesis_prompt,
                synthesis_include_location=synthesis_include_location,
                mention_collection_snippet_radius=mention_collection_snippet_radius,
                max_synthesis_entries_per_request=max_synthesis_entries_per_request,
                phrase_relationship_screening_prompt=prompt_service.material_cap_phrase_relationship_screening_prompt,
                phrase_initial_grounding_prompt=prompt_service.material_cap_phrase_initial_grounding_prompt,
                phrase_oov_grounding_prompt=(
                    prompt_service.material_cap_phrase_oov_grounding_prompt
                    if oov_grounding_enabled
                    else None
                ),
                phrase_recursive_grounding_prompt=prompt_service.material_cap_phrase_recursive_grounding_prompt,
                known_concepts=ontology.get_concepts_flat(
                    ConceptTypeEnum.material_caps
                ),
                llm_model=llm_model,
                model_params=model_params,
                created_at=created_at,
            ),
        }
