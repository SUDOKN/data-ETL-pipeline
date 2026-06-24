import logging
from datetime import datetime

from core.models.concept_extraction_results import ConceptExtractionMetadata
from core.models.db.manufacturer import Manufacturer
from core.models.db.deferred_manufacturer import DeferredManufacturer
from core.models.deferred_concept_extraction import (
    ConceptExtractionRequestBundle,
    DeferredConceptExtractionRequests,
)
from core.models.prompt import Prompt
from data_etl_app.models.pipeline_nodes.concept.concept_distillation_node import (
    ConceptDistillationNode,
)
from data_etl_app.models.pipeline_nodes.concept.concept_mapping_node import (
    ConceptMappingNode,
)
from data_etl_app.models.pipeline_nodes.concept.concept_search_node import (
    ConceptSearchNode,
)
from data_etl_app.models.pipeline_nodes.prefill_node import PrefillNode
from data_etl_app.models.skos_concept import Concept
from data_etl_app.models.types_and_enums import ConceptTypeEnum
from data_etl_app.models.pipeline_nodes.base_node import PipelineContext
from data_etl_app.models.chunking_strat import ChunkingStrategy
from litellm_proxy_app.models.llm_model import LLM_Model
from open_ai_key_app.models.gpt_model_params import GPTModelParams
from scraper_app.models.scraped_text_file import ScrapedTextFile

from data_etl_app.services.brute_search_service import brute_search

from data_etl_app.utils.chunk_util import get_chunks_respecting_line_boundaries

logger = logging.getLogger(__name__)


class ConceptExtractionPrefillNode(PrefillNode[ConceptTypeEnum]):
    next_node: ConceptSearchNode

    def __init__(
        self,
        field_type: ConceptTypeEnum,
        chunk_strategy: ChunkingStrategy,
        search_prompt: Prompt,
        distillation_prompt: Prompt,
        mapping_prompt: Prompt,
        ontology_version_id: str,
        known_concepts: set[Concept],
        next_node: ConceptSearchNode,
    ):
        super().__init__(
            field_type=field_type,
            chunk_strategy=chunk_strategy,
            next_node=next_node,
        )
        self.search_prompt: Prompt = search_prompt
        self.distillation_prompt: Prompt = distillation_prompt
        self.mapping_prompt: Prompt = mapping_prompt
        self.ontology_version_id: str = ontology_version_id
        self.known_concepts: set[Concept] = known_concepts

    async def execute(
        self,
        mfg: Manufacturer,
        deferred_mfg: DeferredManufacturer,
        scraped_text_file: ScrapedTextFile,
        timestamp: datetime,
        pipeline_context: PipelineContext,
        llm_model: LLM_Model,
        model_params: GPTModelParams,
        eager: bool,
    ):
        if not bool(getattr(deferred_mfg, self.field_type.name)):
            chunk_map = await get_chunks_respecting_line_boundaries(
                text=scraped_text_file.text,
                soft_limit_tokens=self.chunk_strategy.max_tokens_per_chunk,
                overlap_ratio=self.chunk_strategy.overlap,
                max_chunks=self.chunk_strategy.max_chunks,
                llm_model=llm_model,
            )

            deferred_concept_extraction = DeferredConceptExtractionRequests(
                metadata=ConceptExtractionMetadata(
                    model=llm_model.model_name,
                    model_params=model_params,
                    created_at=timestamp,
                    chunk_strat=self.chunk_strategy,
                    search_prompt_version_id=self.search_prompt.s3_version_id,
                    distillation_prompt_version_id=self.distillation_prompt.s3_version_id,
                    mapping_prompt_version_id=self.mapping_prompt.s3_version_id,
                    ontology_version_id=self.ontology_version_id,
                ),
                request_map={
                    chunk_bounds: ConceptExtractionRequestBundle(
                        brute={
                            label
                            for label in brute_search(chunk_text, self.known_concepts)
                        },
                        llm_search_request_id=ConceptSearchNode.get_request_custom_id(
                            mfg_etld1=deferred_mfg.etld1,
                            field_type=self.field_type,
                            chunk_bounds=chunk_bounds,
                            llm_model=llm_model,
                            model_params=model_params,
                        ),
                        llm_distillation_request_id=ConceptDistillationNode.get_request_custom_id(
                            mfg_etld1=deferred_mfg.etld1,
                            field_type=self.field_type,
                            chunk_bounds=chunk_bounds,
                            llm_model=llm_model,
                            model_params=model_params,
                        ),
                        llm_mapping_request_id=ConceptMappingNode.get_request_custom_id(
                            mfg_etld1=deferred_mfg.etld1,
                            field_type=self.field_type,
                            chunk_bounds=chunk_bounds,
                            llm_model=llm_model,
                            model_params=model_params,
                        ),
                    )
                    for chunk_bounds, chunk_text in chunk_map.items()
                },
            )
            setattr(deferred_mfg, self.field_type.name, deferred_concept_extraction)
            await deferred_mfg.save()

        await self.next_node.execute(
            mfg=mfg,
            deferred_mfg=deferred_mfg,
            scraped_text_file=scraped_text_file,
            timestamp=timestamp,
            pipeline_context=pipeline_context,
            llm_model=llm_model,
            model_params=model_params,
            eager=eager,
        )
