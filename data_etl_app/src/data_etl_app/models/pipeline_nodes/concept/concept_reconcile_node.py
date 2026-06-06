import logging
from datetime import datetime
from typing import Optional

from core.models.db.manufacturer import Manufacturer
from core.models.concept_extraction_results import (
    ConceptExtractionStatsMap,
    ConceptExtractionStats,
    ConceptExtractionResults,
)
from core.models.db.deferred_manufacturer import DeferredManufacturer
from core.models.deferred_concept_extraction import (
    DeferredConceptExtractionRequests,
)
from data_etl_app.models.types_and_enums import ConceptTypeEnum
from data_etl_app.models.pipeline_nodes.base_node import PipelineContext
from data_etl_app.models.pipeline_nodes.concept.concept_search_node import (
    ConceptSearchNode,
)
from data_etl_app.models.pipeline_nodes.concept.concept_evidence_node import (
    ConceptEvidenceNode,
)
from data_etl_app.models.pipeline_nodes.concept.concept_mapping_node import (
    ConceptMappingNode,
)
from data_etl_app.models.pipeline_nodes.reconcile_node import ReconcileNode
from data_etl_app.models.skos_concept import Concept
from litellm_proxy_app.models.llm_model import LLM_Model
from open_ai_key_app.models.gpt_model_params import GPTModelParams
from scraper_app.models.scraped_text_file import ScrapedTextFile

from core.services.manufacturer_service import update_manufacturer

from data_etl_app.utils.ground_truth_helper_util import (
    get_verified_evidence_phrases_from_raw_evidence_results,
)
from data_etl_app.utils.llm_mapping_helper import (
    UnknownToKnownMap,
    get_matched_concepts_and_unmatched_keywords,
    get_mapped_known_concepts_and_unmapped_keywords,
    get_verified_results_from_concept_mapping,
)

logger = logging.getLogger(__name__)


class ConceptReconcileNode(ReconcileNode[ConceptTypeEnum]):

    def __init__(self, concept_type: ConceptTypeEnum, known_concepts: set[Concept]):
        super().__init__(field_type=concept_type)
        self.known_concepts = known_concepts

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
    ) -> None:
        # concept_data: Optional[ConceptExtractionResults] = getattr(
        #     mfg, self.field_type.name
        # )
        # if concept_data and not force:
        #     logger.info(
        #         f"Concept data already exists for {self.field_type.name}, skipping reconciliation for {mfg.etld1}."
        #     )
        #     return

        extraction_requests: Optional[DeferredConceptExtractionRequests] = getattr(
            deferred_mfg, self.field_type.name
        )
        if not extraction_requests:
            raise ValueError(
                f"reconcile was called for {self.field_type.name} but no deferred concept extraction exists."
            )

        completed_search_requests = pipeline_context[ConceptSearchNode]
        completed_evidence_requests = pipeline_context[ConceptEvidenceNode]
        completed_mapping_requests = pipeline_context[ConceptMappingNode]

        all_results: set[str] = set()
        chunk_stats: ConceptExtractionStatsMap = {}
        for (
            chunk_bounds,
            bundle,
        ) in extraction_requests.request_map.items():
            llm_search_results = await ConceptSearchNode.parse_batch_request_result(
                mfg_etld1=deferred_mfg.etld1,
                field_type=self.field_type,
                chunk_bounds=chunk_bounds,
                extraction_bundle=bundle,
                completed_request_map=completed_search_requests,
                deferred_at=timestamp,
            )

            llm_evidence_results = await ConceptEvidenceNode.parse_batch_request_result(
                mfg_etld1=deferred_mfg.etld1,
                field_type=self.field_type,
                chunk_bounds=chunk_bounds,
                extraction_bundle=bundle,
                completed_request_map=completed_evidence_requests,
                deferred_at=timestamp,
            )

            confirmed_keywords_w_evidence = (
                get_verified_evidence_phrases_from_raw_evidence_results(
                    llm_evidence_results=llm_evidence_results
                )
            )

            (
                matched_concepts,
                unmatched_keywords_w_evidence,
            ) = get_matched_concepts_and_unmatched_keywords(
                self.known_concepts, confirmed_keywords_w_evidence
            )

            llm_mapping_raw = await ConceptMappingNode.parse_batch_request_result(
                mfg_etld1=deferred_mfg.etld1,
                field_type=self.field_type,
                chunk_bounds=chunk_bounds,
                extraction_bundle=bundle,
                completed_request_map=completed_mapping_requests,
                deferred_at=timestamp,
            )

            llm_mapping_result: UnknownToKnownMap = (
                get_mapped_known_concepts_and_unmapped_keywords(
                    mfg_etld1=deferred_mfg.etld1,
                    concept_type=self.field_type,
                    known_concepts=self.known_concepts,
                    unmatched_keywords_w_evidence=unmatched_keywords_w_evidence,
                    raw_gpt_mapping=llm_mapping_raw,
                )
            )

            chunk_stats[chunk_bounds] = ConceptExtractionStats(
                results=(
                    {c.name for c in matched_concepts}
                    | get_verified_results_from_concept_mapping(
                        llm_mapping_result["unknown_to_knowns"]
                    )
                ),
                brute_search=bundle.brute,
                llm_search=llm_search_results.copy(),
                llm_evidence=llm_evidence_results.copy(),
                llm_mapping={
                    mu: {mk.name: reason for mk, reason in mk_dict.items()}
                    for mu, mk_dict in llm_mapping_result["unknown_to_knowns"].items()
                },
                unmapped=llm_mapping_result["unmapped_unknowns"],
            )

            all_results |= chunk_stats[chunk_bounds].results

        logger.debug(f"llm_mapping_result: {llm_mapping_result}")
        final_extraction_result = ConceptExtractionResults(
            metadata=extraction_requests.metadata,
            results=all_results,
            chunk_stats=chunk_stats,
        )

        setattr(mfg, self.field_type.name, final_extraction_result)
        await update_manufacturer(updated_at=timestamp, manufacturer=mfg)

        # call super wipe_down to clear deferred field and completed GPT requests from pipeline context
        await super().wipe_down(
            deferred_mfg=deferred_mfg,
            associated_batch_request_custom_ids=list(
                [
                    *completed_search_requests.keys(),
                    *completed_evidence_requests.keys(),
                    *completed_mapping_requests.keys(),
                ]
            ),
        )
