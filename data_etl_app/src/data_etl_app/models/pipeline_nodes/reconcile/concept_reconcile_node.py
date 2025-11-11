import logging
from datetime import datetime
from typing import Optional

from core.models.db.manufacturer import Manufacturer
from core.services.manufacturer_service import update_manufacturer

from data_etl_app.models.types_and_enums import ConceptTypeEnum
from data_etl_app.services.llm_powered.extraction.extract_concept_deferred_service import (
    parse_llm_search_response,
)
from core.models.concept_extraction_results import (
    ConceptSearchChunkMap,
    ConceptSearchChunkStats,
    ConceptExtractionStats,
    ConceptExtractionResults,
)
from core.models.deferred_concept_extraction import (
    DeferredConceptExtraction,
)
from data_etl_app.models.pipeline_nodes.reconcile.reconcile_node import ReconcileNode
from data_etl_app.services.llm_powered.extraction.extract_concept_service import (
    get_matched_concepts_and_unmatched_keywords_by_concept_type,
)
from data_etl_app.services.llm_powered.map.map_known_to_unknown_service import (
    LLMMappingResult,
    get_mapped_known_concepts_and_unmapped_keywords_in_chunk_by_concept_type,
    get_mapped_known_concepts_and_unmapped_keywords_by_concept_type,
    parse_llm_concept_mapping_result,
)
from core.models.db.deferred_manufacturer import DeferredManufacturer
from core.services.gpt_batch_request_service import (
    find_completed_gpt_batch_requests_by_custom_ids,
    find_completed_gpt_batch_request_by_custom_id,
    bulk_delete_gpt_batch_requests_by_custom_ids,
)

logger = logging.getLogger(__name__)


class ConceptReconcileNode(ReconcileNode):
    field_type: ConceptTypeEnum  # Override the type annotation

    def __init__(self, concept_type: ConceptTypeEnum):
        super().__init__(field_type=concept_type)

    async def reconcile(
        self,
        mfg: Manufacturer,
        deferred_mfg: DeferredManufacturer,
        timestamp: datetime,
        force: bool = False,
    ) -> None:
        concept_data: Optional[ConceptExtractionResults] = getattr(
            mfg, self.field_type.name
        )
        if concept_data and not force:
            logger.info(
                f"Concept data already exists for {self.field_type.name}, skipping reconciliation for {mfg.etld1}."
            )
            return

        deferred_concept_extraction: Optional[DeferredConceptExtraction] = getattr(
            deferred_mfg, self.field_type.name
        )
        if not deferred_concept_extraction:
            raise ValueError(
                f"reconcile was called for {self.field_type.name} but no deferred concept extraction exists."
            )

        gpt_search_request_ids = [
            bundle.llm_search_request_id
            for bundle in deferred_concept_extraction.chunk_request_bundle_map.values()
        ]
        llm_search_requests_map = await find_completed_gpt_batch_requests_by_custom_ids(
            gpt_batch_request_custom_ids=gpt_search_request_ids
        )

        assert deferred_concept_extraction.llm_mapping_request_id is not None
        llm_map_request = await find_completed_gpt_batch_request_by_custom_id(
            gpt_batch_request_custom_id=deferred_concept_extraction.llm_mapping_request_id
        )
        assert llm_map_request is not None
        assert llm_map_request.response_blob is not None
        raw_gpt_mapping = parse_llm_concept_mapping_result(
            gpt_response=llm_map_request.response_blob.result
        )
        logger.debug(f"full raw_gpt_mapping: {raw_gpt_mapping}")

        final_results: set[str] = set()
        unmatched_keywords: set[str] = set()
        chunked_stats: ConceptSearchChunkMap = {}
        for (
            chunk_bounds,
            bundle,
        ) in deferred_concept_extraction.chunk_request_bundle_map.items():
            llm_search_req = llm_search_requests_map.get(bundle.llm_search_request_id)
            assert (
                llm_search_req is not None
            ), f"Missing GPTBatchRequest for {bundle.llm_search_request_id}"
            assert (  # should be ensured by find_completed_gpt_batch_requests_by_custom_ids
                llm_search_req.response_blob is not None
            ), f"Missing response_blob for {llm_search_req.request.custom_id}"

            llm_search_results_in_chunk = parse_llm_search_response(
                llm_search_req.response_blob.result
            )
            matched_concepts_in_chunk, unmatched_keywords_in_chunk = (
                await get_matched_concepts_and_unmatched_keywords_by_concept_type(
                    self.field_type, llm_search_results_in_chunk
                )
            )
            llm_mapping_chunk_result: LLMMappingResult = (
                await get_mapped_known_concepts_and_unmapped_keywords_in_chunk_by_concept_type(
                    mfg_etld1=mfg.etld1,
                    unmatched_keywords_in_chunk=unmatched_keywords_in_chunk,
                    concept_type=self.field_type,
                    raw_gpt_mapping=raw_gpt_mapping,
                )
            )
            logger.debug(
                f"llm_mapping_chunk_result for chunk {chunk_bounds}: {llm_mapping_chunk_result}"
            )
            chunked_stats[chunk_bounds] = ConceptSearchChunkStats(
                results=(
                    {c.name for c in matched_concepts_in_chunk}
                    | {
                        c.name
                        for c in llm_mapping_chunk_result["known_to_unknowns"].keys()
                    }
                ),
                brute=bundle.brute,
                llm=llm_search_results_in_chunk.copy(),
                mapping={
                    known.name: unknowns
                    for known, unknowns in llm_mapping_chunk_result[
                        "known_to_unknowns"
                    ].items()
                },
                unmapped_llm=llm_mapping_chunk_result["unmapped_unknowns"],
            )

            unmatched_keywords |= unmatched_keywords_in_chunk
            final_results |= chunked_stats[chunk_bounds].results

        llm_mapping_result: LLMMappingResult = (
            await get_mapped_known_concepts_and_unmapped_keywords_by_concept_type(
                mfg_etld1=mfg.etld1,
                unmatched_keywords=unmatched_keywords,
                concept_type=self.field_type,
                raw_gpt_mapping=raw_gpt_mapping,
            )
        )
        logger.debug(f"llm_mapping_result: {llm_mapping_result}")
        concept_data = ConceptExtractionResults(
            extracted_at=timestamp,
            results=list(final_results),
            stats=ConceptExtractionStats(
                extract_prompt_version_id=deferred_concept_extraction.extract_prompt_version_id,
                map_prompt_version_id=deferred_concept_extraction.map_prompt_version_id,
                ontology_version_id=deferred_concept_extraction.ontology_version_id,
                mapping={
                    known.name: unknowns
                    for known, unknowns in llm_mapping_result[
                        "known_to_unknowns"
                    ].items()
                },
                chunked_stats=chunked_stats,
                unmapped_llm=llm_mapping_result["unmapped_unknowns"],
            ),
        )

        setattr(mfg, self.field_type.name, concept_data)
        await update_manufacturer(updated_at=timestamp, manufacturer=mfg)
        logger.info(
            f"Reconciled concept data for {self.field_type.name} for manufacturer {mfg.etld1}. "
            f"Attempting to clean up GPTBatchRequests."
        )

        await bulk_delete_gpt_batch_requests_by_custom_ids(
            gpt_batch_request_custom_ids=[
                *gpt_search_request_ids,
                deferred_concept_extraction.llm_mapping_request_id,
            ],
            mfg_etld1=mfg.etld1,
        )
        logger.info(
            f"Cleaned up GPTBatchRequests for concept reconciliation of {self.field_type.name} for manufacturer {mfg.etld1}. "
            f"Attempting to clear deferred concept extraction."
        )

        # call super reconcile to clear deferred field
        await super().reconcile(
            mfg=mfg,
            deferred_mfg=deferred_mfg,
            timestamp=timestamp,
            force=force,
        )
