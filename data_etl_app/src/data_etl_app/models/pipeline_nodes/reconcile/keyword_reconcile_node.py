import logging
from datetime import datetime
import traceback
from typing import Optional

from core.models.db.manufacturer import Manufacturer
from core.services.manufacturer_service import update_manufacturer

from core.models.deferred_keyword_extraction import DeferredKeywordExtraction
from core.models.keyword_extraction_results import (
    KeywordExtractionChunkStats,
    KeywordExtractionResults,
    KeywordExtractionStats,
    KeywordSearchChunkMap,
)
from data_etl_app.models.types_and_enums import KeywordTypeEnum
from data_etl_app.services.llm_powered.search.llm_search_service import (
    parse_llm_search_response,
)
from core.models.db.deferred_manufacturer import DeferredManufacturer
from data_etl_app.models.pipeline_nodes.reconcile.reconcile_node import ReconcileNode
from core.services.gpt_batch_request_service import (
    find_completed_gpt_batch_requests_by_custom_ids,
    bulk_delete_gpt_batch_requests_by_custom_ids,
    record_response_parse_error,
)

logger = logging.getLogger(__name__)


class KeywordReconcileNode(ReconcileNode):
    field_type: KeywordTypeEnum

    def __init__(
        self,
        field_type: KeywordTypeEnum,
    ) -> None:
        super().__init__(field_type)

    async def reconcile(
        self,
        mfg: Manufacturer,
        deferred_mfg: DeferredManufacturer,
        timestamp: datetime,
        force: bool = False,
    ) -> None:
        logger.info(
            f"Starting keyword reconciliation for field {self.field_type.name} for manufacturer {mfg.etld1}."
        )
        keyword_data: Optional[KeywordExtractionResults] = getattr(
            mfg, self.field_type.name
        )
        if keyword_data and not force:
            logger.info(
                f"Keyword data already exists for {self.field_type.name}, skipping reconciliation for {mfg.etld1}."
            )
            return

        deferred_keyword_extraction: Optional[DeferredKeywordExtraction] = getattr(
            deferred_mfg, self.field_type.name
        )
        if not deferred_keyword_extraction:
            raise ValueError(
                f"reconcile was called for keyword extraction but no deferred keyword extraction exists."
            )

        gpt_search_request_ids = [
            llm_search_request_id
            for llm_search_request_id in deferred_keyword_extraction.chunk_request_id_map.values()
        ]
        llm_search_requests_map = await find_completed_gpt_batch_requests_by_custom_ids(
            gpt_batch_request_custom_ids=gpt_search_request_ids
        )

        final_results: set[str] = set()
        chunked_stats: KeywordSearchChunkMap = {}
        for (
            chunk_bounds,
            llm_search_request_id,
        ) in deferred_keyword_extraction.chunk_request_id_map.items():
            llm_search_req = llm_search_requests_map.get(llm_search_request_id)
            assert (
                llm_search_req is not None
            ), f"Missing GPTBatchRequest for {llm_search_request_id}"
            assert (  # should be ensured by find_completed_gpt_batch_requests_by_custom_ids
                llm_search_req.response_blob is not None
            ), f"Missing response_blob for {llm_search_req.request.custom_id}"
            try:
                llm_search_results_in_chunk = parse_llm_search_response(
                    llm_search_req.response_blob.result
                )
            except Exception as e:
                await record_response_parse_error(
                    gpt_batch_request=llm_search_req,
                    error_message=str(e),
                    timestamp=timestamp,
                    traceback_str=traceback.format_exc(),
                )
                logger.error(
                    f"Error parsing keyword search results for manufacturer {mfg.etld1} from GPT response: {e}"
                )
                raise

            chunked_stats[chunk_bounds] = KeywordExtractionChunkStats(
                results=llm_search_results_in_chunk
            )

            final_results |= llm_search_results_in_chunk

        keyword_data = KeywordExtractionResults(
            extracted_at=timestamp,
            results=final_results,
            stats=KeywordExtractionStats(
                extract_prompt_version_id=deferred_keyword_extraction.extract_prompt_version_id,
                chunked_stats=chunked_stats,
            ),
        )
        setattr(mfg, self.field_type.name, keyword_data)

        await update_manufacturer(updated_at=timestamp, manufacturer=mfg)

        await bulk_delete_gpt_batch_requests_by_custom_ids(
            gpt_batch_request_custom_ids=gpt_search_request_ids,
            mfg_etld1=mfg.etld1,
        )

        # call super reconcile to clear deferred field
        await super().reconcile(
            mfg=mfg,
            deferred_mfg=deferred_mfg,
            timestamp=timestamp,
            force=force,
        )
