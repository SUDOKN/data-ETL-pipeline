import logging
from datetime import datetime
from typing import Optional

from core.models.db.manufacturer import Manufacturer
from core.models.deferred_search_requests import DeferredSearchRequests
from core.models.keyword_extraction_results import (
    KeywordExtractionStats,
    KeywordExtractionResults,
    KeywordExtractionStats,
    KeywordExtractionStatsMap,
)
from core.models.db.deferred_manufacturer import DeferredManufacturer
from data_etl_app.models.pipeline_nodes.keyword.keyword_search_node import (
    KeywordSearchNode,
)
from data_etl_app.models.types_and_enums import KeywordTypeEnum, PipelineContext
from data_etl_app.models.pipeline_nodes.reconcile_node import ReconcileNode
from scraper_app.models.scraped_text_file import ScrapedTextFile

from core.services.manufacturer_service import update_manufacturer
from core.services.gpt_batch_request_writes import (
    bulk_delete_gpt_batch_requests_by_custom_ids,
)

logger = logging.getLogger(__name__)


class KeywordReconcileNode(ReconcileNode[KeywordTypeEnum]):
    def __init__(self, field_type: KeywordTypeEnum) -> None:
        super().__init__(field_type=field_type)

    async def execute(
        self,
        mfg: Manufacturer,
        deferred_mfg: DeferredManufacturer,
        scraped_text_file: ScrapedTextFile,
        timestamp: datetime,
        pipeline_context: PipelineContext,
        eager: bool,
    ) -> None:
        extraction_requests: Optional[DeferredSearchRequests] = getattr(
            deferred_mfg, self.field_type.name
        )
        if not extraction_requests:
            raise ValueError(
                f"execute was called for {self.field_type.name} but no deferred extraction requests exist."
            )

        completed_extraction_requests = pipeline_context[KeywordSearchNode]
        all_keywords: set[str] = set()
        chunk_stats: KeywordExtractionStatsMap = {}
        for (
            chunk_bounds,
            bundle,
        ) in extraction_requests.request_map.items():
            keyword_extraction_results = (
                await KeywordSearchNode.parse_batch_request_result(
                    mfg_etld1=deferred_mfg.mfg_etld1,
                    field_type=self.field_type,
                    chunk_bounds=chunk_bounds,
                    extraction_bundle=bundle,
                    completed_request_map=completed_extraction_requests,
                    deferred_at=timestamp,
                )
            )
            chunk_stats[chunk_bounds] = KeywordExtractionStats(
                results=keyword_extraction_results,
                llm_search=keyword_extraction_results,
            )
            all_keywords.update(keyword_extraction_results)

        final_extraction_result = KeywordExtractionResults(
            metadata=extraction_requests.metadata,
            results=all_keywords,
            chunk_stats=chunk_stats,
        )

        setattr(mfg, self.field_type.name, final_extraction_result)
        await update_manufacturer(updated_at=timestamp, manufacturer=mfg)

        # call super wipe_down to clear deferred field and completed GPT requests from pipeline context
        await self.wipe_down(
            deferred_mfg=deferred_mfg, pipeline_context=pipeline_context
        )

    async def wipe_down(
        self,
        deferred_mfg: DeferredManufacturer,
        pipeline_context: PipelineContext,
    ) -> None:
        logger.info(
            f"Reconciled keyword {self.field_type.name} data for manufacturer {deferred_mfg.etld1}. "
            f"Attempting to clean up GPTBatchRequests."
        )
        completed_extraction_requests = pipeline_context[KeywordSearchNode]
        await bulk_delete_gpt_batch_requests_by_custom_ids(
            gpt_batch_request_custom_ids=list(completed_extraction_requests.keys()),
            mfg_etld1=deferred_mfg.etld1,
        )
        logger.info(
            f"Cleaned up GPTBatchRequests for keyword {self.field_type.name} for manufacturer {deferred_mfg.etld1}. "
            f"Attempting to clear deferred_mfg keyword extraction field."
        )
        await super().wipe_down(
            deferred_mfg=deferred_mfg, pipeline_context=pipeline_context
        )
