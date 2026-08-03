from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING, Optional

from data_etl_app.models.db.manufacturer import Manufacturer
from data_etl_app.models.extraction_results.business_description_extraction_result import (
    BusinessDescriptionExtractionStats,
    BusinessDescriptionExtractionStatsMap,
    BusinessDescriptionExtractionResult,
)
from core.models.deferred_extraction.deferred_single_stage_extraction_requests import (
    DeferredSingleStageExtractionRequests,
)
from data_etl_app.models.db.deferred_manufacturer import DeferredManufacturer
from data_etl_app.models.pipeline_nodes.base.base_llm_extraction_node import (
    PipelineContext,
)
from core.models.types_and_enums import BasicFieldTypeEnum
from data_etl_app.models.pipeline_nodes.base.base_reconcile_node import ReconcileNode
from scraper_app.models.scraped_text_file import ScrapedTextFile

from data_etl_app.services.manufacturer_service import update_manufacturer

# if TYPE_CHECKING:
#     from data_etl_app.models.pipeline_nodes.single_stage.basic_fields.business_desc_extraction_node import (
#         BusinessDescExtractionNode,
#     )

logger = logging.getLogger(__name__)


class BusinessDescReconcileNode(ReconcileNode[BasicFieldTypeEnum.business_desc]):
    def __init__(
        self,
    ) -> None:
        super().__init__(field_type=BasicFieldTypeEnum.business_desc)

    async def execute(
        self,
        mfg: Manufacturer,
        deferred_mfg: DeferredManufacturer,
        scraped_text_file: ScrapedTextFile,
        timestamp: datetime,
        pipeline_context: PipelineContext,
        eager: bool,
    ) -> None:
        from data_etl_app.models.pipeline_nodes.single_stage.basic_fields.business_desc_extraction_node import (
            BusinessDescExtractionNode,
        )

        extraction_requests: Optional[DeferredSingleStageExtractionRequests] = getattr(
            deferred_mfg, self.field_type.name
        )
        if not extraction_requests:
            raise ValueError(
                f"execute was called for {self.field_type.name} but no deferred extraction requests exist."
            )

        completed_extraction_requests = pipeline_context[BusinessDescExtractionNode]
        first_chunk_bounds, first_req_bundle = list(
            extraction_requests.chunked_request_map.items()
        )[0]
        result = await BusinessDescExtractionNode.get_result(
            mfg_etld1=deferred_mfg.etld1,
            field_type=self.field_type,
            chunk_bounds=first_chunk_bounds,
            extraction_bundle=first_req_bundle,
            completed_request_map=completed_extraction_requests,
            timestamp=timestamp,
        )
        chunk_stats: BusinessDescriptionExtractionStatsMap = {
            first_chunk_bounds: BusinessDescriptionExtractionStats(result=result)
        }
        final_result = BusinessDescriptionExtractionResult(
            metadata=extraction_requests.metadata,
            result=result,
            chunk_stats=chunk_stats,
        )
        setattr(mfg, self.field_type.name, final_result)
        await update_manufacturer(updated_at=timestamp, manufacturer=mfg)

        # call super wipe_down to clear deferred field and completed GPT requests from pipeline context
        await super().wipe_down(
            deferred_mfg=deferred_mfg,
            associated_batch_request_custom_ids=list(
                completed_extraction_requests.keys()
            ),
        )
