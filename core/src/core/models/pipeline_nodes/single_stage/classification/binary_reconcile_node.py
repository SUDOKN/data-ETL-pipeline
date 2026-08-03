from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

from core.models.base.extraction_subject import (
    AbstractExtractionSubject,
    AbstractDeferredExtractionSubject,
)
from core.models.extraction_results.binary_classification_result import (
    BinaryClassificationResult,
    BinaryClassificationStats,
    BinaryClassificationStatsMap,
)
from core.models.deferred_extraction.deferred_single_stage_extraction_requests import (
    DeferredSingleStageExtractionRequests,
)
from data_etl_app.models.pipeline_nodes.base.base_node import PipelineContext
from core.models.types_and_enums import (
    BinaryClassificationTypeEnum,
)
from data_etl_app.models.pipeline_nodes.base.base_reconcile_node import ReconcileNode
from data_etl_app.models.pipeline_nodes.single_stage.classification.binary_classification_node import (
    BinaryClassificationNode,
)
from scraper_app.models.scraped_text_file import ScrapedTextFile

logger = logging.getLogger(__name__)


class BinaryReconcileNode(ReconcileNode[BinaryClassificationTypeEnum]):

    def __init__(self, binary_field_type: BinaryClassificationTypeEnum) -> None:
        super().__init__(field_type=binary_field_type)

    async def execute(
        self,
        mfg: AbstractExtractionSubject,
        deferred_mfg: AbstractDeferredExtractionSubject,
        scraped_text_file: ScrapedTextFile,
        timestamp: datetime,
        pipeline_context: PipelineContext,
        eager: bool,
    ) -> None:

        extraction_requests: Optional[DeferredSingleStageExtractionRequests] = getattr(
            deferred_mfg, self.field_type.name
        )
        if not extraction_requests:
            raise ValueError(
                f"reconcile/execute was called for {self.field_type.name} but no deferred extraction requests exist."
            )

        completed_classification_requests = pipeline_context[BinaryClassificationNode]
        first_chunk_bounds, first_req_bundle = list(
            extraction_requests.chunked_request_map.items()
        )[0]
        result = await BinaryClassificationNode.get_result(
            subject_unique_id=deferred_mfg.subject_unique_id,
            field_type=self.field_type,
            chunk_bounds=first_chunk_bounds,
            extraction_bundle=first_req_bundle,
            completed_request_map=completed_classification_requests,
            timestamp=timestamp,
        )
        chunk_stats: BinaryClassificationStatsMap = {
            first_chunk_bounds: BinaryClassificationStats(result=result)
        }

        classification_result = BinaryClassificationResult(
            metadata=extraction_requests.metadata,
            result=result,
            chunk_stats=chunk_stats,
        )

        setattr(mfg, self.field_type.name, classification_result)
        await mfg.record_update(updated_at=timestamp)

        # call super wipe_down to clear deferred field and completed GPT requests from pipeline context
        await super().wipe_down(
            deferred_mfg=deferred_mfg,
            associated_batch_request_custom_ids=list(
                completed_classification_requests.keys()
            ),
        )
