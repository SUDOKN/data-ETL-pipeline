from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

from core.models.extraction_subject import (
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
from core.models.pipeline_nodes.base.base_node import PipelineContext
from core.models.field_types import (
    ExtractionFieldType,
)
from core.models.pipeline_nodes.base.base_reconcile_node import (
    ReconcileNode,
)
from core.models.pipeline_nodes.single_stage.classification.binary_classification_node import (
    BinaryClassificationNode,
)
from core.utils.extraction_dump_util import (
    build_run_provenance,
    write_extraction_dump,
)
from scraper.models.s3.scraped_text_file import ScrapedTextFile

logger = logging.getLogger(__name__)


class BinaryReconcileNode(ReconcileNode[ExtractionFieldType]):

    def __init__(self, binary_field_type: ExtractionFieldType) -> None:
        super().__init__(field_type=binary_field_type)

    async def execute(
        self,
        subject: AbstractExtractionSubject,
        deferred_subject: AbstractDeferredExtractionSubject,
        scraped_text_file: ScrapedTextFile,
        timestamp: datetime,
        pipeline_context: PipelineContext,
        eager: bool,
    ) -> None:
        if await self.stop_if_stage_disabled(
            subject=subject,
            deferred_subject=deferred_subject,
            scraped_text_file=scraped_text_file,
            timestamp=timestamp,
            pipeline_context=pipeline_context,
        ):
            return

        extraction_requests: Optional[DeferredSingleStageExtractionRequests] = getattr(
            deferred_subject, self.field_type.name
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
            subject_unique_id=deferred_subject.subject_unique_id,
            field_type=self.field_type,
            chunk_bounds=first_chunk_bounds,
            extraction_bundle=first_req_bundle,
            completed_request_map=completed_classification_requests,
            timestamp=timestamp,
        )
        chunk_stats: BinaryClassificationStatsMap = {
            first_chunk_bounds: BinaryClassificationStats(result=result)
        }

        # Only the first chunk is dumped, because only the first chunk is
        # reconciled — this node reads exactly one.
        write_extraction_dump(
            subject_unique_id=deferred_subject.subject_unique_id,
            field_type=self.field_type,
            timestamp=timestamp,
            chunked_contents={
                first_chunk_bounds: {"result": result.model_dump(mode="json")}
            },
            chunked_request_map=extraction_requests.chunked_request_map,
            completed_requests=completed_classification_requests,
            run_provenance=build_run_provenance(
                metadata=extraction_requests.metadata,
                scraped_text_file=scraped_text_file,
                partial=False,
            ),
        )

        classification_result = BinaryClassificationResult(
            metadata=extraction_requests.metadata,
            result=result,
            chunk_stats=chunk_stats,
        )

        setattr(subject, self.field_type.name, classification_result)
        await subject.record_update(updated_at=timestamp)

        # call super wipe_down to clear deferred field and completed GPT requests from pipeline context
        await super().wipe_down(
            deferred_subject=deferred_subject,
            associated_batch_request_custom_ids=list(
                completed_classification_requests.keys()
            ),
        )
