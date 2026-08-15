from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

from data_etl_app.models.extraction_results.address_extraction_result import (
    Address,
    AddressExtractionStats,
    AddressExtractionStatsMap,
    AddressExtractionResult,
)
from core.models.extraction_subject import (
    AbstractExtractionSubject,
    AbstractDeferredExtractionSubject,
)
from core.models.deferred_extraction.deferred_single_stage_extraction_requests import (
    DeferredSingleStageExtractionRequests,
)
from data_etl_app.models.types_and_enums import (
    BasicFieldTypeEnum,
)
from core.models.pipeline_nodes import PipelineContext
from data_etl_app.models.pipeline_nodes.single_stage.basic_fields.address_extraction_node import (
    AddressExtractionNode,
)
from core.models.pipeline_nodes.base.base_reconcile_node import (
    ReconcileNode,
)
from core.models.field_types import ExtractionFieldType
from core.utils.extraction_dump_util import (
    build_run_provenance,
    jsonable_result,
    write_extraction_dump,
)
from scraper.models.s3.scraped_text_file import ScrapedTextFile

logger = logging.getLogger(__name__)


class AddressReconcileNode(ReconcileNode[ExtractionFieldType]):
    def __init__(self) -> None:
        super().__init__(field_type=BasicFieldTypeEnum.addresses)

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
                f"reconcile was called for {self.field_type.name} but no deferred extraction requests exist."
            )

        completed_extraction_requests = pipeline_context[AddressExtractionNode]
        all_addresses: list[Address] = []
        chunk_stats: AddressExtractionStatsMap = {}
        chunked_dump_contents: dict[str, dict[str, object]] = {}
        for (
            chunk_bounds,
            bundle,
        ) in extraction_requests.chunked_request_map.items():
            address_extraction_results = await AddressExtractionNode.get_result(
                subject_unique_id=deferred_subject.subject_unique_id,
                field_type=self.field_type,
                chunk_bounds=chunk_bounds,
                extraction_bundle=bundle,
                completed_request_map=completed_extraction_requests,
                timestamp=timestamp,
            )
            chunk_stats[chunk_bounds] = AddressExtractionStats(
                result=address_extraction_results
            )
            chunked_dump_contents[chunk_bounds] = {
                "result": jsonable_result(address_extraction_results)
            }
            all_addresses.extend(address_extraction_results)

        write_extraction_dump(
            subject_unique_id=deferred_subject.subject_unique_id,
            field_type=self.field_type,
            timestamp=timestamp,
            chunked_contents=chunked_dump_contents,
            chunked_request_map=extraction_requests.chunked_request_map,
            completed_requests=completed_extraction_requests,
            run_provenance=build_run_provenance(
                metadata=extraction_requests.metadata,
                scraped_text_file=scraped_text_file,
                partial=False,
            ),
        )

        final_extraction_result = AddressExtractionResult(
            metadata=extraction_requests.metadata,
            result=all_addresses,
            chunk_stats=chunk_stats,
        )

        setattr(subject, self.field_type.name, final_extraction_result)
        await subject.record_update(updated_at=timestamp)

        # call super wipe_down to clear deferred field and completed GPT requests from pipeline context
        await super().wipe_down(
            deferred_subject=deferred_subject,
            associated_batch_request_custom_ids=list(
                completed_extraction_requests.keys()
            ),
        )
