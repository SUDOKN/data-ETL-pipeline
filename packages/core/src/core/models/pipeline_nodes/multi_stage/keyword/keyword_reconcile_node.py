from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

from core.models.extraction_subject import (
    AbstractExtractionSubject,
    AbstractDeferredExtractionSubject,
)
from core.models.deferred_extraction.deferred_keyword_extraction import (
    DeferredKeywordExtractionRequests,
)
from core.models.extraction_results.keyword_extraction_results import (
    KeywordExtractionStats,
    KeywordExtractionResults,
    KeywordExtractionStatsMap,
)
from core.models.extraction_results.llm_phrase_extraction_results import (
    partition_by_search_round,
)
from core.models.extraction_schemas.grounding import (
    is_sentinel_grounding_label,
)
from core.models.field_types import ExtractionFieldType
from core.models.pipeline_nodes.base.base_node import PipelineContext
from core.models.pipeline_nodes.base.base_reconcile_node import (
    ReconcileNode,
)
from scraper.models.s3.scraped_text_file import ScrapedTextFile

# if TYPE_CHECKING:

from core.services.pipeline_nodes.multi_stage.llm_phrase_recursive_search_node_service import (
    build_llm_phrase_search_results,
)
from core.services.pipeline_nodes.multi_stage.llm_phrase_relationship_node_service import (
    get_phrase_relationship_result as get_phrase_relationship_result,
)
from core.services.pipeline_nodes.multi_stage.llm_relationship_screening_node_service import (
    get_phrase_relationship_screening_result as parse_relationship_screening_batch_req_result,
)
from core.services.pipeline_nodes.multi_stage.llm_freehand_grounding_service import (
    get_freehand_grounding_result as parse_freehand_grounding_batch_req_result,
)
from core.utils.label_dedupe_util import dedupe_equivalent_keywords
from core.utils.phrase_trail_dump_util import (
    build_keyword_phrase_rows,
    merge_stage_repairs,
    write_phrase_trails_dump,
)

logger = logging.getLogger(__name__)


class KeywordReconcileNode(ReconcileNode[ExtractionFieldType]):
    """Base class: phase 6, aggregate & write final results.

    This is a BASE class: the 5 ``get_upstream_*_map`` getters are left
    unimplemented here. Concrete leaves such as ``PureProductReconcileNode`` /
    ``ContractProductReconcileNode`` must implement them, pointing at their own
    sibling node classes.
    """

    def __init__(self, field_type: ExtractionFieldType) -> None:
        super().__init__(field_type=field_type)

    def get_upstream_search_map(self, pipeline_context: PipelineContext) -> dict:
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement get_upstream_search_map"
        )

    def get_upstream_recursive_search_map(
        self, pipeline_context: PipelineContext
    ) -> dict:
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement get_upstream_recursive_search_map"
        )

    def get_upstream_relationship_map(self, pipeline_context: PipelineContext) -> dict:
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement get_upstream_relationship_map"
        )

    def get_upstream_screening_map(self, pipeline_context: PipelineContext) -> dict:
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement get_upstream_screening_map"
        )

    def get_upstream_freehand_grounding_map(
        self, pipeline_context: PipelineContext
    ) -> dict:
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement get_upstream_freehand_grounding_map"
        )

    async def execute(
        self,
        subject: AbstractExtractionSubject,
        deferred_subject: AbstractDeferredExtractionSubject,
        scraped_text_file: ScrapedTextFile,
        timestamp: datetime,
        pipeline_context: PipelineContext,
        eager: bool,
    ) -> None:
        extraction_requests: Optional[DeferredKeywordExtractionRequests] = getattr(
            deferred_subject, self.field_type.name
        )
        if not extraction_requests:
            raise ValueError(
                f"execute was called for {self.field_type.name} but no deferred extraction requests exist."
            )

        completed_search_requests = self.get_upstream_search_map(pipeline_context)
        completed_recursive_search_requests = self.get_upstream_recursive_search_map(
            pipeline_context
        )
        completed_phrase_relationship_requests = self.get_upstream_relationship_map(
            pipeline_context
        )
        completed_relationship_screening_requests = self.get_upstream_screening_map(
            pipeline_context
        )
        completed_freehand_grounding_requests = (
            self.get_upstream_freehand_grounding_map(pipeline_context)
        )
        all_keywords: set[str] = set()
        chunk_stats: KeywordExtractionStatsMap = {}
        chunked_phrase_trails_dump: dict[str, list[dict[str, object]]] = {}
        for (
            chunk_bounds,
            bundle,
        ) in extraction_requests.chunked_request_map.items():
            llm_search_results = await build_llm_phrase_search_results(
                subject_unique_id=deferred_subject.subject_unique_id,
                field_type=self.field_type,
                chunk_bounds=chunk_bounds,
                extraction_bundle=bundle,
                completed_search_req_map=completed_search_requests,
                completed_recursive_search_req_map=completed_recursive_search_requests,
                timestamp=timestamp,
            )

            llm_phrase_relationship_flat = await get_phrase_relationship_result(
                subject_unique_id=deferred_subject.subject_unique_id,
                field_type=self.field_type,
                chunk_bounds=chunk_bounds,
                extraction_bundle=bundle,
                completed_request_map=completed_phrase_relationship_requests,
                timestamp=timestamp,
            )

            # Collected per chunk so the dump can say which phrases the model
            # answered under a different string. One sink PER STAGE: the same
            # phrase can be mis-echoed at more than one, and a shared dict would
            # keep only whichever was parsed last. Relationship is the only stage
            # still unheld, so a repair there stays invisible here.
            screening_repairs: dict[str, str] = {}
            freehand_grounding_repairs: dict[str, str] = {}
            llm_phrase_screening_flat = (
                await parse_relationship_screening_batch_req_result(
                    subject_unique_id=deferred_subject.subject_unique_id,
                    field_type=self.field_type,
                    chunk_bounds=chunk_bounds,
                    extraction_bundle=bundle,
                    completed_request_map=completed_relationship_screening_requests,
                    timestamp=timestamp,
                    repairs=screening_repairs,
                )
            )

            llm_phrase_freehand_grounding_flat = (
                await parse_freehand_grounding_batch_req_result(
                    subject_unique_id=deferred_subject.subject_unique_id,
                    field_type=self.field_type,
                    chunk_bounds=chunk_bounds,
                    extraction_bundle=bundle,
                    completed_request_map=completed_freehand_grounding_requests,
                    timestamp=timestamp,
                    repairs=freehand_grounding_repairs,
                )
            )

            # Partition flat dicts into per-round dicts by earliest search round.
            llm_phrase_relationship_results = partition_by_search_round(
                llm_phrase_relationship_flat, llm_search_results
            )
            llm_phrase_screening_results = partition_by_search_round(
                llm_phrase_screening_flat, llm_search_results
            )
            llm_phrase_freehand_grounding_results = partition_by_search_round(
                llm_phrase_freehand_grounding_flat, llm_search_results
            )

            grounded_keywords = {
                grounded_label
                for phrase_groundings in llm_phrase_freehand_grounding_flat.values()
                for grounded_label in phrase_groundings.keys()
                if not is_sentinel_grounding_label(grounded_label)
            }
            # Rows are driven by the SCREENED phrase set (plus any drifted
            # grounding-only phrases) — a grounding-driven dump made
            # screening.passed a constant and hid the screened-out majority.
            chunked_phrase_trails_dump[chunk_bounds] = build_keyword_phrase_rows(
                screening_flat=llm_phrase_screening_flat,
                relationship_flat=llm_phrase_relationship_flat,
                freehand_grounding_flat=llm_phrase_freehand_grounding_flat,
                search_rounds=llm_search_results,
                repairs_flat=merge_stage_repairs(
                    {
                        "screening": screening_repairs,
                        "freehand_grounding": freehand_grounding_repairs,
                    }
                ),
            )

            chunk_stats[chunk_bounds] = KeywordExtractionStats(
                results=grounded_keywords,
                llm_phrase_search=llm_search_results,
                llm_phrase_relationship=llm_phrase_relationship_results,
                llm_phrase_screening=llm_phrase_screening_results,
                llm_phrase_freehand_grounding=llm_phrase_freehand_grounding_results,
            )
            all_keywords.update(grounded_keywords)

        write_phrase_trails_dump(
            subject_unique_id=subject.subject_unique_id,
            field_type=self.field_type,
            timestamp=timestamp,
            chunked_phrase_trails=chunked_phrase_trails_dump,
        )

        final_extraction_result = KeywordExtractionResults(
            metadata=extraction_requests.metadata,
            # Freehand grounding names categories independently per chunk and
            # round, so the union carries case and singular/plural variants of
            # one category; per-chunk stats keep them raw.
            results=dedupe_equivalent_keywords(all_keywords),
            chunk_stats=chunk_stats,
        )

        setattr(subject, self.field_type.name, final_extraction_result)
        await subject.record_update(updated_at=timestamp)

        # call super wipe_down to clear deferred field and completed GPT requests from pipeline context
        await super().wipe_down(
            deferred_subject=deferred_subject,
            associated_batch_request_custom_ids=list(
                [
                    *completed_search_requests.keys(),
                    *completed_recursive_search_requests.keys(),
                    *completed_phrase_relationship_requests.keys(),
                    *completed_relationship_screening_requests.keys(),
                    *completed_freehand_grounding_requests.keys(),
                ]
            ),
        )
