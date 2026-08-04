from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

from packages.core.src.core.models.base.extraction_subject import (
    AbstractExtractionSubject,
    AbstractDeferredExtractionSubject,
)
from packages.core.src.core.models.deferred_extraction.deferred_keyword_extraction import (
    DeferredKeywordExtractionRequests,
)
from packages.core.src.core.models.extraction_results.keyword_extraction_results import (
    KeywordExtractionStats,
    KeywordExtractionResults,
    KeywordExtractionStatsMap,
)
from packages.core.src.core.models.extraction_results.llm_phrase_extraction_results import (
    partition_by_search_round,
)
from packages.core.src.core.models.types_and_enums import KeywordTypeEnum
from packages.core.src.core.models.pipeline_nodes.base.base_node import PipelineContext
from packages.core.src.core.models.pipeline_nodes.base.base_reconcile_node import (
    ReconcileNode,
)
from apps.data_etl_app.src.data_etl_app.models.scraped_text_file import ScrapedTextFile

# if TYPE_CHECKING:

from packages.core.src.core.services.pipeline_nodes.multi_stage.llm_phrase_recursive_search_node_service import (
    build_llm_phrase_search_results,
)
from packages.core.src.core.services.pipeline_nodes.multi_stage.llm_phrase_relationship_node_service import (
    get_phrase_relationship_result as get_phrase_relationship_result,
)
from packages.core.src.core.services.pipeline_nodes.multi_stage.llm_relationship_screening_node_service import (
    get_phrase_relationship_screening_result as parse_relationship_screening_batch_req_result,
)
from packages.core.src.core.services.pipeline_nodes.multi_stage.llm_freehand_grounding_service import (
    get_freehand_grounding_result as parse_freehand_grounding_batch_req_result,
)
from packages.core.src.core.utils.phrase_trail_dump_util import (
    build_keyword_phrase_trail_entry,
    write_phrase_trails_dump,
)

logger = logging.getLogger(__name__)


class KeywordReconcileNode(ReconcileNode[KeywordTypeEnum]):
    """Base class: phase 6, aggregate & write final results.

    This is a BASE class: the 5 ``get_upstream_*_map`` getters are left
    unimplemented here. Concrete leaves such as ``PureProductReconcileNode`` /
    ``ContractProductReconcileNode`` must implement them, pointing at their own
    sibling node classes.
    """

    def __init__(self, field_type: KeywordTypeEnum) -> None:
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
                mfg_etld1=deferred_subject.subject_unique_id,
                field_type=self.field_type,
                chunk_bounds=chunk_bounds,
                extraction_bundle=bundle,
                completed_search_req_map=completed_search_requests,
                completed_recursive_search_req_map=completed_recursive_search_requests,
                timestamp=timestamp,
            )

            llm_phrase_relationship_flat = await get_phrase_relationship_result(
                mfg_etld1=deferred_subject.subject_unique_id,
                field_type=self.field_type,
                chunk_bounds=chunk_bounds,
                extraction_bundle=bundle,
                completed_request_map=completed_phrase_relationship_requests,
                timestamp=timestamp,
            )

            llm_phrase_screening_flat = (
                await parse_relationship_screening_batch_req_result(
                    mfg_etld1=deferred_subject.subject_unique_id,
                    field_type=self.field_type,
                    chunk_bounds=chunk_bounds,
                    extraction_bundle=bundle,
                    completed_request_map=completed_relationship_screening_requests,
                    timestamp=timestamp,
                )
            )

            llm_phrase_freehand_grounding_flat = (
                await parse_freehand_grounding_batch_req_result(
                    mfg_etld1=deferred_subject.subject_unique_id,
                    field_type=self.field_type,
                    chunk_bounds=chunk_bounds,
                    extraction_bundle=bundle,
                    completed_request_map=completed_freehand_grounding_requests,
                    timestamp=timestamp,
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

            # phrase -> earliest round index (for trail dump)
            phrase_to_round: dict[str, int] = {}
            for round_idx in sorted(llm_search_results.keys()):
                for phrase in llm_search_results[round_idx]:
                    if phrase not in phrase_to_round:
                        phrase_to_round[phrase] = round_idx

            grounded_keywords = {
                grounded_label
                for phrase_groundings in llm_phrase_freehand_grounding_flat.values()
                for grounded_label in phrase_groundings.keys()
            }
            chunked_phrase_trails_dump[chunk_bounds] = [
                build_keyword_phrase_trail_entry(
                    phrase=phrase,
                    search_round=phrase_to_round.get(phrase, 0),
                    relationship_result=llm_phrase_relationship_results,
                    screening_result=llm_phrase_screening_results,
                    phrase_groundings=phrase_groundings,
                )
                for phrase, phrase_groundings in sorted(
                    llm_phrase_freehand_grounding_flat.items()
                )
            ]

            chunk_stats[chunk_bounds] = KeywordExtractionStats(
                results=grounded_keywords,
                llm_phrase_search=llm_search_results,
                llm_phrase_relationship=llm_phrase_relationship_results,
                llm_phrase_screening=llm_phrase_screening_results,
                llm_phrase_freehand_grounding=llm_phrase_freehand_grounding_results,
            )
            all_keywords.update(grounded_keywords)

        write_phrase_trails_dump(
            mfg_etld1=subject.subject_unique_id,
            field_type=self.field_type,
            timestamp=timestamp,
            chunked_phrase_trails=chunked_phrase_trails_dump,
        )

        final_extraction_result = KeywordExtractionResults(
            metadata=extraction_requests.metadata,
            results=all_keywords,
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
