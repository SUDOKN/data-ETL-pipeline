from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING, Optional

from core.models.db.manufacturer import Manufacturer
from core.models.deferred_extraction.deferred_keyword_extraction import (
    DeferredKeywordExtractionRequests,
)
from core.models.extraction_results.keyword_extraction_results import (
    KeywordExtractionStats,
    KeywordExtractionResults,
    KeywordExtractionStatsMap,
)
from core.models.db.deferred_manufacturer import DeferredManufacturer
from data_etl_app.models.pipeline_nodes.multi_stage.keyword.keyword_phrase_search_node import (
    KeywordPhraseSearchNode,
)
from data_etl_app.models.pipeline_nodes.multi_stage.keyword.keyword_recursive_search_node import (
    KeywordRecursiveSearchNode,
)
from data_etl_app.models.pipeline_nodes.multi_stage.keyword.keyword_relationship_node import (
    KeywordRelationshipNode,
)
from data_etl_app.models.pipeline_nodes.multi_stage.keyword.keyword_relationship_screening_node import (
    KeywordRelationshipScreeningNode,
)
from data_etl_app.models.pipeline_nodes.multi_stage.keyword.keyword_freehand_grounding_node import (
    KeywordFreehandGroundingNode,
)
from data_etl_app.models.types_and_enums import KeywordTypeEnum
from data_etl_app.models.pipeline_nodes.base.base_node import PipelineContext
from data_etl_app.models.pipeline_nodes.base.base_reconcile_node import (
    ReconcileNode,
    ResultT,
)

if TYPE_CHECKING:
    from scraper_app.models.scraped_text_file import ScrapedTextFile

from core.services.manufacturer_service import update_manufacturer
from data_etl_app.services.extraction.deferred_llm_phrase_search_node_service import (
    parse_batch_request_result as parse_phrase_search_batch_req_result,
)
from data_etl_app.services.extraction.deferred_llm_phrase_relationship_node_service import (
    get_phrase_relationship_result as get_phrase_relationship_result,
)
from data_etl_app.services.extraction.deferred_llm_relationship_screening_node_service import (
    get_phrase_relationship_screening_result as parse_relationship_screening_batch_req_result,
)
from data_etl_app.services.extraction.deferred_llm_freehand_grounding_service import (
    parse_batch_request_result as parse_freehand_grounding_batch_req_result,
)

logger = logging.getLogger(__name__)


class KeywordReconcileNode(ReconcileNode[KeywordTypeEnum, ResultT]):
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
        extraction_requests: Optional[DeferredKeywordExtractionRequests] = getattr(
            deferred_mfg, self.field_type.name
        )
        if not extraction_requests:
            raise ValueError(
                f"execute was called for {self.field_type.name} but no deferred extraction requests exist."
            )

        completed_search_requests = pipeline_context[KeywordPhraseSearchNode]
        completed_recursive_search_requests = pipeline_context[
            KeywordRecursiveSearchNode
        ]
        completed_phrase_relationship_requests = pipeline_context[
            KeywordRelationshipNode
        ]
        completed_relationship_screening_requests = pipeline_context[
            KeywordRelationshipScreeningNode
        ]
        completed_freehand_grounding_requests = pipeline_context[
            KeywordFreehandGroundingNode
        ]
        all_keywords: set[str] = set()
        chunk_stats: KeywordExtractionStatsMap = {}
        for (
            chunk_bounds,
            bundle,
        ) in extraction_requests.chunked_request_map.items():
            llm_search_results = await parse_phrase_search_batch_req_result(
                mfg_etld1=deferred_mfg.etld1,
                field_type=self.field_type,
                chunk_bounds=chunk_bounds,
                extraction_bundle=bundle,
                all_phrase_search_req_responses_map=completed_search_requests,
                deferred_at=timestamp,
            )

            llm_phrase_relationship_results = await get_phrase_relationship_result(
                mfg_etld1=deferred_mfg.etld1,
                field_type=self.field_type,
                chunk_bounds=chunk_bounds,
                extraction_bundle=bundle,
                completed_request_map=completed_phrase_relationship_requests,
                timestamp=timestamp,
            )

            llm_phrase_relationship_screening_results = (
                await parse_relationship_screening_batch_req_result(
                    mfg_etld1=deferred_mfg.etld1,
                    field_type=self.field_type,
                    chunk_bounds=chunk_bounds,
                    extraction_bundle=bundle,
                    completed_request_map=completed_relationship_screening_requests,
                    timestamp=timestamp,
                )
            )

            llm_phrase_freehand_grounding_results = (
                await parse_freehand_grounding_batch_req_result(
                    mfg_etld1=deferred_mfg.etld1,
                    field_type=self.field_type,
                    chunk_bounds=chunk_bounds,
                    extraction_bundle=bundle,
                    completed_request_map=completed_freehand_grounding_requests,
                    deferred_at=timestamp,
                )
            )
            grounded_keywords = {
                grounded_label
                for phrase_groundings in llm_phrase_freehand_grounding_results.values()
                for grounded_label in phrase_groundings.keys()
            }

            chunk_stats[chunk_bounds] = KeywordExtractionStats(
                results=grounded_keywords,
                llm_phrase_search=llm_search_results,
                llm_phrase_relationship=llm_phrase_relationship_results,
                llm_phrase_screening=llm_phrase_relationship_screening_results,
                llm_phrase_freehand_grounding=llm_phrase_freehand_grounding_results,
            )
            all_keywords.update(grounded_keywords)

        final_extraction_result = KeywordExtractionResults(
            metadata=extraction_requests.metadata,
            results=all_keywords,
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
                    *completed_recursive_search_requests.keys(),
                    *completed_phrase_relationship_requests.keys(),
                    *completed_relationship_screening_requests.keys(),
                    *completed_freehand_grounding_requests.keys(),
                ]
            ),
        )
