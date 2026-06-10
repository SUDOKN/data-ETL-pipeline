import logging
from datetime import datetime
from typing import Optional

from core.models.db.manufacturer import Manufacturer
from core.models.deferred_keyword_extraction import (
    DeferredKeywordExtractionRequests,
)
from core.models.keyword_extraction_results import (
    KeywordExtractionStats,
    KeywordExtractionResults,
    KeywordExtractionStatsMap,
)
from core.models.db.deferred_manufacturer import DeferredManufacturer
from data_etl_app.models.pipeline_nodes.keyword.keyword_search_node import (
    KeywordSearchNode,
)
from data_etl_app.models.pipeline_nodes.keyword.keyword_evidence_node import (
    KeywordEvidenceNode,
)
from data_etl_app.models.types_and_enums import KeywordTypeEnum
from data_etl_app.models.pipeline_nodes.base_node import PipelineContext
from data_etl_app.models.pipeline_nodes.reconcile_node import ReconcileNode
from open_ai_key_app.models.gpt_model_params import GPTModelParams
from litellm_proxy_app.models.llm_model import LLM_Model
from scraper_app.models.scraped_text_file import ScrapedTextFile

from core.services.manufacturer_service import update_manufacturer

from data_etl_app.utils.ground_truth_helper_util import (
    get_verified_evidence_phrases_from_raw_evidence_results,
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
        llm_model: LLM_Model,
        model_params: GPTModelParams,
        eager: bool,
    ) -> None:
        extraction_requests: Optional[DeferredKeywordExtractionRequests] = getattr(
            deferred_mfg, self.field_type.name
        )
        if not extraction_requests:
            raise ValueError(
                f"execute was called for {self.field_type.name} but no deferred extraction requests exist."
            )

        completed_search_requests = pipeline_context[KeywordSearchNode]
        completed_evidence_requests = pipeline_context[KeywordEvidenceNode]
        all_keywords: set[str] = set()
        chunk_stats: KeywordExtractionStatsMap = {}
        for (
            chunk_bounds,
            bundle,
        ) in extraction_requests.request_map.items():
            llm_search_results = await KeywordSearchNode.parse_batch_request_result(
                mfg_etld1=deferred_mfg.etld1,
                field_type=self.field_type,
                chunk_bounds=chunk_bounds,
                extraction_bundle=bundle,
                completed_request_map=completed_search_requests,
                deferred_at=timestamp,
            )

            llm_evidence_results = await KeywordEvidenceNode.parse_batch_request_result(
                mfg_etld1=deferred_mfg.etld1,
                field_type=self.field_type,
                chunk_bounds=chunk_bounds,
                extraction_bundle=bundle,
                completed_request_map=completed_evidence_requests,
                deferred_at=timestamp,
            )

            confirmed_keywords_w_evidence = (
                get_verified_evidence_phrases_from_raw_evidence_results(
                    llm_evidence_results=llm_evidence_results
                )
            )
            confirmed_keywords = set(confirmed_keywords_w_evidence.keys())

            chunk_stats[chunk_bounds] = KeywordExtractionStats(
                results=confirmed_keywords,
                llm_search=llm_search_results,
                llm_evidence=llm_evidence_results,
            )
            all_keywords.update(confirmed_keywords)

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
                    *completed_evidence_requests.keys(),
                ]
            ),
        )
