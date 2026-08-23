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
from core.models.extraction_results.concept_extraction_results import (
    ConceptsFound,
)
from core.models.extraction_results.llm_phrase_extraction_results_v2 import (
    KeywordExtractionResultsV2,
    KeywordExtractionStatsMapV2,
    KeywordExtractionStatsV2,
    partition_records_by_search_round,
)
from core.models.field_types import ExtractionFieldType
from core.models.rule_catalog import STAGE_FREEHAND_GROUNDING
from core.models.pipeline_nodes.base.base_node import PipelineContext
from core.models.pipeline_nodes.base.base_reconcile_node import (
    ReconcileNode,
)
from scraper.models.s3.scraped_text_file import ScrapedTextFile

from core.services.pipeline_nodes.multi_stage.llm_phrase_recursive_search_node_service import (
    build_llm_phrase_search_results,
)
from core.services.pipeline_nodes.multi_stage.llm_phrase_relationship_node_service import (
    get_masked_phrase_relationship_result,
)
from core.services.pipeline_nodes.multi_stage.llm_grounding_node_service import (
    get_record_grounding_result,
)
from core.services.pipeline_nodes.multi_stage.llm_relationship_screening_node_service import (
    get_record_screening_result,
    screening_catalog_for,
)
from core.services.pipeline_nodes.multi_stage.pipeline_v2_derivations import (
    candidates_that_passed,
)
from core.services.rule_catalog_registry import get_rule_catalog
from core.utils.label_dedupe_util import dedupe_equivalent_keywords
from core.utils.record_id_util import phrases_by_record_id
from core.utils.extraction_dump_util import (
    build_keyword_record_rows,
    build_run_provenance,
    write_extraction_dump,
)

logger = logging.getLogger(__name__)


class KeywordReconcileNode(ReconcileNode[ExtractionFieldType]):
    """Base class: final phase, aggregate & write final results.

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
        if await self.stop_if_stage_disabled(
            subject=subject,
            deferred_subject=deferred_subject,
            scraped_text_file=scraped_text_file,
            timestamp=timestamp,
            pipeline_context=pipeline_context,
        ):
            return

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
        completed_freehand_grounding_requests = (
            self.get_upstream_freehand_grounding_map(pipeline_context)
        )
        completed_relationship_screening_requests = self.get_upstream_screening_map(
            pipeline_context
        )

        freehand_catalog = get_rule_catalog(
            STAGE_FREEHAND_GROUNDING, self.field_type.name
        )
        screening_catalog = screening_catalog_for(self.field_type.name)

        all_keywords: set[str] = set()
        chunk_stats: KeywordExtractionStatsMapV2 = {}
        chunked_dump_contents: dict[str, dict[str, object]] = {}
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

            # The relationship stage is the one place the model still echoes
            # phrases back, so it keeps a repairs sink; every record-keyed
            # stage downstream holds exactly and has none.
            relationship_repairs: dict[str, str] = {}
            masked_flat = await get_masked_phrase_relationship_result(
                subject_unique_id=deferred_subject.subject_unique_id,
                field_type=self.field_type,
                chunk_bounds=chunk_bounds,
                extraction_bundle=bundle,
                completed_request_map=completed_phrase_relationship_requests,
                timestamp=timestamp,
                repairs=relationship_repairs,
            )
            phrase_by_record_id = phrases_by_record_id(masked_flat)

            freehand_flat = await get_record_grounding_result(
                stage_label="freehand grounding",
                subject_unique_id=deferred_subject.subject_unique_id,
                field_name=self.field_type.name,
                chunk_bounds=chunk_bounds,
                catalog=freehand_catalog,
                group_req_ids=bundle.llm_phrase_freehand_grounding_req_ids,
                completed_request_map=completed_freehand_grounding_requests,
                timestamp=timestamp,
            )

            screening_flat = await get_record_screening_result(
                subject_unique_id=deferred_subject.subject_unique_id,
                field_name=self.field_type.name,
                chunk_bounds=chunk_bounds,
                catalog=screening_catalog,
                group_req_ids=bundle.llm_phrase_relationship_screening_req_ids,
                completed_request_map=completed_relationship_screening_requests,
                timestamp=timestamp,
            )

            # Fork F10: a keyword IS a minted candidate that passed screening
            # on at least one record. Uniform ConceptsFound with in_vocab empty
            # by construction — keyword fields have no vocabulary.
            grounded_keywords = candidates_that_passed(freehand_flat, screening_flat)

            chunked_dump_contents[chunk_bounds] = {
                "rows": build_keyword_record_rows(
                    masked_flat=masked_flat,
                    freehand_flat=freehand_flat,
                    screening_flat=screening_flat,
                    search_rounds=llm_search_results,
                    relationship_repairs=relationship_repairs,
                    subject_name=pipeline_context.subject_name,
                )
            }

            chunk_stats[chunk_bounds] = KeywordExtractionStatsV2(
                results=ConceptsFound(
                    in_vocab=set(), out_of_vocab=grounded_keywords
                ),
                llm_phrase_search=llm_search_results,
                llm_phrase_relationship=partition_records_by_search_round(
                    masked_flat, phrase_by_record_id, llm_search_results
                ),
                llm_phrase_screening=partition_records_by_search_round(
                    screening_flat, phrase_by_record_id, llm_search_results
                ),
                llm_phrase_freehand_grounding=partition_records_by_search_round(
                    freehand_flat, phrase_by_record_id, llm_search_results
                ),
            )
            all_keywords.update(grounded_keywords)

        write_extraction_dump(
            subject_unique_id=subject.subject_unique_id,
            field_type=self.field_type,
            timestamp=timestamp,
            chunked_contents=chunked_dump_contents,
            chunked_request_map=extraction_requests.chunked_request_map,
            completed_requests={
                **completed_search_requests,
                **completed_recursive_search_requests,
                **completed_phrase_relationship_requests,
                **completed_freehand_grounding_requests,
                **completed_relationship_screening_requests,
            },
            run_provenance=build_run_provenance(
                metadata=extraction_requests.metadata,
                scraped_text_file=scraped_text_file,
                partial=False,
                page_exclusion=pipeline_context.page_exclusion,
            ),
        )

        final_extraction_result = KeywordExtractionResultsV2(
            metadata=extraction_requests.metadata,
            # Freehand grounding mints candidates independently per chunk, so
            # the union carries case and singular/plural variants of one label;
            # per-chunk stats keep them raw.
            results=ConceptsFound(
                in_vocab=set(),
                out_of_vocab=dedupe_equivalent_keywords(all_keywords),
            ),
            chunked_extraction_stats=chunk_stats,
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
                    *completed_freehand_grounding_requests.keys(),
                    *completed_relationship_screening_requests.keys(),
                ]
            ),
        )
