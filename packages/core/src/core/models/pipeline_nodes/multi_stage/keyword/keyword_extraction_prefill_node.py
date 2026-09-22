from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING, Optional

from core.models.extraction_subject import (
    AbstractExtractionSubject,
    AbstractDeferredExtractionSubject,
)
from core.models.extraction_results.extraction_node_metadata import (
    AggregationFoldMetadata,
    BatchedSynthesisNodeMetadata,
    ExtractionNodeMetadata,
    RecursiveSearchNodeMetadata,
    BatchedRelationshipNodeMetadata,
    BatchedScreeningNodeMetadata,
)
from core.models.extraction_results.keyword_extraction_results import (
    BatchedFreehandGroundingNodeMetadata,
)
from core.models.extraction_results.llm_phrase_extraction_results import (
    KeywordExtractionMetadata,
)
from core.models.deferred_extraction.deferred_keyword_extraction import (
    DeferredKeywordExtractionRequests,
    KeywordExtractionRequestBundle,
)
from core.models.pipeline_nodes.multi_stage.keyword.keyword_phrase_search_node import (
    KeywordPhraseSearchNode,
)
from core.models.pipeline_nodes.base.base_prefill_node import (
    PrefillNode,
)
from core.models.field_types import ExtractionFieldType
from core.models.pipeline_nodes.base.base_node import PipelineContext
from core.models.chunking_strat import (
    ChunkingStrategy,
    chunk_break_predicate,
    derive_search_sub_bounds,
)

if TYPE_CHECKING:
    from scraper.models.s3.scraped_text_file import (
        ScrapedTextFile,
    )


from llm_providers.utils.chunk_util import (
    get_chunks_respecting_line_boundaries,
)

logger = logging.getLogger(__name__)


class KeywordExtractionPrefillNode(PrefillNode[ExtractionFieldType]):
    next_node: KeywordPhraseSearchNode

    def __init__(
        self,
        field_type: ExtractionFieldType,
        chunk_strategy: ChunkingStrategy,
        next_node: KeywordPhraseSearchNode,
        ontology_version_id: str,
        llm_phrase_search_metadata: ExtractionNodeMetadata,
        llm_phrase_recursive_search_metadata: RecursiveSearchNodeMetadata,
        llm_phrase_freehand_grounding_metadata: BatchedFreehandGroundingNodeMetadata,
        # v3 (PIPELINE_V3_PLAN.md Phase 3.1): the aggregation fold's identity
        # (which since the 2026-09-03 location-stage merge also carries the
        # snippet-radius clip dial). Optional only so older construction
        # sites still compile; the factory always passes it.
        # v2 relationship — RETIRED at v3 3.3; Optional so older construction
        # sites still compile, and the factory no longer passes it.
        llm_phrase_relationship_metadata: Optional[
            BatchedRelationshipNodeMetadata
        ] = None,
        aggregation_fold_metadata: Optional[AggregationFoldMetadata] = None,
        # v3 (Phase 3.2): the synthesis stage's identity, same contract.
        llm_phrase_synthesis_metadata: Optional[BatchedSynthesisNodeMetadata] = None,
        # Step 2 (2026-09-22): unit screening over the freehand candidates.
        llm_phrase_unit_screening_metadata: Optional[BatchedScreeningNodeMetadata] = None,
    ):
        super().__init__(
            field_type=field_type,
            chunk_strategy=chunk_strategy,
            next_node=next_node,
        )
        self.llm_phrase_unit_screening_metadata = llm_phrase_unit_screening_metadata
        self.ontology_version_id = ontology_version_id
        self.aggregation_fold_metadata = aggregation_fold_metadata
        self.llm_phrase_synthesis_metadata = llm_phrase_synthesis_metadata
        self.llm_phrase_search_metadata = llm_phrase_search_metadata
        self.llm_phrase_recursive_search_metadata = llm_phrase_recursive_search_metadata
        self.llm_phrase_relationship_metadata = llm_phrase_relationship_metadata
        self.llm_phrase_freehand_grounding_metadata = (
            llm_phrase_freehand_grounding_metadata
        )

    async def execute(
        self,
        subject: AbstractExtractionSubject,
        deferred_subject: AbstractDeferredExtractionSubject,
        scraped_text_file: ScrapedTextFile,
        timestamp: datetime,
        pipeline_context: PipelineContext,
        eager: bool,
    ):
        # The text this chain chunks and reads: excluded pages dropped first when
        # the strategy says so (every run — bounds are offsets into this text).
        scraped_text_file = self.apply_page_exclusion(scraped_text_file, pipeline_context)
        latest_keyword_extraction_metadata = KeywordExtractionMetadata(
            created_at=timestamp,
            chunk_strat=self.chunk_strategy,
            ontology_version_id=self.ontology_version_id,
            llm_phrase_search=self.llm_phrase_search_metadata,
            llm_phrase_recursive_search=self.llm_phrase_recursive_search_metadata,
            llm_phrase_relationship=self.llm_phrase_relationship_metadata,
            llm_phrase_freehand_grounding=self.llm_phrase_freehand_grounding_metadata,
            aggregation_fold=self.aggregation_fold_metadata,
            llm_phrase_synthesis=self.llm_phrase_synthesis_metadata,
            llm_phrase_unit_screening=self.llm_phrase_unit_screening_metadata,
        )

        if not bool(getattr(deferred_subject, self.field_type.name)):
            chunk_map = await get_chunks_respecting_line_boundaries(
                text=scraped_text_file.text,
                soft_limit_tokens=self.chunk_strategy.max_tokens_per_chunk,
                overlap_ratio=self.chunk_strategy.overlap,
                max_chunks=self.chunk_strategy.max_chunks,
                llm_model=self.llm_phrase_search_metadata.llm_model,
                break_before=chunk_break_predicate(self.chunk_strategy),
            )

            deferred_keyword_extraction = DeferredKeywordExtractionRequests(
                metadata=latest_keyword_extraction_metadata,
                chunked_request_map={
                    chunk_bounds: KeywordExtractionRequestBundle(
                        search_sub_bounds=await derive_search_sub_bounds(
                            chunk_bounds=chunk_bounds,
                            chunk_text=chunk_text,
                            chunk_strategy=self.chunk_strategy,
                            llm_model=self.llm_phrase_search_metadata.llm_model,
                        ),
                        llm_phrase_search_req_ids=[],
                        llm_phrase_recursive_search_req_ids={},
                        llm_phrase_relationship_req_ids=[],
                        llm_phrase_freehand_grounding_req_ids=[],
                    )
                    for chunk_bounds, chunk_text in chunk_map.items()
                },
            )
            setattr(deferred_subject, self.field_type.name, deferred_keyword_extraction)
            await deferred_subject.save()
        else:
            deferred_keyword_extraction: DeferredKeywordExtractionRequests = getattr(
                deferred_subject, self.field_type.name
            )
            self.raise_if_metadata_is_stale(
                subject_unique_id=subject.subject_unique_id,
                stored_metadata_dump=deferred_keyword_extraction.metadata.model_dump(),
                latest_metadata_dump=latest_keyword_extraction_metadata.model_dump(),
            )

            logger.info(
                f"Chunking already done, resuming extraction for subject:{subject.subject_unique_id}"
            )

        await self.next_node.execute(
            subject=subject,
            deferred_subject=deferred_subject,
            scraped_text_file=scraped_text_file,
            timestamp=timestamp,
            pipeline_context=pipeline_context,
            eager=eager,
        )
