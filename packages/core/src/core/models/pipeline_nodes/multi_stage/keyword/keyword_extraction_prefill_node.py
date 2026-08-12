from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING

from core.models.extraction_subject import (
    AbstractExtractionSubject,
    AbstractDeferredExtractionSubject,
)
from core.models.extraction_results.llm_phrase_extraction_results import (
    ExtractionNodeMetadata,
    RecursiveSearchNodeMetadata,
    BatchedRelationshipNodeMetadata,
    BatchedScreeningNodeMetadata,
)
from core.models.extraction_results.keyword_extraction_results import (
    BatchedFreehandGroundingNodeMetadata,
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
from core.models.pipeline_nodes.base.base_node import ResultT
from core.models.field_types import ExtractionFieldType
from core.models.pipeline_nodes.base.base_node import PipelineContext
from core.models.chunking_strat import ChunkingStrategy

if TYPE_CHECKING:
    from scraper.models.s3.scraped_text_file import (
        ScrapedTextFile,
    )


from llm_providers.utils.chunk_util import (
    get_chunks_respecting_line_boundaries,
)
from pure_utils.dict_diff import find_diffs

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
        llm_phrase_relationship_metadata: BatchedRelationshipNodeMetadata,
        llm_phrase_relationship_screening_metadata: BatchedScreeningNodeMetadata,
        llm_phrase_freehand_grounding_metadata: BatchedFreehandGroundingNodeMetadata,
    ):
        super().__init__(
            field_type=field_type,
            chunk_strategy=chunk_strategy,
            next_node=next_node,
        )
        self.ontology_version_id = ontology_version_id
        self.llm_phrase_search_metadata = llm_phrase_search_metadata
        self.llm_phrase_recursive_search_metadata = llm_phrase_recursive_search_metadata
        self.llm_phrase_relationship_metadata = llm_phrase_relationship_metadata
        self.llm_phrase_relationship_screening_metadata = (
            llm_phrase_relationship_screening_metadata
        )
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
        latest_keyword_extraction_metadata = KeywordExtractionMetadata(
            created_at=timestamp,
            chunk_strat=self.chunk_strategy,
            ontology_version_id=self.ontology_version_id,
            llm_phrase_search=self.llm_phrase_search_metadata,
            llm_phrase_recursive_search=self.llm_phrase_recursive_search_metadata,
            llm_phrase_relationship=self.llm_phrase_relationship_metadata,
            llm_phrase_relationship_screening=self.llm_phrase_relationship_screening_metadata,
            llm_phrase_freehand_grounding=self.llm_phrase_freehand_grounding_metadata,
        )

        if not bool(getattr(deferred_subject, self.field_type.name)):
            chunk_map = await get_chunks_respecting_line_boundaries(
                text=scraped_text_file.text,
                soft_limit_tokens=self.chunk_strategy.max_tokens_per_chunk,
                overlap_ratio=self.chunk_strategy.overlap,
                max_chunks=self.chunk_strategy.max_chunks,
                llm_model=self.llm_phrase_search_metadata.llm_model,
            )

            deferred_keyword_extraction = DeferredKeywordExtractionRequests(
                metadata=latest_keyword_extraction_metadata,
                chunked_request_map={
                    chunk_bounds: KeywordExtractionRequestBundle(
                        llm_phrase_search_req_id=None,
                        llm_phrase_recursive_search_req_ids=[],
                        llm_phrase_relationship_req_ids=[],
                        llm_phrase_relationship_screening_req_ids=[],
                        llm_phrase_freehand_grounding_req_ids=[],
                    )
                    for chunk_bounds, _chunk_text in chunk_map.items()
                },
            )
            setattr(deferred_subject, self.field_type.name, deferred_keyword_extraction)
            await deferred_subject.save()
        else:
            deferred_keyword_extraction: DeferredKeywordExtractionRequests = getattr(
                deferred_subject, self.field_type.name
            )
            existing = deferred_keyword_extraction.metadata.model_dump()
            latest = latest_keyword_extraction_metadata.model_dump()
            diffs = find_diffs(existing, latest, exclude={"created_at"})
            if diffs:
                raise ValueError(
                    f"Cannot proceed {__class__.__name__} for subject:{subject.subject_unique_id} as "
                    f"Metadata mismatch for {subject.subject_unique_id}.{self.field_type.name} — differing fields: {diffs}"
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
