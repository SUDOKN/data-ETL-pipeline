from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING


from typing import Optional

from core.models.extraction_results.concept_extraction_results import (
    BatchedInitialGroundingNodeMetadata,
)
from core.models.extraction_results.llm_phrase_extraction_results import (
    ConceptExtractionMetadata,
)
from core.models.extraction_results.extraction_node_metadata import (
    DescentNodeMetadata,
    ExtractionNodeMetadata,
    RecursiveSearchNodeMetadata,
    BatchedRelationshipNodeMetadata,
    BatchedScreeningNodeMetadata,
)
from core.models.extraction_subject import (
    AbstractExtractionSubject,
    AbstractDeferredExtractionSubject,
)
from core.models.deferred_extraction.deferred_concept_extraction import (
    ConceptExtractionRequestBundle,
    DeferredConceptExtractionRequests,
)
from core.models.ontology import Ontology
from core.models.pipeline_nodes.multi_stage.concept.concept_phrase_search_node import (
    ConceptPhraseSearchNode,
)
from core.models.pipeline_nodes.base.base_prefill_node import (
    PrefillNode,
)
from core.models.field_types import ConceptFieldType
from core.models.pipeline_nodes.base.base_node import PipelineContext
from core.models.chunking_strat import (
    ChunkingStrategy,
    chunk_break_predicate,
    derive_search_sub_bounds,
)
from core.models.extraction_results.extraction_node_metadata import (
    AggregationFoldMetadata,
    BatchedSynthesisNodeMetadata,
)
from core.services.pipeline_nodes.multi_stage.aggregation_fold_service import (
    brute_casings_in_window,
    window_text_of,
)

if TYPE_CHECKING:
    from scraper.models.s3.scraped_text_file import (
        ScrapedTextFile,
    )

from core.services.brute_search_service import brute_search

from llm_providers.utils.chunk_util import (
    get_chunks_respecting_line_boundaries,
)

logger = logging.getLogger(__name__)


class ConceptExtractionPrefillNode(PrefillNode[ConceptFieldType]):
    next_node: ConceptPhraseSearchNode

    def __init__(
        self,
        field_type: ConceptFieldType,
        chunk_strategy: ChunkingStrategy,
        next_node: ConceptPhraseSearchNode,
        ontology: Ontology,
        llm_phrase_search_metadata: ExtractionNodeMetadata,
        llm_phrase_recursive_search_metadata: RecursiveSearchNodeMetadata,
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
        # Step 2 (2026-09-22): the one grounding call, the proposal pass (None =
        # off for this run), unit screening (the descent's wave screens) and
        # descent (with the leaf-step flag). Optional only so older
        # construction sites compile; the factory passes all four. The four
        # families they replaced were retired at the cutover.
        llm_phrase_grounding_metadata: Optional[BatchedInitialGroundingNodeMetadata] = None,
        llm_phrase_proposal_metadata: Optional[BatchedInitialGroundingNodeMetadata] = None,
        llm_phrase_unit_screening_metadata: Optional[BatchedScreeningNodeMetadata] = None,
        llm_phrase_descent_metadata: Optional[DescentNodeMetadata] = None,
    ):
        super().__init__(
            field_type=field_type,
            chunk_strategy=chunk_strategy,
            next_node=next_node,
        )
        self.llm_phrase_grounding_metadata = llm_phrase_grounding_metadata
        self.llm_phrase_proposal_metadata = llm_phrase_proposal_metadata
        self.llm_phrase_unit_screening_metadata = llm_phrase_unit_screening_metadata
        self.llm_phrase_descent_metadata = llm_phrase_descent_metadata
        self.ontology = ontology
        self.llm_phrase_search_metadata = llm_phrase_search_metadata
        self.llm_phrase_recursive_search_metadata = llm_phrase_recursive_search_metadata
        self.llm_phrase_relationship_metadata = llm_phrase_relationship_metadata
        self.aggregation_fold_metadata = aggregation_fold_metadata
        self.llm_phrase_synthesis_metadata = llm_phrase_synthesis_metadata

    async def execute(
        self,
        subject: AbstractExtractionSubject,
        deferred_subject: AbstractDeferredExtractionSubject,
        scraped_text_file: ScrapedTextFile,
        timestamp: datetime,
        pipeline_context: PipelineContext,
        eager: bool,
    ):
        """
        BUSINESS LOGIC:

        1.  initialize metadata as latest
        2.  check if field is brand new
            yes? chunk up text, set metadata and populate an empty chunked_request_map
            no?  do nothing
        """
        # The text this chain chunks and reads: excluded pages dropped first when
        # the strategy says so (every run — bounds are offsets into this text).
        scraped_text_file = self.apply_page_exclusion(scraped_text_file, pipeline_context)
        latest_concept_extraction_metadata = ConceptExtractionMetadata(
            created_at=timestamp,
            chunk_strat=self.chunk_strategy,
            ontology_version_id=self.ontology.s3_version_id,
            llm_phrase_search=self.llm_phrase_search_metadata,
            llm_phrase_recursive_search=self.llm_phrase_recursive_search_metadata,
            llm_phrase_relationship=self.llm_phrase_relationship_metadata,
            aggregation_fold=self.aggregation_fold_metadata,
            llm_phrase_synthesis=self.llm_phrase_synthesis_metadata,
            llm_phrase_grounding=self.llm_phrase_grounding_metadata,
            llm_phrase_proposal=self.llm_phrase_proposal_metadata,
            llm_phrase_unit_screening=self.llm_phrase_unit_screening_metadata,
            llm_phrase_descent=self.llm_phrase_descent_metadata,
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
            known_concepts = self.ontology.get_concepts_flat(self.field_type)

            chunked_request_map: dict[str, ConceptExtractionRequestBundle] = {}
            for chunk_bounds, chunk_text in chunk_map.items():
                search_sub_bounds = await derive_search_sub_bounds(
                    chunk_bounds=chunk_bounds,
                    chunk_text=chunk_text,
                    chunk_strategy=self.chunk_strategy,
                    llm_model=self.llm_phrase_search_metadata.llm_model,
                )
                brute = {label for label in brute_search(chunk_text, known_concepts)}
                chunked_request_map[chunk_bounds] = ConceptExtractionRequestBundle(
                    search_sub_bounds=search_sub_bounds,
                    brute=brute,
                    # v3: the casings of the brute labels as each sub-window
                    # actually spells them — the brute survivors' way into the
                    # fold's scan (exact-string contract). Text is in hand only
                    # here, so it is written now and read at embed time.
                    brute_by_sub_bounds={
                        sub_bounds: brute_casings_in_window(
                            window_text_of(scraped_text_file.text, sub_bounds), brute
                        )
                        for sub_bounds in search_sub_bounds
                    },
                    llm_phrase_search_req_ids=[],
                    llm_phrase_recursive_search_req_ids={},
                    llm_phrase_relationship_req_ids=[],
                )

            deferred_concept_extraction = DeferredConceptExtractionRequests(
                metadata=latest_concept_extraction_metadata,
                chunked_request_map=chunked_request_map,
            )
            setattr(deferred_subject, self.field_type.name, deferred_concept_extraction)
            await deferred_subject.save()
        else:
            deferred_concept_extraction: DeferredConceptExtractionRequests = getattr(
                deferred_subject, self.field_type.name
            )
            # Metadata is written once, when the field is first deferred, and never
            # rewritten, so a resumed subject reuses requests built under it. This
            # check is what keeps that honest: any drift stops the run rather than
            # letting today's prompt be filed under yesterday's version id.
            self.raise_if_metadata_is_stale(
                subject_unique_id=subject.subject_unique_id,
                stored_metadata_dump=deferred_concept_extraction.metadata.model_dump(),
                latest_metadata_dump=latest_concept_extraction_metadata.model_dump(),
            )

            logger.info(
                f"Chunking already done, resuming extraction for subject:{subject.subject_unique_id}"
            )

        await super().execute(
            subject=subject,
            deferred_subject=deferred_subject,
            scraped_text_file=scraped_text_file,
            timestamp=timestamp,
            pipeline_context=pipeline_context,
            eager=eager,
        )
