from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING


from core.models.extraction_results.concept_extraction_results import (
    ConceptExtractionMetadata,
    BatchedInitialGroundingNodeMetadata,
)
from core.models.extraction_results.llm_phrase_extraction_results import (
    ExtractionNodeMetadata,
    RecursiveSearchNodeMetadata,
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
from core.models.chunking_strat import ChunkingStrategy

if TYPE_CHECKING:
    from scraper.models.s3.scraped_text_file import (
        ScrapedTextFile,
    )

from core.services.brute_search_service import brute_search

from llm_providers.utils.chunk_util import (
    get_chunks_respecting_line_boundaries,
)
from pure_utils.dict_diff import find_diffs

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
        llm_phrase_relationship_metadata: ExtractionNodeMetadata,
        llm_phrase_relationship_screening_metadata: BatchedScreeningNodeMetadata,
        llm_phrase_initial_grounding_metadata: BatchedInitialGroundingNodeMetadata,
        llm_phrase_recursive_grounding_metadata: ExtractionNodeMetadata,
    ):
        super().__init__(
            field_type=field_type,
            chunk_strategy=chunk_strategy,
            next_node=next_node,
        )
        self.ontology = ontology
        self.llm_phrase_search_metadata = llm_phrase_search_metadata
        self.llm_phrase_recursive_search_metadata = llm_phrase_recursive_search_metadata
        self.llm_phrase_relationship_metadata = llm_phrase_relationship_metadata
        self.llm_phrase_relationship_screening_metadata = (
            llm_phrase_relationship_screening_metadata
        )
        self.llm_phrase_initial_grounding_metadata = (
            llm_phrase_initial_grounding_metadata
        )
        self.llm_phrase_recursive_grounding_metadata = (
            llm_phrase_recursive_grounding_metadata
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
        """
        BUSINESS LOGIC:

        1.  initialize metadata as latest
        2.  check if field is brand new
            yes? chunk up text, set metadata and populate an empty chunked_request_map
            no?  do nothing
        """
        latest_concept_extraction_metadata = ConceptExtractionMetadata(
            created_at=timestamp,
            chunk_strat=self.chunk_strategy,
            ontology_version_id=self.ontology.s3_version_id,
            llm_phrase_search=self.llm_phrase_search_metadata,
            llm_phrase_recursive_search=self.llm_phrase_recursive_search_metadata,
            llm_phrase_relationship=self.llm_phrase_relationship_metadata,
            llm_phrase_relationship_screening=self.llm_phrase_relationship_screening_metadata,
            llm_phrase_initial_grounding=self.llm_phrase_initial_grounding_metadata,
            llm_phrase_recursive_grounding=self.llm_phrase_recursive_grounding_metadata,
        )

        if not bool(getattr(deferred_subject, self.field_type.name)):
            chunk_map = await get_chunks_respecting_line_boundaries(
                text=scraped_text_file.text,
                soft_limit_tokens=self.chunk_strategy.max_tokens_per_chunk,
                overlap_ratio=self.chunk_strategy.overlap,
                max_chunks=self.chunk_strategy.max_chunks,
                llm_model=self.llm_phrase_search_metadata.llm_model,
            )
            known_concepts = self.ontology.get_concepts_flat(self.field_type)

            deferred_concept_extraction = DeferredConceptExtractionRequests(
                metadata=latest_concept_extraction_metadata,
                chunked_request_map={
                    chunk_bounds: ConceptExtractionRequestBundle(
                        brute={
                            label for label in brute_search(chunk_text, known_concepts)
                        },
                        llm_phrase_search_req_id=None,
                        llm_phrase_recursive_search_req_ids=[],
                        llm_phrase_relationship_req_id=None,
                        llm_phrase_relationship_screening_req_ids=[],
                        llm_phrase_initial_grounding_req_ids=[],
                        llm_phrase_recursive_tagging_reqs=None,
                    )
                    for chunk_bounds, chunk_text in chunk_map.items()
                },
            )
            setattr(deferred_subject, self.field_type.name, deferred_concept_extraction)
            await deferred_subject.save()
        else:
            deferred_concept_extraction: DeferredConceptExtractionRequests = getattr(
                deferred_subject, self.field_type.name
            )
            existing = deferred_concept_extraction.metadata.model_dump()
            latest = latest_concept_extraction_metadata.model_dump()
            diffs = find_diffs(existing, latest, exclude={"created_at"})

            # Metadata is written once, when the field is first deferred, and never
            # rewritten. A resumed subject therefore reuses cached requests built
            # from the OLD prompt while reporting whatever is configured now — so a
            # changed catalog would silently mix two rule sets in one extraction.
            # Prompt identity is not part of the request custom_id, so this check is
            # the only thing that catches it.
            catalog_diffs = {
                field: diff
                for field, diff in diffs.items()
                if "catalog_version" in field
            }
            if (
                deferred_concept_extraction.metadata.chunk_strat != self.chunk_strategy
                or catalog_diffs
            ):
                raise ValueError(
                    f"Cannot proceed {__class__.__name__} for subject:{subject.subject_unique_id} as "
                    f"Metadata mismatch for {subject.subject_unique_id}.{self.field_type.name} — differing fields: {diffs}"
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
