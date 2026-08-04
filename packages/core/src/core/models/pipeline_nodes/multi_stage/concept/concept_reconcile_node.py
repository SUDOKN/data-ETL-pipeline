from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

from packages.core.src.core.models.base.extraction_subject import (
    AbstractExtractionSubject,
    AbstractDeferredExtractionSubject,
)
from packages.core.src.core.models.extraction_results.concept_extraction_results import (
    ConceptsFound,
    ConceptExtractionStatsMap,
    ConceptExtractionStats,
    ConceptExtractionResults,
)
from packages.core.src.core.models.extraction_results.llm_phrase_extraction_results import (
    partition_by_search_round,
)
from packages.core.src.core.models.deferred_extraction.deferred_concept_extraction import (
    DeferredConceptExtractionRequests,
    IterativeTaggingRequest,
)
from packages.core.src.core.models.types_and_enums import ConceptTypeEnum
from packages.core.src.core.models.skos_concept import Concept
from packages.core.src.core.models.pipeline_nodes.base.base_node import PipelineContext
from packages.core.src.core.models.pipeline_nodes.base.base_reconcile_node import (
    ReconcileNode,
)
from packages.core.src.core.models.pipeline_nodes.multi_stage.concept.concept_phrase_search_node import (
    ConceptPhraseSearchNode,
)
from packages.core.src.core.models.pipeline_nodes.multi_stage.concept.concept_recursive_search_node import (
    ConceptRecursiveSearchNode,
)
from packages.core.src.core.models.pipeline_nodes.multi_stage.concept.concept_relationship_node import (
    ConceptRelationshipNode,
)
from packages.core.src.core.models.pipeline_nodes.multi_stage.concept.concept_relationship_screening_node import (
    ConceptRelationshipScreeningNode,
)
from packages.core.src.core.models.pipeline_nodes.multi_stage.concept.concept_initial_grounding_node import (
    ConceptInitialGroundingNode,
)
from packages.core.src.core.models.pipeline_nodes.multi_stage.concept.concept_iterative_grounding_node import (
    ConceptIterativeGroundingNode,
)
from apps.data_etl_app.src.data_etl_app.models.scraped_text_file import ScrapedTextFile

from packages.core.src.core.services.pipeline_nodes.multi_stage.llm_phrase_recursive_search_node_service import (
    build_llm_phrase_search_results,
)
from packages.core.src.core.services.pipeline_nodes.multi_stage.llm_initial_grounding_service import (
    get_tagged_results_from_initial_grounding,
)
from packages.core.src.core.services.pipeline_nodes.multi_stage.llm_recursive_grounding_service import (
    get_phrase_trails,
    get_deepest_concepts_and_oov,
)


from packages.core.src.core.utils.rdf_to_graph_util import (
    get_match_label_to_concept_map,
)
from packages.core.src.core.utils.phrase_trail_dump_util import (
    build_concept_phrase_trail_entry,
    write_phrase_trails_dump,
)

logger = logging.getLogger(__name__)


class ConceptReconcileNode(ReconcileNode[ConceptTypeEnum]):
    def __init__(
        self,
        concept_type: ConceptTypeEnum,
        known_concepts: set[Concept],
    ):
        super().__init__(field_type=concept_type)
        self.known_concepts = known_concepts
        self.match_label_to_concept_map = get_match_label_to_concept_map(known_concepts)

    async def execute(
        self,
        subject: AbstractExtractionSubject,
        deferred_subject: AbstractDeferredExtractionSubject,
        scraped_text_file: ScrapedTextFile,
        timestamp: datetime,
        pipeline_context: PipelineContext,
        eager: bool,
    ) -> None:

        extraction_requests: Optional[DeferredConceptExtractionRequests] = getattr(
            deferred_subject, self.field_type.name
        )
        if not extraction_requests:
            raise ValueError(
                f"reconcile was called for {self.field_type.name} but no deferred concept extraction exists."
            )

        completed_phrase_search_req_map = pipeline_context[ConceptPhraseSearchNode]
        completed_recursive_search_req_map = pipeline_context[
            ConceptRecursiveSearchNode
        ]
        completed_phrase_relationship_req_map = pipeline_context[
            ConceptRelationshipNode
        ]
        completed_relationship_screening_req_map = pipeline_context[
            ConceptRelationshipScreeningNode
        ]
        completed_initial_grounding_req_map = pipeline_context[
            ConceptInitialGroundingNode
        ]
        completed_recursive_grounding_req_map = pipeline_context[
            ConceptIterativeGroundingNode
        ]

        all_recognized_tagged_concepts: set[Concept] = set()
        all_unrecognized_tagged_concepts: set[str] = set()
        chunk_stats: ConceptExtractionStatsMap = {}
        chunked_phrase_trails_dump: dict[str, list[dict[str, object]]] = {}
        for (
            chunk_bounds,
            bundle,
        ) in extraction_requests.chunked_request_map.items():
            iteratively_tagged_concept_nodes = bundle.llm_phrase_recursive_tagging_reqs
            if iteratively_tagged_concept_nodes is None:
                raise ValueError(
                    f"Cannot proceed to reconcile {self.field_type.name} for {subject.subject_unique_id}:{chunk_bounds} "
                    f"as bundle.llm_phrase_recursive_grounding_root_req_nodes is None implying "
                    f"recursive grounding hasn't been executed yet."
                )

            unrecognized_tagged_concepts: set[str] = set()
            recognized_tagged_concepts: set[Concept] = set()

            initially_tagged_trs = await get_tagged_results_from_initial_grounding(
                mfg_etld1=subject.subject_unique_id,
                field_type=self.field_type,
                chunk_bounds=chunk_bounds,
                extraction_bundle=bundle,
                completed_request_map=completed_initial_grounding_req_map,
                timestamp=timestamp,
            )
            for tr in initially_tagged_trs:
                if tr.group_id not in self.match_label_to_concept_map:
                    unrecognized_tagged_concepts.add(tr.group_id)
                # else: it will have already entered iterative tagging at some level

            llm_phrase_relationship_flat = await ConceptRelationshipNode.get_result(
                subject_unique_id=subject.subject_unique_id,
                field_type=self.field_type,
                chunk_bounds=chunk_bounds,
                extraction_bundle=bundle,
                timestamp=timestamp,
                completed_request_map=completed_phrase_relationship_req_map,
            )
            llm_phrase_screening_flat = (
                await ConceptRelationshipScreeningNode.get_result(
                    subject_unique_id=subject.subject_unique_id,
                    field_type=self.field_type,
                    chunk_bounds=chunk_bounds,
                    extraction_bundle=bundle,
                    completed_request_map=completed_relationship_screening_req_map,
                    timestamp=timestamp,
                )
            )

            llm_search_results = await build_llm_phrase_search_results(
                mfg_etld1=subject.subject_unique_id,
                field_type=self.field_type,
                chunk_bounds=chunk_bounds,
                extraction_bundle=bundle,
                completed_search_req_map=completed_phrase_search_req_map,
                completed_recursive_search_req_map=completed_recursive_search_req_map,
                timestamp=timestamp,
                brute_search_results=bundle.brute,
            )
            llm_phrase_initial_grounding_flat = (
                await ConceptInitialGroundingNode.get_result(
                    subject_unique_id=subject.subject_unique_id,
                    field_type=self.field_type,
                    chunk_bounds=chunk_bounds,
                    extraction_bundle=bundle,
                    completed_request_map=completed_initial_grounding_req_map,
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
            llm_phrase_initial_grounding_results = partition_by_search_round(
                llm_phrase_initial_grounding_flat, llm_search_results
            )

            # phrase -> earliest round index (for trail dump)
            phrase_to_round: dict[str, int] = {}
            for round_idx in sorted(llm_search_results.keys()):
                for phrase in llm_search_results[round_idx]:
                    if phrase not in phrase_to_round:
                        phrase_to_round[phrase] = round_idx

            lvl_by_lvl_iterative_grounding_results = await ConceptIterativeGroundingNode.get_result(
                subject_unique_id=subject.subject_unique_id,
                field_type=self.field_type,
                chunk_bounds=chunk_bounds,
                extraction_bundle=bundle,
                completed_initial_grounding_req_map=completed_initial_grounding_req_map,
                completed_recursive_grounding_req_map=completed_recursive_grounding_req_map,
                match_label_to_concept_map=self.match_label_to_concept_map,
                timestamp=timestamp,
            )
            phrase_trails = get_phrase_trails(
                lvl_by_lvl_iterative_grounding_results=lvl_by_lvl_iterative_grounding_results
            )
            chunked_phrase_trails_dump[chunk_bounds] = [
                build_concept_phrase_trail_entry(
                    phrase_trail=phrase_trail,
                    search_round=phrase_to_round.get(phrase_trail.phrase, 0),
                    relationship_result=llm_phrase_relationship_results,
                    screening_result=llm_phrase_screening_results,
                )
                for phrase_trail in sorted(
                    phrase_trails,
                    key=lambda phrase_trail: phrase_trail.phrase,
                )
            ]
            for phrase_trail in phrase_trails:
                recognized_deepest_concepts, oov = get_deepest_concepts_and_oov(
                    phrase_trail=phrase_trail,
                    match_label_to_concept_map=self.match_label_to_concept_map,
                )
                unrecognized_tagged_concepts.update(oov)
                recognized_tagged_concepts.update(recognized_deepest_concepts)

            # prune recognized_tagged_concepts to only contain only deepest nodes (may or may not be a leaf)

            chunk_stats[chunk_bounds] = ConceptExtractionStats(
                results=ConceptsFound(
                    in_vocab={c.name for c in recognized_tagged_concepts},
                    out_of_vocab={uc for uc in unrecognized_tagged_concepts},
                ),
                brute_search=bundle.brute,
                llm_phrase_search=llm_search_results,
                llm_phrase_relationship=llm_phrase_relationship_results,
                llm_phrase_screening=llm_phrase_screening_results,
                llm_phrase_initial_grounding=llm_phrase_initial_grounding_results,
                llm_phrase_recursive_grounding=lvl_by_lvl_iterative_grounding_results,
            )

            all_recognized_tagged_concepts.update(recognized_tagged_concepts)
            all_unrecognized_tagged_concepts.update(unrecognized_tagged_concepts)

        write_phrase_trails_dump(
            mfg_etld1=subject.subject_unique_id,
            field_type=self.field_type,
            timestamp=timestamp,
            chunked_phrase_trails=chunked_phrase_trails_dump,
        )

        final_extraction_result = ConceptExtractionResults(
            metadata=extraction_requests.metadata,
            results=ConceptsFound(
                in_vocab={c.name for c in all_recognized_tagged_concepts},
                out_of_vocab={uc for uc in all_unrecognized_tagged_concepts},
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
                    *completed_phrase_search_req_map.keys(),
                    *completed_recursive_search_req_map.keys(),
                    *completed_phrase_relationship_req_map.keys(),
                    *completed_relationship_screening_req_map.keys(),
                    *completed_initial_grounding_req_map.keys(),
                    *completed_recursive_grounding_req_map.keys(),
                ]
            ),
        )
