from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

from core.models.db.manufacturer import Manufacturer
from core.models.extraction_results.concept_extraction_results import (
    ConceptsFound,
    ConceptExtractionStatsMap,
    ConceptExtractionStats,
    ConceptExtractionResults,
)
from core.models.db.deferred_manufacturer import DeferredManufacturer
from core.models.deferred_extraction.deferred_concept_extraction import (
    DeferredConceptExtractionRequests,
    IterativeTaggingRequest,
)
from data_etl_app.models.types_and_enums import ConceptTypeEnum
from data_etl_app.models.skos_concept import Concept
from data_etl_app.models.pipeline_nodes.base.base_node import PipelineContext
from data_etl_app.models.pipeline_nodes.base.base_reconcile_node import (
    ReconcileNode,
)
from data_etl_app.models.pipeline_nodes.multi_stage.concept.concept_phrase_search_node import (
    ConceptPhraseSearchNode,
)
from data_etl_app.models.pipeline_nodes.multi_stage.concept.concept_relationship_node import (
    ConceptRelationshipNode,
)
from data_etl_app.models.pipeline_nodes.multi_stage.concept.concept_relationship_screening_node import (
    ConceptRelationshipScreeningNode,
)
from data_etl_app.models.pipeline_nodes.multi_stage.concept.concept_initial_grounding_node import (
    ConceptInitialGroundingNode,
)
from data_etl_app.models.pipeline_nodes.multi_stage.concept.concept_recursive_grounding_node import (
    ConceptRecursiveGroundingNode,
)
from scraper_app.models.scraped_text_file import ScrapedTextFile

from core.services.manufacturer_service import update_manufacturer
from data_etl_app.services.extraction.deferred_llm_initial_grounding_service import (
    get_tagged_results_from_initial_grounding,
)
from data_etl_app.services.extraction.deferred_llm_recursive_grounding_service import (
    get_phrase_trails,
    get_deepest_concepts_and_oov,
)


from data_etl_app.utils.rdf_to_graph_util import get_match_label_to_concept_map

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
        mfg: Manufacturer,
        deferred_mfg: DeferredManufacturer,
        scraped_text_file: ScrapedTextFile,
        timestamp: datetime,
        pipeline_context: PipelineContext,
        eager: bool,
    ) -> None:

        extraction_requests: Optional[DeferredConceptExtractionRequests] = getattr(
            deferred_mfg, self.field_type.name
        )
        if not extraction_requests:
            raise ValueError(
                f"reconcile was called for {self.field_type.name} but no deferred concept extraction exists."
            )

        completed_phrase_search_req_map = pipeline_context[ConceptPhraseSearchNode]
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
            ConceptRecursiveGroundingNode
        ]  # TODO: do not rely on saving RecursiveTaggingRequest

        all_recognized_tagged_concepts: set[Concept] = set()
        all_unrecognized_tagged_concepts: set[str] = set()
        chunk_stats: ConceptExtractionStatsMap = {}
        for (
            chunk_bounds,
            bundle,
        ) in extraction_requests.chunked_request_map.items():
            iteratively_tagged_concept_nodes = bundle.llm_phrase_recursive_tagging_reqs
            if iteratively_tagged_concept_nodes is None:
                raise ValueError(
                    f"Cannot proceed to reconcile {self.field_type.name} for {mfg.etld1}:{chunk_bounds} "
                    f"as bundle.llm_phrase_recursive_grounding_root_req_nodes is None implying "
                    f"recursive grounding hasn't been executed yet."
                )

            unrecognized_tagged_concepts: set[str] = set()
            recognized_tagged_concepts: set[Concept] = set()

            initially_tagged_trs = await get_tagged_results_from_initial_grounding(
                mfg_etld1=mfg.etld1,
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

            lvl_by_lvl_iterative_grounding_results = await ConceptRecursiveGroundingNode.get_result(
                mfg_etld1=mfg.etld1,
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
                llm_phrase_search=(
                    await ConceptPhraseSearchNode.get_result(
                        mfg_etld1=mfg.etld1,
                        field_type=self.field_type,
                        chunk_bounds=chunk_bounds,
                        extraction_bundle=bundle,
                        completed_request_map=completed_phrase_search_req_map,
                        timestamp=timestamp,
                    )
                ),
                llm_phrase_relationship=(
                    await ConceptRelationshipNode.get_result(
                        mfg_etld1=mfg.etld1,
                        field_type=self.field_type,
                        chunk_bounds=chunk_bounds,
                        extraction_bundle=bundle,
                        timestamp=timestamp,
                        completed_request_map=completed_phrase_relationship_req_map,
                    )
                ),
                llm_phrase_screening=(
                    await ConceptRelationshipScreeningNode.get_result(
                        mfg_etld1=mfg.etld1,
                        field_type=self.field_type,
                        chunk_bounds=chunk_bounds,
                        extraction_bundle=bundle,
                        completed_request_map=completed_relationship_screening_req_map,
                        timestamp=timestamp,
                    )
                ),
                llm_phrase_initial_grounding=(
                    await ConceptInitialGroundingNode.get_result(
                        mfg_etld1=mfg.etld1,
                        field_type=self.field_type,
                        chunk_bounds=chunk_bounds,
                        extraction_bundle=bundle,
                        completed_request_map=completed_initial_grounding_req_map,
                        timestamp=timestamp,
                    )
                ),
                llm_phrase_recursive_grounding=lvl_by_lvl_iterative_grounding_results,
            )

            all_recognized_tagged_concepts.update(recognized_tagged_concepts)
            all_unrecognized_tagged_concepts.update(unrecognized_tagged_concepts)

        final_extraction_result = ConceptExtractionResults(
            metadata=extraction_requests.metadata,
            results=ConceptsFound(
                in_vocab={c.name for c in all_recognized_tagged_concepts},
                out_of_vocab={uc for uc in all_unrecognized_tagged_concepts},
            ),
            chunked_extraction_stats=chunk_stats,
        )

        setattr(mfg, self.field_type.name, final_extraction_result)
        await update_manufacturer(updated_at=timestamp, manufacturer=mfg)

        # call super wipe_down to clear deferred field and completed GPT requests from pipeline context
        await super().wipe_down(
            deferred_mfg=deferred_mfg,
            associated_batch_request_custom_ids=list(
                [
                    *completed_phrase_search_req_map.keys(),
                    *completed_phrase_relationship_req_map.keys(),
                    *completed_relationship_screening_req_map.keys(),
                    *completed_initial_grounding_req_map.keys(),
                    *completed_recursive_grounding_req_map.keys(),
                ]
            ),
        )
