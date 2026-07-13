from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING, Optional

from core.models.db.manufacturer import Manufacturer
from core.models.concept_extraction_results import (
    ConceptsFound,
    ConceptExtractionStatsMap,
    ConceptExtractionStats,
    ConceptExtractionResults,
)
from core.models.db.deferred_manufacturer import DeferredManufacturer
from core.models.deferred_concept_extraction import (
    DeferredConceptExtractionRequests,
    RecursivelyTaggedConceptNode,
)
from data_etl_app.models.types_and_enums import ConceptTypeEnum
from data_etl_app.models.pipeline_nodes.base.base_node import PipelineContext
from data_etl_app.models.pipeline_nodes.base.base_reconcile_node import ReconcileNode
from data_etl_app.models.skos_concept import Concept

if TYPE_CHECKING:
    from scraper_app.models.scraped_text_file import ScrapedTextFile

from core.services.manufacturer_service import update_manufacturer
from data_etl_app.services.extraction.deferred_llm_phrase_search_node_service import (
    parse_batch_request_result as parse_phrase_search_batch_req_result,
)
from data_etl_app.services.extraction.deferred_llm_phrase_relationship_node_service import (
    parse_batch_request_result as parse_phrase_relationhip_batch_req_result,
)
from data_etl_app.services.extraction.deferred_llm_relationship_screening_node_service import (
    parse_batch_request_result as parse_relationhip_screening_batch_req_result,
)
from data_etl_app.services.extraction.deferred_llm_initial_grounding_service import (
    parse_batch_request_result as parse_initial_grounding_batch_req_result,
)
from data_etl_app.services.extraction.deferred_llm_recursive_grounding_service import (
    get_leaf_w_parent_from_tagged_concepts,
    get_tagged_concepts_from_grounding_results,
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
        ]  # TODO: do not rely on saving RecursivelyTaggedConceptNode

        all_recognized_tagged_concepts: set[RecursivelyTaggedConceptNode] = set()
        all_unrecognized_tagged_concepts: set[RecursivelyTaggedConceptNode] = set()
        chunk_stats: ConceptExtractionStatsMap = {}
        for (
            chunk_bounds,
            bundle,
        ) in extraction_requests.chunked_request_map.items():
            recursively_tagged_concept_nodes = (
                bundle.llm_phrase_recursive_grounding_root_req_nodes
            )
            if recursively_tagged_concept_nodes is None:
                raise ValueError(
                    f"Cannot proceed to reconcile {self.field_type.name} for {mfg.etld1}:{chunk_bounds} "
                    f"as bundle.llm_phrase_recursive_grounding_root_req_nodes is None implying "
                    f"recursive grounding hasn't been executed yet."
                )

            recognized_tagged_concepts, unrecognized_tagged_concepts = (
                get_leaf_w_parent_from_tagged_concepts(recursively_tagged_concept_nodes)
            )

            chunk_initial_grounding_results = (
                await (
                    parse_initial_grounding_batch_req_result(
                        mfg_etld1=mfg.etld1,
                        field_type=self.field_type,
                        chunk_bounds=chunk_bounds,
                        extraction_bundle=bundle,
                        completed_request_map=completed_initial_grounding_req_map,
                        deferred_at=timestamp,
                    )
                )
            )
            initially_tagged_vocab_agnostic_concepts_from_all_levels: dict[
                str, RecursivelyTaggedConceptNode
            ] = get_tagged_concepts_from_grounding_results(
                mfg_etld1=mfg.etld1,
                field_type=self.field_type,
                chunk_bounds=chunk_bounds,
                initial=True,
                grounding_results=chunk_initial_grounding_results,
                match_label_to_concept_map=self.match_label_to_concept_map,
                llm_model=extraction_requests.metadata.llm_phrase_initial_grounding.llm_model,
                model_params=extraction_requests.metadata.llm_phrase_initial_grounding.model_params,
                level=None,
            )
            initially_tagged_out_of_vocab_concepts_from_all_levels = {
                tagged_name: node
                for tagged_name, node in initially_tagged_vocab_agnostic_concepts_from_all_levels.items()
                if tagged_name not in self.match_label_to_concept_map
            }

            all_recognized_tagged_concepts |= recognized_tagged_concepts
            all_unrecognized_tagged_concepts |= unrecognized_tagged_concepts
            all_unrecognized_tagged_concepts |= set(
                (initially_tagged_out_of_vocab_concepts_from_all_levels.values())
            )

            chunk_stats[chunk_bounds] = ConceptExtractionStats(
                results=ConceptsFound(
                    in_vocab={c.name for c in recognized_tagged_concepts},
                    out_of_vocab={uc.name for uc in unrecognized_tagged_concepts},
                ),
                brute_search=bundle.brute,
                llm_phrase_search=(
                    await parse_phrase_search_batch_req_result(
                        mfg_etld1=mfg.etld1,
                        field_type=self.field_type,
                        chunk_bounds=chunk_bounds,
                        extraction_bundle=bundle,
                        all_phrase_search_req_responses_map=completed_phrase_search_req_map,
                        deferred_at=timestamp,
                    )
                ),
                llm_phrase_relationship=(
                    await parse_phrase_relationhip_batch_req_result(
                        mfg_etld1=mfg.etld1,
                        field_type=self.field_type,
                        chunk_bounds=chunk_bounds,
                        extraction_bundle=bundle,
                        deferred_at=timestamp,
                        completed_request_map=completed_phrase_relationship_req_map,
                    )
                ),
                llm_phrase_screening=(
                    await parse_relationhip_screening_batch_req_result(
                        mfg_etld1=mfg.etld1,
                        field_type=self.field_type,
                        chunk_bounds=chunk_bounds,
                        extraction_bundle=bundle,
                        completed_request_map=completed_relationship_screening_req_map,
                        deferred_at=timestamp,
                    )
                ),
                llm_phrase_initial_grounding=(
                    await parse_initial_grounding_batch_req_result(
                        mfg_etld1=mfg.etld1,
                        field_type=self.field_type,
                        chunk_bounds=chunk_bounds,
                        extraction_bundle=bundle,
                        completed_request_map=completed_initial_grounding_req_map,
                        deferred_at=timestamp,
                    )
                ),
                llm_phrase_recursive_grounding=recursively_tagged_concept_nodes,
            )

        final_extraction_result = ConceptExtractionResults(
            metadata=extraction_requests.metadata,
            results=ConceptsFound(
                in_vocab={c.name for c in all_recognized_tagged_concepts},
                out_of_vocab={uc.name for uc in all_unrecognized_tagged_concepts},
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
