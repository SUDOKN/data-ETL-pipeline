from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

from core.models.extraction_subject import (
    AbstractExtractionSubject,
    AbstractDeferredExtractionSubject,
)
from core.models.extraction_results.concept_extraction_results import (
    ConceptsFound,
)
from core.models.extraction_results.llm_phrase_extraction_results_v2 import (
    ConceptExtractionResultsV2,
    ConceptExtractionStatsMapV2,
    ConceptExtractionStatsV2,
    InitialGroundingStats,
    partition_records_by_search_round,
)
from core.models.deferred_extraction.deferred_concept_extraction import (
    DeferredConceptExtractionRequests,
)
from core.models.extraction_schemas.grounding import RecordGroundingResults
from core.models.field_types import ConceptFieldType
from core.models.rule_catalog import (
    STAGE_INITIAL_GROUNDING,
    STAGE_OOV_GROUNDING,
)
from core.models.skos_concept import Concept
from core.models.pipeline_nodes.base.base_node import PipelineContext
from core.models.pipeline_nodes.base.base_reconcile_node import (
    ReconcileNode,
)
from core.models.pipeline_nodes.multi_stage.concept.concept_phrase_search_node import (
    ConceptPhraseSearchNode,
)
from core.models.pipeline_nodes.multi_stage.concept.concept_recursive_search_node import (
    ConceptRecursiveSearchNode,
)
from core.models.pipeline_nodes.multi_stage.concept.concept_relationship_node import (
    ConceptRelationshipNode,
)
from core.models.pipeline_nodes.multi_stage.concept.concept_relationship_screening_node import (
    ConceptRelationshipScreeningNode,
)
from core.models.pipeline_nodes.multi_stage.concept.concept_initial_grounding_node import (
    ConceptInitialGroundingNode,
)
from core.models.pipeline_nodes.multi_stage.concept.concept_oov_grounding_node import (
    ConceptOovGroundingNode,
)
from core.models.pipeline_nodes.multi_stage.concept.concept_iterative_grounding_node import (
    ConceptIterativeGroundingNode,
)
from scraper.models.s3.scraped_text_file import ScrapedTextFile

from core.services.pipeline_nodes.multi_stage.llm_phrase_recursive_search_node_service import (
    build_llm_phrase_search_results,
)
from core.services.pipeline_nodes.multi_stage.llm_grounding_node_service import (
    get_record_grounding_result,
)
from core.services.pipeline_nodes.multi_stage.llm_recursive_grounding_service import (
    get_phrase_trails,
    get_deepest_concepts_and_oov,
)
from core.services.pipeline_nodes.multi_stage.pipeline_v2_derivations import (
    candidates_that_passed,
)
from core.services.rule_catalog_registry import get_rule_catalog

from core.utils.label_dedupe_util import dedupe_case_insensitive
from core.utils.rdf_to_graph_util import (
    get_match_label_to_concept_map,
)
from core.utils.record_id_util import phrases_by_record_id
from core.utils.extraction_dump_util import (
    build_concept_record_rows,
    build_run_provenance,
    write_extraction_dump,
)

logger = logging.getLogger(__name__)


class ConceptReconcileNode(ReconcileNode[ConceptFieldType]):
    def __init__(
        self,
        concept_type: ConceptFieldType,
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
        if await self.stop_if_stage_disabled(
            subject=subject,
            deferred_subject=deferred_subject,
            scraped_text_file=scraped_text_file,
            timestamp=timestamp,
            pipeline_context=pipeline_context,
        ):
            return

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
        completed_in_vocab_grounding_req_map = pipeline_context[
            ConceptInitialGroundingNode
        ]
        completed_oov_grounding_req_map = pipeline_context[ConceptOovGroundingNode]
        completed_screening_req_map = pipeline_context[
            ConceptRelationshipScreeningNode
        ]
        completed_recursive_grounding_req_map = pipeline_context[
            ConceptIterativeGroundingNode
        ]

        vocab_labels = list(self.match_label_to_concept_map.keys())
        in_vocab_catalog = get_rule_catalog(
            STAGE_INITIAL_GROUNDING, self.field_type.name
        )
        oov_catalog = get_rule_catalog(STAGE_OOV_GROUNDING, self.field_type.name)

        all_in_vocab_results: set[str] = set()
        all_out_of_vocab_results: set[str] = set()
        chunk_stats: ConceptExtractionStatsMapV2 = {}
        chunked_dump_contents: dict[str, dict[str, object]] = {}
        for (
            chunk_bounds,
            bundle,
        ) in extraction_requests.chunked_request_map.items():
            if bundle.llm_phrase_recursive_tagging_reqs is None:
                raise ValueError(
                    f"Cannot proceed to reconcile {self.field_type.name} for {subject.subject_unique_id}:{chunk_bounds} "
                    f"as bundle.llm_phrase_recursive_tagging_reqs is None implying "
                    f"recursive grounding hasn't been executed yet."
                )

            llm_search_results = await build_llm_phrase_search_results(
                subject_unique_id=subject.subject_unique_id,
                field_type=self.field_type,
                chunk_bounds=chunk_bounds,
                extraction_bundle=bundle,
                completed_search_req_map=completed_phrase_search_req_map,
                completed_recursive_search_req_map=completed_recursive_search_req_map,
                timestamp=timestamp,
                brute_search_results=bundle.brute,
            )

            # The relationship stage is the one place the model still echoes
            # phrases back, so it keeps a repairs sink; every record-keyed
            # stage downstream holds exactly and has none.
            relationship_repairs: dict[str, str] = {}
            masked_flat = await ConceptRelationshipNode.get_result(
                subject_unique_id=subject.subject_unique_id,
                field_type=self.field_type,
                chunk_bounds=chunk_bounds,
                extraction_bundle=bundle,
                completed_request_map=completed_phrase_relationship_req_map,
                timestamp=timestamp,
                repairs=relationship_repairs,
            )
            phrase_by_record_id = phrases_by_record_id(masked_flat)

            in_vocab_flat = await get_record_grounding_result(
                stage_label="in-vocab grounding",
                subject_unique_id=subject.subject_unique_id,
                field_name=self.field_type.name,
                chunk_bounds=chunk_bounds,
                catalog=in_vocab_catalog,
                group_req_ids=bundle.llm_phrase_initial_grounding_req_ids,
                completed_request_map=completed_in_vocab_grounding_req_map,
                timestamp=timestamp,
                allowed_labels=vocab_labels,
            )
            # An OFF OOV pass embedded no ids; None here keeps "never asked"
            # distinguishable from "asked, found nothing" in stats and dump.
            oov_ran = bool(bundle.llm_phrase_oov_grounding_req_ids)
            oov_flat: RecordGroundingResults = (
                await get_record_grounding_result(
                    stage_label="oov grounding",
                    subject_unique_id=subject.subject_unique_id,
                    field_name=self.field_type.name,
                    chunk_bounds=chunk_bounds,
                    catalog=oov_catalog,
                    group_req_ids=bundle.llm_phrase_oov_grounding_req_ids,
                    completed_request_map=completed_oov_grounding_req_map,
                    timestamp=timestamp,
                )
                if oov_ran
                else {}
            )

            screening_flat = await ConceptRelationshipScreeningNode.get_result(
                subject_unique_id=subject.subject_unique_id,
                field_type=self.field_type,
                chunk_bounds=chunk_bounds,
                extraction_bundle=bundle,
                completed_request_map=completed_screening_req_map,
                timestamp=timestamp,
            )

            lvl_by_lvl_iterative_grounding_results = await ConceptIterativeGroundingNode.get_result(
                subject_unique_id=subject.subject_unique_id,
                field_type=self.field_type,
                chunk_bounds=chunk_bounds,
                extraction_bundle=bundle,
                completed_in_vocab_grounding_req_map=completed_in_vocab_grounding_req_map,
                completed_screening_req_map=completed_screening_req_map,
                completed_recursive_grounding_req_map=completed_recursive_grounding_req_map,
                match_label_to_concept_map=self.match_label_to_concept_map,
                timestamp=timestamp,
            )
            phrase_trails = get_phrase_trails(
                lvl_by_lvl_iterative_grounding_results=lvl_by_lvl_iterative_grounding_results
            )

            # Results per fork F10. in_vocab = passed screening then deepened by
            # the descent (the trails' deepest concepts); out_of_vocab = passed
            # OOV candidates, plus the descent's own sibling proposals. An OOV
            # candidate that restates a vocabulary label re-routes in-vocab via
            # the existing label map — never raised, never persisted as a fake
            # ontology gap.
            recognized_tagged_concepts: set[Concept] = set()
            unrecognized_tagged_concepts: set[str] = set()
            for phrase_trail in phrase_trails:
                recognized_deepest_concepts, descent_oov = get_deepest_concepts_and_oov(
                    phrase_trail=phrase_trail,
                    match_label_to_concept_map=self.match_label_to_concept_map,
                )
                recognized_tagged_concepts.update(recognized_deepest_concepts)
                unrecognized_tagged_concepts.update(descent_oov)
            for passed_candidate in candidates_that_passed(oov_flat, screening_flat):
                concept = self.match_label_to_concept_map.get(passed_candidate)
                if concept is not None:
                    recognized_tagged_concepts.add(concept)
                else:
                    unrecognized_tagged_concepts.add(passed_candidate)

            chunked_dump_contents[chunk_bounds] = {
                "rows": build_concept_record_rows(
                    masked_flat=masked_flat,
                    in_vocab_flat=in_vocab_flat,
                    oov_flat=oov_flat if oov_ran else None,
                    screening_flat=screening_flat,
                    phrase_trails=phrase_trails,
                    search_rounds=llm_search_results,
                    match_label_to_concept_map=self.match_label_to_concept_map,
                    relationship_repairs=relationship_repairs,
                    subject_name=pipeline_context.subject_name,
                )
            }

            chunk_stats[chunk_bounds] = ConceptExtractionStatsV2(
                results=ConceptsFound(
                    in_vocab={c.name for c in recognized_tagged_concepts},
                    out_of_vocab={uc for uc in unrecognized_tagged_concepts},
                ),
                brute_search=bundle.brute,
                llm_phrase_search=llm_search_results,
                llm_phrase_relationship=partition_records_by_search_round(
                    masked_flat, phrase_by_record_id, llm_search_results
                ),
                llm_phrase_screening=partition_records_by_search_round(
                    screening_flat, phrase_by_record_id, llm_search_results
                ),
                llm_phrase_initial_grounding=InitialGroundingStats(
                    in_vocab=partition_records_by_search_round(
                        in_vocab_flat, phrase_by_record_id, llm_search_results
                    ),
                    out_of_vocab=partition_records_by_search_round(
                        oov_flat, phrase_by_record_id, llm_search_results
                    ),
                ),
                llm_phrase_recursive_grounding=lvl_by_lvl_iterative_grounding_results,
            )

            all_in_vocab_results.update(c.name for c in recognized_tagged_concepts)
            all_out_of_vocab_results.update(unrecognized_tagged_concepts)

        write_extraction_dump(
            subject_unique_id=subject.subject_unique_id,
            field_type=self.field_type,
            timestamp=timestamp,
            chunked_contents=chunked_dump_contents,
            chunked_request_map=extraction_requests.chunked_request_map,
            completed_requests={
                **completed_phrase_search_req_map,
                **completed_recursive_search_req_map,
                **completed_phrase_relationship_req_map,
                **completed_in_vocab_grounding_req_map,
                **completed_oov_grounding_req_map,
                **completed_screening_req_map,
                **completed_recursive_grounding_req_map,
            },
            run_provenance=build_run_provenance(
                metadata=extraction_requests.metadata,
                scraped_text_file=scraped_text_file,
                partial=False,
            ),
        )

        final_extraction_result = ConceptExtractionResultsV2(
            metadata=extraction_requests.metadata,
            results=ConceptsFound(
                in_vocab=all_in_vocab_results,
                # Chunks propose out-of-vocab labels independently, so the union
                # carries case variants of one label; per-chunk stats keep them raw.
                out_of_vocab=dedupe_case_insensitive(all_out_of_vocab_results),
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
                    *completed_in_vocab_grounding_req_map.keys(),
                    *completed_oov_grounding_req_map.keys(),
                    *completed_screening_req_map.keys(),
                    *completed_recursive_grounding_req_map.keys(),
                ]
            ),
        )
