"""The concept fields' reconcile step on the Step 2 stages (cutover 6b,
2026-09-22). Per chunk it reads the one grounding call, the proposal pass
(when on) and the descent's whole trail — every depth wave's units, verdicts,
descent answers, leaf answers and false children, then the proposal wave —
and DECIDES what ships:

- in vocabulary: per record the deepest accepted label replaces its
  ancestors, each on its own passed verdict (``deepest_accepted_labels``,
  user decision 2026-09-22); a parent whose children were rejected, or never
  screened, stands;
- out of vocabulary: every proposal the proposal wave accepted on at least
  one record;
- every proposal, accepted or not, is written to ``vocabulary_candidates``
  with its records, quotes, sources and verdict.

The trail itself is stored whole (``llm_phrase_descent_trail``) and dumped
level by level, so the decision can be re-read from what the model said.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

from scraper.models.s3.scraped_text_file import ScrapedTextFile

from core.db_models.vocabulary_candidates import VocabularyCandidate
from core.models.deferred_extraction.deferred_concept_extraction import (
    DeferredConceptExtractionRequests,
)
from core.models.extraction_results.concept_extraction_results import ConceptsFound
from core.models.extraction_results.llm_phrase_extraction_results import (
    ConceptExtractionResults,
    ConceptExtractionStats,
    ConceptExtractionStatsMap,
    InitialGroundingStats,
    partition_records_by_search_round,
)
from core.models.extraction_schemas.descent import PROPOSAL_WAVE
from core.models.extraction_schemas.grounding import RecordGroundingResults
from core.models.extraction_schemas.run_provenance import build_run_provenance_record
from core.models.extraction_schemas.stored_fold import build_stored_fold
from core.models.extraction_subject import (
    AbstractDeferredExtractionSubject,
    AbstractExtractionSubject,
)
from core.models.field_types import ConceptFieldType
from core.models.pipeline_nodes.base.base_node import PipelineContext
from core.models.pipeline_nodes.base.base_reconcile_node import ReconcileNode
from core.models.pipeline_nodes.multi_stage.concept.concept_descent_node import (
    ConceptDescentNode,
)
from core.models.pipeline_nodes.multi_stage.concept.concept_grounding_node import (
    ConceptGroundingNode,
)
from core.models.pipeline_nodes.multi_stage.concept.concept_phrase_search_node import (
    ConceptPhraseSearchNode,
)
from core.models.pipeline_nodes.multi_stage.concept.concept_proposal_node import (
    ConceptProposalNode,
)
from core.models.pipeline_nodes.multi_stage.concept.concept_recursive_search_node import (
    ConceptRecursiveSearchNode,
)
from core.models.pipeline_nodes.multi_stage.concept.concept_synthesis_node import (
    ConceptSynthesisNode,
)
from core.models.skos_concept import Concept
from core.services.extraction_run_service import save_extraction_run
from core.services.pipeline_nodes.multi_stage.aggregation_fold_service import (
    fold_collapse_compounds_of,
    fold_snippet_radius_of,
    fold_verb_fold_of,
)
from core.services.pipeline_nodes.multi_stage.llm_descent_node_service import (
    Vocabulary,
    accepted_proposals,
    chunk_inputs_from,
    leaf_step_view,
    merged_screening,
    read_descent_trail,
    shipped_labels_by_record,
    split_grounding_results,
    vocabulary_candidates_from,
)
from core.services.pipeline_nodes.multi_stage.llm_phrase_recursive_search_node_service import (
    build_llm_phrase_search_results,
)
from core.services.pipeline_nodes.multi_stage.llm_phrase_synthesis_node_service import (
    downstream_group_records,
    get_chunk_synthesis_result,
    synthesis_max_entries_of,
)
from core.services.vocabulary_candidate_service import save_vocabulary_candidates
from core.utils.extraction_dump_util import (
    build_concept_group_rows,
    build_run_provenance,
    write_extraction_dump,
)
from core.utils.fold_dump_util import build_fold_dump
from core.utils.label_dedupe_util import dedupe_case_insensitive
from core.utils.synthesis_dump_util import build_synthesis_dump

logger = logging.getLogger(__name__)


class ConceptReconcileNode(ReconcileNode[ConceptFieldType]):
    def __init__(
        self,
        concept_type: ConceptFieldType,
        known_concepts: set[Concept],
    ):
        super().__init__(field_type=concept_type)
        self.known_concepts = known_concepts
        self.vocab = Vocabulary(known_concepts)
        self.match_label_to_concept_map = self.vocab.match_label_to_concept_map

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
        metadata = extraction_requests.metadata

        completed_phrase_search_req_map = pipeline_context[ConceptPhraseSearchNode]
        completed_recursive_search_req_map = pipeline_context[ConceptRecursiveSearchNode]
        completed_synthesis_req_map = pipeline_context[ConceptSynthesisNode]
        completed_grounding_req_map = pipeline_context[ConceptGroundingNode]
        completed_proposal_req_map = pipeline_context[ConceptProposalNode]
        completed_descent_req_map = pipeline_context[ConceptDescentNode]
        proposal_pass_ran = metadata.llm_phrase_proposal is not None

        all_in_vocab_results: set[str] = set()
        all_out_of_vocab_results: set[str] = set()
        candidates: list[VocabularyCandidate] = []
        chunk_stats: ConceptExtractionStatsMap = {}
        chunked_dump_contents: dict[str, dict[str, object]] = {}
        for chunk_bounds, bundle in extraction_requests.chunked_request_map.items():
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

            # v3 (3.3, D16): the chunk's synthesis result — the fold recomputed
            # from the text and stored forms, the held syntheses — is the spine
            # every downstream verdict keys against.
            synthesis_result = await get_chunk_synthesis_result(
                subject_unique_id=subject.subject_unique_id,
                field_type=self.field_type,
                chunk_bounds=chunk_bounds,
                extraction_bundle=bundle,
                completed_request_map=completed_synthesis_req_map,
                timestamp=timestamp,
                subject_text=scraped_text_file.text,
                verb_fold=fold_verb_fold_of(metadata),
                snippet_radius=fold_snippet_radius_of(metadata),
                collapse_compounds=fold_collapse_compounds_of(metadata),
            )
            group_records = downstream_group_records(synthesis_result)
            focal_by_group = {
                group_id: record.focal_form
                for group_id, record in group_records.items()
            }

            # The one grounding call: vocabulary matches and proposals in one
            # per-record map, split here into the two stat buckets.
            grounding_flat = await ConceptGroundingNode.get_result(
                subject_unique_id=subject.subject_unique_id,
                field_type=self.field_type,
                chunk_bounds=chunk_bounds,
                extraction_bundle=bundle,
                completed_request_map=completed_grounding_req_map,
                timestamp=timestamp,
                allowed_labels=self.vocab.allowed_labels(),
            )
            # The proposal pass (run flag): the records the call left with no
            # label, read again. An OFF pass reads as an empty map.
            proposal_flat: RecordGroundingResults = (
                await ConceptProposalNode.get_result(
                    subject_unique_id=subject.subject_unique_id,
                    field_type=self.field_type,
                    chunk_bounds=chunk_bounds,
                    extraction_bundle=bundle,
                    completed_request_map=completed_proposal_req_map,
                    timestamp=timestamp,
                    allowed_labels=self.vocab.allowed_labels(),
                )
                if proposal_pass_ran
                else {}
            )
            grounding_in_vocab, grounding_proposals = split_grounding_results(self.vocab, grounding_flat)
            pass_in_vocab, pass_proposals = split_grounding_results(self.vocab, proposal_flat)
            # Everything proposed before descent, for the stats' proposal bucket.
            proposals_before_descent: RecordGroundingResults = dict(grounding_proposals)
            for rid, entry in pass_proposals.items():
                if rid in proposals_before_descent:
                    proposals_before_descent[rid].tags.update(entry.tags)
                else:
                    proposals_before_descent[rid] = entry

            # The descent's whole trail, level by level, re-read from the
            # answers exactly as the descent node read them.
            inputs = chunk_inputs_from(
                self.vocab, group_records,
                [("grounding", grounding_flat), *([("proposal_pass", proposal_flat)] if proposal_pass_ran else [])],
            )
            trail = await read_descent_trail(
                vocab=self.vocab,
                subject_unique_id=subject.subject_unique_id,
                field_name=self.field_type.name,
                inputs=inputs,
                screens=bundle.llm_phrase_unit_screening_req_ids,
                descents=bundle.llm_phrase_descent_reqs,
                completed=completed_descent_req_map,
                timestamp=timestamp,
            )

            # THE DECISION. In vocabulary: the deepest accepted label per
            # record replaces its ancestors. Out of vocabulary: the proposals
            # the proposal wave accepted. Both re-derivable from the trail.
            shipped_by_record = shipped_labels_by_record(self.vocab, trail)
            in_vocab_labels = {label for labels in shipped_by_record.values() for label in labels}
            out_of_vocab_labels = accepted_proposals(trail)

            candidates.extend(vocabulary_candidates_from(
                trail=trail, group_records=group_records,
                grounding_proposals=[grounding_proposals, pass_proposals],
                subject_unique_id=subject.subject_unique_id, field_name=self.field_type.name,
                run_timestamp=timestamp, ontology_version_id=metadata.ontology_version_id, created_at=timestamp,
            ))

            # Guarded dump blocks (a diagnostic must not sink the run).
            try:
                synthesis_dump: Optional[dict[str, object]] = build_synthesis_dump(
                    synthesis_result,
                    subject_name=pipeline_context.subject_name,
                    max_entries_per_request=synthesis_max_entries_of(metadata),
                )
            except Exception as synthesis_dump_error:
                logger.error(
                    f"[{subject.subject_unique_id}] full-run dump could not build the "
                    f"synthesis block of chunk {chunk_bounds} of "
                    f"'{self.field_type.name}': {synthesis_dump_error}",
                    exc_info=True,
                )
                synthesis_dump = None
            try:
                fold_dump: Optional[dict[str, object]] = build_fold_dump(
                    synthesis_result.fold, subject_name=pipeline_context.subject_name
                )
            except Exception as fold_dump_error:
                logger.error(
                    f"[{subject.subject_unique_id}] full-run dump could not build the "
                    f"fold block of chunk {chunk_bounds} of "
                    f"'{self.field_type.name}': {fold_dump_error}",
                    exc_info=True,
                )
                fold_dump = None

            # The persisted twin of that same fold: NOT guarded (a window whose
            # bounds do not describe its own text must fail the run).
            stored_fold = build_stored_fold(
                synthesis_result.fold,
                text_version_id=scraped_text_file.s3_version_id,
            )

            # The rows: the grounding call's matches, the proposals (the call's
            # and the pass's), and every verdict of every wave on the record.
            # The trail is dumped whole beside them, level by level (6c renders
            # it into the rows).
            chunked_dump_contents[chunk_bounds] = {
                "rows": build_concept_group_rows(
                    synthesis_result=synthesis_result,
                    in_vocab_flat=grounding_in_vocab,
                    oov_flat=proposals_before_descent,
                    screening_flat=merged_screening(trail),
                    phrase_trails=[],
                    search_rounds=llm_search_results,
                    match_label_to_concept_map=self.match_label_to_concept_map,
                    subject_name=pipeline_context.subject_name,
                ),
                "descent_trail": trail.model_dump(mode="json"),
                "shipped": {
                    "in_vocab_by_record": shipped_by_record,
                    "out_of_vocab": sorted(out_of_vocab_labels),
                },
                "fold": fold_dump,
                "synthesis": synthesis_dump,
            }

            chunk_stats[chunk_bounds] = ConceptExtractionStats(
                results=ConceptsFound(in_vocab=set(in_vocab_labels), out_of_vocab=set(out_of_vocab_labels)),
                brute_search=bundle.brute,
                aggregation_fold=stored_fold,
                llm_phrase_search=llm_search_results,
                llm_phrase_synthesis=partition_records_by_search_round(
                    group_records, focal_by_group, llm_search_results
                ),
                llm_phrase_grounding=InitialGroundingStats(
                    in_vocab=partition_records_by_search_round(
                        grounding_in_vocab, focal_by_group, llm_search_results
                    ),
                    out_of_vocab=partition_records_by_search_round(
                        proposals_before_descent, focal_by_group, llm_search_results
                    ),
                ),
                llm_phrase_proposal=partition_records_by_search_round(
                    {**pass_in_vocab, **{rid: e for rid, e in proposal_flat.items() if rid not in pass_in_vocab}},
                    focal_by_group, llm_search_results,
                ) if proposal_pass_ran else {},
                llm_phrase_unit_screening=(
                    {1: trail.waves[1].screening} if 1 in trail.waves else {}
                ),
                llm_phrase_descent_screening={
                    **{wave: wt.screening for wave, wt in trail.waves.items()},
                    PROPOSAL_WAVE: trail.proposal_screening,
                },
                llm_phrase_leaf_step=leaf_step_view(trail),
                llm_phrase_descent_trail=trail,
            )

            all_in_vocab_results.update(in_vocab_labels)
            all_out_of_vocab_results.update(out_of_vocab_labels)

        stored_run_provenance = build_run_provenance_record(
            run_timestamp=timestamp,
            scraped_text_file=scraped_text_file,
            page_exclusion=pipeline_context.page_exclusion,
        )

        write_extraction_dump(
            subject_unique_id=subject.subject_unique_id,
            field_type=self.field_type,
            timestamp=timestamp,
            chunked_contents=chunked_dump_contents,
            chunked_request_map=extraction_requests.chunked_request_map,
            completed_requests={
                **completed_phrase_search_req_map,
                **completed_recursive_search_req_map,
                **completed_synthesis_req_map,
                **completed_grounding_req_map,
                **completed_proposal_req_map,
                **completed_descent_req_map,
            },
            run_provenance=build_run_provenance(
                metadata=metadata,
                scraped_text_file=scraped_text_file,
                partial=False,
                page_exclusion=pipeline_context.page_exclusion,
            ),
        )

        final_extraction_result = ConceptExtractionResults(
            metadata=metadata,
            run_provenance=stored_run_provenance,
            results=ConceptsFound(
                in_vocab=all_in_vocab_results,
                # Chunks propose independently, so the union carries case
                # variants of one label; per-chunk stats keep them raw.
                out_of_vocab=dedupe_case_insensitive(all_out_of_vocab_results),
            ),
            chunked_extraction_stats=chunk_stats,
        )

        # The RECORD first, the candidates second, the subject's CACHE last:
        # a crash between them leaves a record with a stale cache, which the
        # next run repairs, rather than a cache with no record.
        await save_extraction_run(
            subject_unique_id=subject.subject_unique_id,
            field_name=self.field_type.name,
            field_family="concept",
            run_timestamp=timestamp,
            run_provenance=stored_run_provenance,
            results=final_extraction_result,
        )
        await save_vocabulary_candidates(candidates)

        setattr(subject, self.field_type.name, final_extraction_result)
        await subject.record_update(updated_at=timestamp)

        # call super wipe_down to clear deferred field and completed GPT requests from pipeline context
        await super().wipe_down(
            deferred_subject=deferred_subject,
            associated_batch_request_custom_ids=list(
                [
                    *completed_phrase_search_req_map.keys(),
                    *completed_recursive_search_req_map.keys(),
                    *completed_synthesis_req_map.keys(),
                    *completed_grounding_req_map.keys(),
                    *completed_proposal_req_map.keys(),
                    *completed_descent_req_map.keys(),
                ]
            ),
        )
