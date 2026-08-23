"""The dump a stage-gated run writes when it stops short of reconcile.

The full extraction dump is built by the reconcile node, which a stopped run
never reaches — so without this, switching a stage off would buy you a cheaper
run and nothing to read from it. This walks the stages that DID publish a
completed request map and emits the same per-chunk shape: per-record rows for
the multi-stage pipelines (minus the verdict fields only a finished chain can
justify), the parsed result for a single-stage field.

Every stage that can precede a stop point shares one ``get_result`` signature,
which is what makes the walk generic. Iterative grounding is the single
exception (it takes three request maps and the ontology's label map), and it
is also the only stage that cannot sit before a stop point in practice —
stopping after it means everything but reconcile ran. It is reported in the
header as having run, without its detail in the rows.

The grounding stages here parse WITHOUT the vocabulary hold (the walk has node
classes, not instances, and no ontology in hand), so an in-vocab label appears
exactly as the model wrote it — for a diagnostic that is a feature, not a gap.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional, Protocol

from core.models.extraction_schemas.grounding import RecordGroundingResults
from core.models.extraction_schemas.relationship import (
    MaskedLLMPhraseRelationshipResults,
)
from core.models.extraction_schemas.screening import RecordScreeningResults
from core.models.extraction_schemas.search import LLMSearchResults
from core.models.field_types import ExtractionFieldType
from core.models.pipeline_nodes.base.base_node import PipelineContext
from core.models.pipeline_nodes.base.pipeline_stage import PipelineStage
from core.services.pipeline_nodes.multi_stage.llm_phrase_recursive_search_node_service import (
    build_llm_phrase_search_results,
)
from core.utils.extraction_dump_util import (
    build_partial_record_rows,
    build_run_provenance,
    jsonable_result,
    write_extraction_dump,
)
from core.services.pipeline_nodes.multi_stage.llm_phrase_mention_collection_node_service import (
    fold_verb_fold_of,
)
from core.utils.fold_dump_util import build_fold_dump
from scraper.models.s3.scraped_text_file import ScrapedTextFile

logger = logging.getLogger(__name__)

# Stage value -> the row key its results are dumped under. Both concept
# grounding passes and the keyword one can precede a stop point, and more than
# one can have run — each present stage gets its own key on the row.
_GROUNDING_STAGE_ROW_KEYS = {
    PipelineStage.initial_grounding: "in_vocab_grounding",
    PipelineStage.oov_grounding: "oov_grounding",
    PipelineStage.freehand_grounding: "freehand_grounding",
}


class _HasChunkedRequestMap(Protocol):
    """The shape this dump needs off any deferred-extraction requests object."""

    @property
    def chunked_request_map(self) -> dict: ...

    @property
    def metadata(self) -> object: ...


async def write_partial_run_dump(
    *,
    subject_unique_id: str,
    field_type: ExtractionFieldType,
    stopped_at: PipelineStage,
    disabled_stages: set[PipelineStage],
    extraction_requests: _HasChunkedRequestMap,
    scraped_text_file: ScrapedTextFile,
    pipeline_context: PipelineContext,
    timestamp: datetime,
) -> None:
    """Dump whatever the stages that ran produced, per chunk.

    Never raises: a dump is a diagnostic, and a run that already stopped on
    purpose must not then fail on the way out and lose the stages it did
    complete. Parse failures are logged and the stage is dropped from the rows.
    """
    try:
        completed_by_stage = {
            stage: (node_class, pipeline_context[node_class])
            for stage, node_class in pipeline_context.stages_completed
        }
        stages_run = [stage.value for stage, _node_class in pipeline_context.stages_completed]
        # One flat lookup across every completed stage, for pricing the dump:
        # custom_ids are globally unique, so merging cannot collide.
        completed_requests = {
            custom_id: request_doc
            for _node_class, request_map in completed_by_stage.values()
            for custom_id, request_doc in request_map.items()
        }

        chunked_contents: dict[str, dict[str, object]] = {}
        for chunk_bounds, bundle in extraction_requests.chunked_request_map.items():
            # A single-stage field has no phrase stages at all — its whole
            # pipeline is one request per chunk, so the chunk's content is the
            # parsed result rather than rows.
            if PipelineStage.single_stage_extraction in completed_by_stage:
                node_class, request_map = completed_by_stage[
                    PipelineStage.single_stage_extraction
                ]
                try:
                    result = await node_class.get_result(
                        subject_unique_id=subject_unique_id,
                        field_type=field_type,
                        chunk_bounds=chunk_bounds,
                        extraction_bundle=bundle,
                        completed_request_map=request_map,
                        timestamp=timestamp,
                    )
                    chunked_contents[chunk_bounds] = {
                        "result": jsonable_result(result)
                    }
                except Exception as parse_error:
                    # The dump must still say the request ran; a null result
                    # with a note beats losing the whole file.
                    logger.error(
                        f"[{subject_unique_id}] partial dump could not parse the "
                        f"single-stage result for '{field_type.name}' chunk "
                        f"{chunk_bounds}: {parse_error}",
                        exc_info=True,
                    )
                    chunked_contents[chunk_bounds] = {
                        "result": None,
                        "note": "result_parse_failed",
                    }
                continue
            search_rounds: dict[int, LLMSearchResults] = {}
            if PipelineStage.phrase_search in completed_by_stage:
                _search_node, search_map = completed_by_stage[
                    PipelineStage.phrase_search
                ]
                _recursive_node, recursive_map = completed_by_stage.get(
                    PipelineStage.recursive_search, (None, {})
                )
                search_rounds = await build_llm_phrase_search_results(
                    subject_unique_id=subject_unique_id,
                    field_type=field_type,
                    chunk_bounds=chunk_bounds,
                    extraction_bundle=bundle,
                    completed_search_req_map=search_map,
                    completed_recursive_search_req_map=recursive_map,
                    timestamp=timestamp,
                    # Concept bundles carry brute-search survivors at round 0;
                    # keyword bundles have no brute phase and no such field.
                    brute_search_results=getattr(bundle, "brute", None),
                )

            # One sink for the relationship stage's phrase repairs — the one
            # stage left where the model echoes a phrase back; every
            # record-keyed stage downstream holds exactly, so it has none.
            relationship_repairs: dict[str, str] = {}

            masked_flat: Optional[MaskedLLMPhraseRelationshipResults] = None
            if PipelineStage.relationship in completed_by_stage:
                node_class, request_map = completed_by_stage[PipelineStage.relationship]
                masked_flat = await node_class.get_result(
                    subject_unique_id=subject_unique_id,
                    field_type=field_type,
                    chunk_bounds=chunk_bounds,
                    extraction_bundle=bundle,
                    completed_request_map=request_map,
                    timestamp=timestamp,
                    repairs=relationship_repairs,
                )

            grounding_by_stage: dict[str, Optional[RecordGroundingResults]] = {}
            for stage, row_key in _GROUNDING_STAGE_ROW_KEYS.items():
                if stage not in completed_by_stage:
                    continue
                node_class, request_map = completed_by_stage[stage]
                grounding_by_stage[row_key] = await node_class.get_result(
                    subject_unique_id=subject_unique_id,
                    field_type=field_type,
                    chunk_bounds=chunk_bounds,
                    extraction_bundle=bundle,
                    completed_request_map=request_map,
                    timestamp=timestamp,
                )

            screening_flat: Optional[RecordScreeningResults] = None
            if PipelineStage.screening in completed_by_stage:
                node_class, request_map = completed_by_stage[PipelineStage.screening]
                screening_flat = await node_class.get_result(
                    subject_unique_id=subject_unique_id,
                    field_type=field_type,
                    chunk_bounds=chunk_bounds,
                    extraction_bundle=bundle,
                    completed_request_map=request_map,
                    timestamp=timestamp,
                )

            contents: dict[str, object] = {
                "rows": build_partial_record_rows(
                    search_rounds=search_rounds,
                    masked_flat=masked_flat,
                    grounding_by_stage=grounding_by_stage,
                    screening_flat=screening_flat,
                    relationship_repairs=relationship_repairs,
                    subject_name=pipeline_context.subject_name,
                )
            }
            # v3: the mention stage's aggregation fold — groups with member
            # forms inline, mentions in locked order, and the per-window hold.
            # Computed here from the completed map and the text (the fold is
            # code, not a request), so every re-run of a stopped chain shows the
            # CURRENT fold rules over the stored answers.
            if PipelineStage.mention_collection in completed_by_stage:
                node_class, request_map = completed_by_stage[PipelineStage.mention_collection]
                try:
                    fold_result = await node_class.get_result(
                        subject_unique_id=subject_unique_id,
                        field_type=field_type,
                        chunk_bounds=chunk_bounds,
                        extraction_bundle=bundle,
                        completed_request_map=request_map,
                        timestamp=timestamp,
                        subject_text=scraped_text_file.text,
                        verb_fold=fold_verb_fold_of(extraction_requests.metadata),
                    )
                    contents["fold"] = build_fold_dump(
                        fold_result, subject_name=pipeline_context.subject_name
                    )
                except Exception as fold_error:
                    logger.error(
                        f"[{subject_unique_id}] partial dump could not fold chunk "
                        f"{chunk_bounds} of '{field_type.name}': {fold_error}",
                        exc_info=True,
                    )
                    contents["fold"] = None
                    contents["note"] = "fold_failed"
            chunked_contents[chunk_bounds] = contents

        write_extraction_dump(
            subject_unique_id=subject_unique_id,
            field_type=field_type,
            timestamp=timestamp,
            chunked_contents=chunked_contents,
            chunked_request_map=extraction_requests.chunked_request_map,
            completed_requests=completed_requests,
            run_provenance=build_run_provenance(
                metadata=extraction_requests.metadata,
                scraped_text_file=scraped_text_file,
                partial=True,
                stopped_at=stopped_at.value,
                stages_run=stages_run,
                stages_disabled=sorted(stage.value for stage in disabled_stages),
                page_exclusion=pipeline_context.page_exclusion,
            ),
            name_suffix="__partial",
        )
    except Exception as error:  # diagnostics must not sink the run
        logger.error(
            f"[{subject_unique_id}] Failed to write partial run dump for "
            f"'{field_type.name}' stopped at {stopped_at.value}: {error}",
            exc_info=True,
        )
