"""The dump a stage-gated run writes when it stops short of reconcile.

The full extraction dump is built by the reconcile node, which a stopped run
never reaches — so without this, switching a stage off would buy you a cheaper
run and nothing to read from it. This walks the stages that DID publish a
completed request map and emits the same per-chunk shape: per-phrase rows for
the multi-stage pipelines (minus the verdict fields only a finished chain can
justify), the parsed result for a single-stage field.

Every stage that can precede a stop point shares one ``get_result`` signature,
which is what makes the walk generic. Iterative grounding is the single
exception (it takes two request maps and the ontology's label map), and it is
also the only stage that cannot sit before a stop point in practice — stopping
after it means everything but reconcile ran. It is reported in the header as
having run, without its detail in the rows.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional, Protocol

from core.models.extraction_schemas.grounding import PhraseToTagAndRulesMap
from core.models.extraction_schemas.relationship import (
    LLMPhraseRelationshipResults,
)
from core.models.extraction_schemas.screening import LiveScreeningResults
from core.models.extraction_schemas.search import LLMSearchResults
from core.models.field_types import ExtractionFieldType
from core.models.pipeline_nodes.base.base_node import PipelineContext
from core.models.pipeline_nodes.base.pipeline_stage import PipelineStage
from core.services.pipeline_nodes.multi_stage.llm_phrase_recursive_search_node_service import (
    build_llm_phrase_search_results,
)
from core.utils.extraction_dump_util import (
    build_partial_phrase_rows,
    build_run_provenance,
    jsonable_result,
    merge_stage_repairs,
    write_extraction_dump,
)
from scraper.models.s3.scraped_text_file import ScrapedTextFile

logger = logging.getLogger(__name__)

_GROUNDING_STAGES = (
    PipelineStage.initial_grounding,
    PipelineStage.freehand_grounding,
)


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

            relationship_flat: Optional[LLMPhraseRelationshipResults] = None
            if PipelineStage.relationship in completed_by_stage:
                node_class, request_map = completed_by_stage[PipelineStage.relationship]
                relationship_flat = await node_class.get_result(
                    subject_unique_id=subject_unique_id,
                    field_type=field_type,
                    chunk_bounds=chunk_bounds,
                    extraction_bundle=bundle,
                    completed_request_map=request_map,
                    timestamp=timestamp,
                )

            # One repairs sink per stage, for the same reason the reconcile
            # nodes keep them apart: the same phrase can be mis-echoed at more
            # than one stage, and a shared dict keeps only the last one parsed.
            screening_repairs: dict[str, str] = {}
            grounding_repairs: dict[str, str] = {}

            screening_flat: Optional[LiveScreeningResults] = None
            if PipelineStage.screening in completed_by_stage:
                node_class, request_map = completed_by_stage[PipelineStage.screening]
                screening_flat = await node_class.get_result(
                    subject_unique_id=subject_unique_id,
                    field_type=field_type,
                    chunk_bounds=chunk_bounds,
                    extraction_bundle=bundle,
                    completed_request_map=request_map,
                    timestamp=timestamp,
                    repairs=screening_repairs,
                )

            grounding_flat: Optional[PhraseToTagAndRulesMap] = None
            grounding_stage: Optional[PipelineStage] = next(
                (stage for stage in _GROUNDING_STAGES if stage in completed_by_stage),
                None,
            )
            if grounding_stage is not None:
                node_class, request_map = completed_by_stage[grounding_stage]
                grounding_flat = await node_class.get_result(
                    subject_unique_id=subject_unique_id,
                    field_type=field_type,
                    chunk_bounds=chunk_bounds,
                    extraction_bundle=bundle,
                    completed_request_map=request_map,
                    timestamp=timestamp,
                    repairs=grounding_repairs,
                )

            chunked_contents[chunk_bounds] = {
                "rows": build_partial_phrase_rows(
                    search_rounds=search_rounds,
                    relationship_flat=relationship_flat,
                    screening_flat=screening_flat,
                    grounding_flat=grounding_flat,
                    grounding_stage=grounding_stage.value if grounding_stage else None,
                    repairs_flat=merge_stage_repairs(
                        {
                            "screening": screening_repairs,
                            **(
                                {grounding_stage.value: grounding_repairs}
                                if grounding_stage
                                else {}
                            ),
                        }
                    ),
                    subject_name=pipeline_context.subject_name,
                )
            }

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
            ),
            name_suffix="__partial",
        )
    except Exception as error:  # diagnostics must not sink the run
        logger.error(
            f"[{subject_unique_id}] Failed to write partial run dump for "
            f"'{field_type.name}' stopped at {stopped_at.value}: {error}",
            exc_info=True,
        )
