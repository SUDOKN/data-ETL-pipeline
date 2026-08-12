from __future__ import annotations

import asyncio
import logging
import traceback
from datetime import datetime
from typing import Optional

from pydantic import ValidationError

from llm_providers.db_models.gpt_batch_request import (
    GPTBatchRequest,
)
from core.models.deferred_extraction.deferred_keyword_extraction import (
    KeywordExtractionRequestBundle,
    KeywordExtractionRequestMap,
)
from core.models.extraction_schemas.grounding import (
    PhraseToTagAndRulesMap,
    TagToAppliedRulesMap,
)
from core.models.rule_catalog import STAGE_FREEHAND_GROUNDING
from core.services.applied_rule_validation import (
    check_applied_rules,
    raise_for_violations,
)
from core.services.rule_catalog_registry import get_rule_catalog
from core.models.extraction_schemas.relationship import (
    LLMPhraseRelationshipResults,
)
from core.models.extraction_schemas.catalog_wire_schema import (
    flatten_rule_slots,
    grounding_response_model,
    response_format_for,
)
from llm_providers.models.open_ai.gpt_batch_response_blob import (
    ChatCompletionChoiceMessage,
)
from llm_providers.models.llm_model import (
    LLM_Model,
    NO_MODEL,
)
from llm_providers.models.file_objects.prompt import Prompt
from llm_providers.services.gpt_batch_request.gpt_batch_request_service import (
    create_base_gpt_batch_request,
    get_dummy_gpt_batch_response,
)
from llm_providers.services.gpt_batch_request.gpt_batch_request_writes import (
    record_response_parse_error_capped,
)
from core.services.phrase_blocks_contract import (
    hold_response_to_sent_phrases,
    render_phrase_blocks,
)
from core.models.field_types import ExtractionFieldType
from core.services.pipeline_nodes.multi_stage.llm_phrase_relationship_node_service import (
    get_phrase_relationship_result as parse_phrase_relationship_batch_req_result,
)
from core.services.pipeline_nodes.multi_stage.llm_relationship_screening_node_service import (
    get_phrase_relationship_screening_result as parse_relationship_screening_batch_req_result,
    get_verified_live_screening_results,
    _split_into_pair_groups,
)
from llm_providers.field_types import BatchRequestIDType
from llm_providers.models.open_ai.gpt_model_params import (
    GPTModelParams,
)

logger = logging.getLogger(__name__)


def get_freehand_grounding_response_schema(field_type: ExtractionFieldType) -> dict:
    """This stage's strict ``response_format``, for one field type. Per catalog
    rather than a module constant — see ``catalog_wire_schema``."""
    return response_format_for(
        get_rule_catalog(STAGE_FREEHAND_GROUNDING, field_type.name)
    )


def parse_llm_phrase_freehand_grounding_result(
    gpt_response: Optional[str],
    field_type: ExtractionFieldType,
) -> PhraseToTagAndRulesMap:
    if not gpt_response:
        logger.error(f"Invalid gpt_response:{gpt_response}")
        raise ValueError(
            "parse_llm_phrase_freehand_grounding_result: Empty or invalid response from GPT"
        )

    catalog = get_rule_catalog(STAGE_FREEHAND_GROUNDING, field_type.name)

    try:
        parsed = grounding_response_model(catalog).model_validate_json(gpt_response)
    except ValidationError as e:
        raise ValueError(
            f"parse_llm_phrase_freehand_grounding_result: Invalid response from GPT:{gpt_response}"
        ) from e

    raw_llm_freehand_grounding_result: PhraseToTagAndRulesMap = {}
    violations: list[str] = []
    for entry in parsed.groundings:
        if entry.phrase in raw_llm_freehand_grounding_result:
            raise ValueError(
                f"parse_llm_phrase_freehand_grounding_result: Duplicate phrase {entry.phrase!r} in groundings response"
            )
        rules_by_category: TagToAppliedRulesMap = {}
        for category in entry.categories:
            applied_rules = flatten_rule_slots(catalog, category)
            # Collected across every category of every phrase, then raised once at
            # the end: a response is measured whole or its defect rate is a
            # function of where the scan stopped.
            report = check_applied_rules(
                catalog=catalog,
                applied_rules=applied_rules,
                where=f"phrase {entry.phrase!r} category {category.category!r}",
            )
            violations.extend(report.problems)
            rules_by_category[category.category] = applied_rules
        # Collapses to the shared tag-keyed map here; see grounding.py on why the
        # category naming stops at the wire schema.
        raw_llm_freehand_grounding_result[entry.phrase] = rules_by_category

    raise_for_violations(violations)

    return raw_llm_freehand_grounding_result


async def parse_phrase_freehand_grounding_group_result(
    subject_unique_id: str,
    field_type: ExtractionFieldType,
    chunk_bounds: str,
    group_req_id: BatchRequestIDType,
    completed_request_map: dict[BatchRequestIDType, GPTBatchRequest],
    timestamp: datetime,
    repairs: Optional[dict[str, str]] = None,
) -> PhraseToTagAndRulesMap:
    """Parse the groundings returned by a single freehand-grounding group request."""
    req_obj = completed_request_map.get(group_req_id)
    if not req_obj:
        raise ValueError(
            f"phrase_freehand_grounding_node.parse_batch_request_result: Missing GPTBatchRequest for phrase_freehand_grounding request ID {group_req_id} in {subject_unique_id}:{field_type.name}"
        )
    if not req_obj.response:
        raise ValueError(
            f"phrase_freehand_grounding_node.parse_batch_request_result: GPTBatchRequest for phrase_freehand_grounding request ID {group_req_id} has no response_blob in {subject_unique_id}:{field_type.name}"
        )

    try:
        parsed_map = parse_llm_phrase_freehand_grounding_result(
            gpt_response=req_obj.response.result,
            field_type=field_type,
        )
        # Missing phrases raise from here, inside the try, so an under-answering
        # response is recorded and re-dispatched like any other parse failure —
        # the observed failure shape is a response that closes its groundings
        # array a few entries in and validates cleanly against the schema.
        return hold_response_to_sent_phrases(
            user_message=req_obj.request.body.user_message(),
            response_by_phrase=parsed_map,
            where=f"{subject_unique_id}:{field_type.name} freehand grounding {group_req_id}",
            on_missing="raise",
            repairs=repairs,
        )
    except Exception as e:
        await record_response_parse_error_capped(
            gpt_batch_request=req_obj,
            error_message=str(e),
            timestamp=timestamp,
            traceback_str=traceback.format_exc(),
        )
        logger.error(
            f"phrase_freehand_grounding_node.parse_batch_request_result: Error parsing phrase_freehand_grounding results for subject {subject_unique_id} from GPT response: {e}"
        )
        raise


async def get_freehand_grounding_result(
    subject_unique_id: str,
    field_type: ExtractionFieldType,
    chunk_bounds: str,
    extraction_bundle: KeywordExtractionRequestBundle,
    completed_request_map: dict[BatchRequestIDType, GPTBatchRequest],
    timestamp: datetime,
    repairs: Optional[dict[str, str]] = None,
) -> PhraseToTagAndRulesMap:
    """Merge groundings across every group embedded for the chunk.

    ``repairs`` is forwarded to the per-group hold; see
    ``hold_response_to_sent_phrases``.
    """
    group_req_ids = extraction_bundle.llm_phrase_freehand_grounding_req_ids
    if not group_req_ids:
        raise ValueError(
            f"phrase_freehand_grounding_node.parse_batch_request_result: llm_phrase_freehand_grounding_req_ids is empty for chunk bounds {chunk_bounds} in {subject_unique_id}:{field_type.name}"
        )

    merged_results: PhraseToTagAndRulesMap = {}
    for group_req_id in group_req_ids:
        merged_results.update(
            await parse_phrase_freehand_grounding_group_result(
                subject_unique_id=subject_unique_id,
                field_type=field_type,
                chunk_bounds=chunk_bounds,
                group_req_id=group_req_id,
                completed_request_map=completed_request_map,
                timestamp=timestamp,
                repairs=repairs,
            )
        )
    return merged_results


async def get_verified_phrases_w_og_summary(
    subject_unique_id: str,
    field_type: ExtractionFieldType,
    chunk_bounds: str,
    extraction_bundle: KeywordExtractionRequestBundle,
    llm_phrase_relationship_gpt_request_map: dict[BatchRequestIDType, GPTBatchRequest],
    llm_phrase_screening_gpt_request_map: dict[BatchRequestIDType, GPTBatchRequest],
    timestamp: datetime,
) -> LLMPhraseRelationshipResults:
    """Return the screened phrase->summary map for a chunk: the relationship pairs
    whose phrase passed screening. This is the exact input set the freehand-grounding
    phase splits into groups, so id-embedding (group count) and request creation
    (group content) derive from it identically."""
    llm_phrase_relationship_results = await parse_phrase_relationship_batch_req_result(
        subject_unique_id=subject_unique_id,
        field_type=field_type,
        chunk_bounds=chunk_bounds,
        extraction_bundle=extraction_bundle,
        completed_request_map=llm_phrase_relationship_gpt_request_map,
        timestamp=timestamp,
    )
    llm_phrase_relationship_screening_results = (
        await parse_relationship_screening_batch_req_result(
            subject_unique_id=subject_unique_id,
            field_type=field_type,
            chunk_bounds=chunk_bounds,
            extraction_bundle=extraction_bundle,
            completed_request_map=llm_phrase_screening_gpt_request_map,
            timestamp=timestamp,
        )
    )

    left_only_phrases = (
        llm_phrase_relationship_results.keys()
        - llm_phrase_relationship_screening_results.keys()
    )
    if left_only_phrases:
        raise ValueError(
            f"Phrases {left_only_phrases} found in relationship results:{extraction_bundle.llm_phrase_relationship_req_ids} but not in screening results:{extraction_bundle.llm_phrase_relationship_screening_req_ids} for {subject_unique_id}:{field_type} chunk {chunk_bounds}"
        )
    right_only_phrases = (
        llm_phrase_relationship_screening_results.keys()
        - llm_phrase_relationship_results.keys()
    )
    if right_only_phrases:
        raise ValueError(
            f"Phrases {right_only_phrases} found in screening results:{extraction_bundle.llm_phrase_relationship_screening_req_ids} but were never listed in relationship results:{extraction_bundle.llm_phrase_relationship_req_ids} for {subject_unique_id}:{field_type} chunk {chunk_bounds}"
        )

    # Discard phrases which have been filtered out by the screening phase
    screened_phrases = get_verified_live_screening_results(
        llm_phrase_relationship_screening_results
    )
    return {
        k: v
        for k, v in llm_phrase_relationship_results.items()
        if k in screened_phrases
    }


async def create_missing_phrase_freehand_grounding_requests(
    subject_unique_id: str,
    subject_name: str,
    field_type: ExtractionFieldType,
    chunked_request_map: KeywordExtractionRequestMap,
    missing_phrase_freehand_grounding_req_ids: set[BatchRequestIDType],
    phrase_freehand_grounding_prompt: Prompt,
    llm_phrase_relationship_gpt_request_map: dict[BatchRequestIDType, GPTBatchRequest],
    llm_phrase_screening_gpt_request_map: dict[BatchRequestIDType, GPTBatchRequest],
    deferred_at: datetime,
    llm_model: LLM_Model,
    model_params: GPTModelParams,
    max_pairs_per_request: int,
    eager: bool,
    BATCH_SIZE=100,
) -> list[GPTBatchRequest]:
    logger.info(
        f"create_missing_phrase_freehand_grounding_requests: Generating GPTBatchRequests for {subject_unique_id}:{field_type}"
    )

    if (
        not llm_phrase_relationship_gpt_request_map
        or not llm_phrase_screening_gpt_request_map
    ):
        raise ValueError(
            f"create_missing_phrase_freehand_grounding_requests: No completed GPTBatchRequests found for {subject_unique_id}:{field_type} in upstream maps."
        )

    batch_requests: list[GPTBatchRequest] = []
    chunk_items = list(
        {
            chunk_bounds: bundle
            for chunk_bounds, bundle in chunked_request_map.items()
            if set(bundle.llm_phrase_freehand_grounding_req_ids)
            & missing_phrase_freehand_grounding_req_ids
        }.items()
    )

    for i in range(0, len(chunk_items), BATCH_SIZE):
        batch = chunk_items[i : i + BATCH_SIZE]

        for chunk_bounds, extraction_bundle in batch:
            verified_phrases_w_og_summary = await get_verified_phrases_w_og_summary(
                subject_unique_id=subject_unique_id,
                field_type=field_type,
                chunk_bounds=chunk_bounds,
                extraction_bundle=extraction_bundle,
                llm_phrase_relationship_gpt_request_map=llm_phrase_relationship_gpt_request_map,
                llm_phrase_screening_gpt_request_map=llm_phrase_screening_gpt_request_map,
                timestamp=deferred_at,
            )

            phrase_groups = _split_into_pair_groups(
                verified_phrases_w_og_summary, max_pairs_per_request
            )
            group_req_ids = extraction_bundle.llm_phrase_freehand_grounding_req_ids
            if len(group_req_ids) != len(phrase_groups):
                raise ValueError(
                    f"create_missing_phrase_freehand_grounding_requests: embedded group count "
                    f"({len(group_req_ids)}) does not match computed group count ({len(phrase_groups)}) "
                    f"for chunk bounds {chunk_bounds} in {subject_unique_id}:{field_type}. Group counts are "
                    f"computed once, upfront, from the same screened phrases, so this should not happen."
                )

            for group_index, group_req_id in enumerate(group_req_ids):
                if group_req_id not in missing_phrase_freehand_grounding_req_ids:
                    continue

                phrase_group = phrase_groups[group_index]
                if not phrase_group:
                    # _split_into_pair_groups only ever produces an empty group as the
                    # sole element of a single-group list (zero screened phrases total)
                    if len(phrase_groups) != 1:
                        raise ValueError(
                            f"create_missing_phrase_freehand_grounding_requests: unexpected empty "
                            f"phrase group at index {group_index} of {len(phrase_groups)} groups for chunk "
                            f"bounds {chunk_bounds} in {subject_unique_id}:{field_type}. Only a single group should "
                            f"ever be empty (the zero-screened-phrases case)."
                        )
                    new_batch_request = (
                        _create_dummy_completed_phrase_freehand_grounding_batch_request(
                            deferred_at=deferred_at,
                            subject_unique_id=subject_unique_id,
                            llm_phrase_freehand_grounding_request_id=group_req_id,
                            model_params=model_params,
                            eager=eager,
                        )
                    )
                else:
                    new_batch_request = create_deferred_phrase_freehand_grounding_gpt_request(
                        deferred_at=deferred_at,
                        subject_unique_id=subject_unique_id,
                        field_type=field_type,
                        llm_phrase_freehand_grounding_request_id=group_req_id,
                        phrase_freehand_grounding_prompt=phrase_freehand_grounding_prompt,
                        subject_name=subject_name,
                        verified_phrases_w_og_summary=phrase_group,
                        gpt_model=llm_model,
                        eager=eager,
                        model_params=model_params,
                    )

                batch_requests.append(new_batch_request)

        await asyncio.sleep(0)

        if (i + BATCH_SIZE) % 500 == 0:
            logger.info(
                f"Created {min(i + BATCH_SIZE, len(chunk_items))}/{len(chunk_items)} "
                f"gpt request for {subject_unique_id}:{field_type}"
            )

    return batch_requests


def _create_dummy_completed_phrase_freehand_grounding_batch_request(
    deferred_at: datetime,
    subject_unique_id: str,
    llm_phrase_freehand_grounding_request_id: BatchRequestIDType,
    model_params: GPTModelParams,
    eager: bool,
) -> GPTBatchRequest:
    base_gpt_batch_request = create_base_gpt_batch_request(
        deferred_at=deferred_at,
        subject_unique_id=subject_unique_id,
        custom_id=llm_phrase_freehand_grounding_request_id,
        # Still carries a block, empty — an absent one has to stay an error.
        context=(
            "No phrase freehand grounding needed - no phrases passed screening.\n"
            f"{render_phrase_blocks({})}"
        ),
        prompt_text="No phrase freehand grounding needed - no phrases passed screening.",
        gpt_model=NO_MODEL,
        model_params=model_params,
        batch_id="Eager" if eager else "dummy_phrase_freehand_grounding_batch_id",
    )

    base_gpt_batch_request.response = get_dummy_gpt_batch_response(
        deferred_at=deferred_at,
        request_custom_id=llm_phrase_freehand_grounding_request_id,
        dummy_chat_completion_id="dummy_completion_id",
        chat_completion_choice_message=ChatCompletionChoiceMessage(
            role="assistant", content='{"groundings": []}'
        ),
    )

    return base_gpt_batch_request


def create_deferred_phrase_freehand_grounding_gpt_request(
    deferred_at: datetime,
    subject_unique_id: str,
    field_type: ExtractionFieldType,
    llm_phrase_freehand_grounding_request_id: BatchRequestIDType,
    phrase_freehand_grounding_prompt: Prompt,
    subject_name: str,
    verified_phrases_w_og_summary: LLMPhraseRelationshipResults,
    gpt_model: LLM_Model,
    eager: bool,
    model_params: GPTModelParams,
) -> GPTBatchRequest:
    logger.info(
        f"create_deferred_phrase_freehand_grounding_gpt_request: Generating GPTBatchRequest for {llm_phrase_freehand_grounding_request_id}"
    )
    # f"Manufacturer name: {subject_name}\n\n"
    context = render_phrase_blocks(verified_phrases_w_og_summary)

    return create_base_gpt_batch_request(
        deferred_at=deferred_at,
        subject_unique_id=subject_unique_id,
        custom_id=llm_phrase_freehand_grounding_request_id,
        context=context,
        prompt_text=phrase_freehand_grounding_prompt.text,
        gpt_model=gpt_model,
        model_params=model_params.with_response_format(
            get_freehand_grounding_response_schema(field_type)
        ),
        batch_id="Eager" if eager else None,
    )
