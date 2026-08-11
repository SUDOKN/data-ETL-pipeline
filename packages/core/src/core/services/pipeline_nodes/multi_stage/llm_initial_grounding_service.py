from __future__ import annotations

import asyncio
import json
import logging
import traceback
from datetime import datetime
from typing import Optional
from pydantic import ValidationError
from requests.structures import CaseInsensitiveDict

from core.models.extraction_schemas.grounding import (
    PhraseOptionGroundingResponse,
    PhraseToTagAndRulesMap,
    TagToAppliedRulesMap,
)
from core.models.rule_catalog import STAGE_INITIAL_GROUNDING
from core.services.applied_rule_validation import (
    check_applied_rules,
    raise_for_violations,
)
from core.services.rule_catalog_registry import get_rule_catalog
from core.models.extraction_schemas.relationship import (
    LLMPhraseRelationshipResults,
)
from core.models.extraction_schemas.response_format_util import (
    build_gpt_response_format,
)
from llm_providers.models.file_objects.prompt import Prompt
from core.services.phrase_summaries_block import render_phrase_summaries_block
from llm_providers.models.open_ai.gpt_batch_response_blob import (
    ChatCompletionChoiceMessage,
)
from llm_providers.db_models.gpt_batch_request import (
    GPTBatchRequest,
)
from core.models.deferred_extraction.deferred_concept_extraction import (
    ConceptExtractionRequestMap,
    ConceptExtractionRequestBundle,
    TaggingResultsGroupedByConcept,
    TaggingResult,
)
from core.models.skos_concept import Concept
from core.models.field_types import (
    ConceptFieldType,
)
from core.services.pipeline_nodes.multi_stage.llm_phrase_relationship_node_service import (
    get_phrase_relationship_result,
)
from core.services.pipeline_nodes.multi_stage.llm_relationship_screening_node_service import (
    get_verified_live_screening_results,
    get_phrase_relationship_screening_result,
    _split_into_pair_groups,
)
from llm_providers.models.open_ai.gpt_model_params import (
    GPTModelParams,
)
from llm_providers.field_types import BatchRequestIDType
from llm_providers.models.llm_model import (
    NO_MODEL,
    LLM_Model,
)

from llm_providers.services.gpt_batch_request.gpt_batch_request_writes import (
    record_response_parse_error,
)
from llm_providers.services.gpt_batch_request.gpt_batch_request_service import (
    create_base_gpt_batch_request,
    get_dummy_gpt_batch_response,
)

logger = logging.getLogger(__name__)


LLM_PHRASE_INITIAL_GROUNDING_RESPONSE_SCHEMA = build_gpt_response_format(
    PhraseOptionGroundingResponse, name="phrase_initial_grounding_result"
)


def parse_llm_phrase_initial_grounding_result(
    gpt_response: Optional[str],
    field_type: ConceptFieldType,
) -> PhraseToTagAndRulesMap:
    if not gpt_response:
        logger.error(f"Invalid gpt_response:{gpt_response}")
        raise ValueError(
            "parse_llm_phrase_initial_grounding_result: Empty or invalid response from GPT"
        )

    try:
        parsed = PhraseOptionGroundingResponse.model_validate_json(gpt_response)
    except ValidationError as e:
        raise ValueError(
            f"parse_llm_phrase_initial_grounding_result: Invalid response from GPT:{gpt_response}"
        ) from e

    raw_llm_initial_grounding_result: PhraseToTagAndRulesMap = {}
    violations: list[str] = []
    for entry in parsed.groundings:
        if entry.phrase in raw_llm_initial_grounding_result:
            raise ValueError(
                f"parse_llm_phrase_initial_grounding_result: Duplicate phrase {entry.phrase!r} in groundings response"
            )
        catalog = get_rule_catalog(STAGE_INITIAL_GROUNDING, field_type.name)
        rules_by_option: TagToAppliedRulesMap = {}
        for option in entry.options:
            # Collected across every option of every phrase, then raised once at
            # the end: a response is measured whole or its defect rate is a
            # function of where the scan stopped.
            report = check_applied_rules(
                catalog=catalog,
                applied_rules=option.applied_rules,
                where=f"phrase {entry.phrase!r} option {option.option!r}",
            )
            violations.extend(report.problems)
            rules_by_option[option.option] = option.applied_rules
        raw_llm_initial_grounding_result[entry.phrase] = rules_by_option

    raise_for_violations(violations)

    logger.debug(f"raw_llm_initial_grounding_result:{raw_llm_initial_grounding_result}")

    return raw_llm_initial_grounding_result


async def parse_phrase_initial_grounding_group_result(
    subject_unique_id: str,
    field_type: ConceptFieldType,
    chunk_bounds: str,
    group_req_id: BatchRequestIDType,
    completed_request_map: dict[BatchRequestIDType, GPTBatchRequest],
    timestamp: datetime,
) -> PhraseToTagAndRulesMap:
    """Parse the groundings returned by a single initial-grounding group request."""
    req_obj = completed_request_map.get(group_req_id)
    if not req_obj:
        raise ValueError(
            f"phrase_initial_grounding_node.parse_batch_request_result: Missing GPTBatchRequest for phrase_initial_grounding request ID {group_req_id} in {subject_unique_id}:{field_type.name}"
        )
    elif not req_obj.response:
        raise ValueError(
            f"phrase_initial_grounding_node.parse_batch_request_result: GPTBatchRequest for phrase_initial_grounding request ID {group_req_id} has no response_blob in {subject_unique_id}:{field_type.name}"
        )

    try:
        return parse_llm_phrase_initial_grounding_result(
            gpt_response=req_obj.response.result,
            field_type=field_type,
        )
    except Exception as e:
        await record_response_parse_error(
            gpt_batch_request=req_obj,
            error_message=str(e),
            timestamp=timestamp,
            traceback_str=traceback.format_exc(),
        )
        logger.error(
            f"phrase_initial_grounding_node.parse_batch_request_result: Error parsing phrase_initial_grounding results for subject {subject_unique_id} from GPT response: {e}"
        )
        raise


async def get_initial_grounding_result(
    subject_unique_id: str,
    field_type: ConceptFieldType,
    chunk_bounds: str,
    extraction_bundle: ConceptExtractionRequestBundle,
    completed_request_map: dict[BatchRequestIDType, GPTBatchRequest],
    timestamp: datetime,
) -> PhraseToTagAndRulesMap:
    """Merge groundings across every group embedded for the chunk."""
    group_req_ids = extraction_bundle.llm_phrase_initial_grounding_req_ids
    if not group_req_ids:
        raise ValueError(
            f"phrase_initial_grounding_node.parse_batch_request_result: llm_phrase_initial_grounding_req_ids is empty for chunk bounds {chunk_bounds} in {subject_unique_id}:{field_type.name}"
        )

    merged_results: PhraseToTagAndRulesMap = {}
    for group_req_id in group_req_ids:
        merged_results.update(
            await parse_phrase_initial_grounding_group_result(
                subject_unique_id=subject_unique_id,
                field_type=field_type,
                chunk_bounds=chunk_bounds,
                group_req_id=group_req_id,
                completed_request_map=completed_request_map,
                timestamp=timestamp,
            )
        )
    return merged_results


async def get_tagged_results_from_initial_grounding(
    subject_unique_id: str,
    field_type: ConceptFieldType,
    chunk_bounds: str,
    extraction_bundle: ConceptExtractionRequestBundle,
    completed_request_map: dict[BatchRequestIDType, GPTBatchRequest],
    timestamp: datetime,
) -> list[TaggingResult]:
    return get_grounding_results_grouped_by_tags(
        await get_initial_grounding_result(
            subject_unique_id=subject_unique_id,
            field_type=field_type,
            chunk_bounds=chunk_bounds,
            extraction_bundle=extraction_bundle,
            completed_request_map=completed_request_map,
            timestamp=timestamp,
        )
    )


def get_grounding_results_grouped_by_tags(
    grounding_result: PhraseToTagAndRulesMap,
) -> list[TaggingResult]:
    # logger.info(f"grounding_result:{json.dumps(grounding_result, indent=2)}")
    tr_map: dict[str, TaggingResult] = {}
    for phrase, tag_rules_map in grounding_result.items():
        for tag, applied_rules in tag_rules_map.items():
            tr = tr_map.setdefault(
                tag, TaggingResult(group_id=tag, phrase_rules_map={})
            )
            if phrase in tr.phrase_rules_map:
                raise ValueError(
                    f"phrase:{phrase} was already present in "
                    f"tr.phrase_rules_map[phrase]:{tr.phrase_rules_map[phrase]}"
                )

            tr.phrase_rules_map[phrase] = applied_rules
            # logger.info(f"After updating tr.phrase_rules_map:{tr.phrase_rules_map}")
        # logger.info(f"After updating tr_map.get(tag={tag}):{tr_map.get(tag)}")

    return list(tr_map.values())


def get_tcs_and_oov_trs_from_trs(
    trs: list[TaggingResult],
    match_label_to_concept_map: CaseInsensitiveDict[Concept],  # DO NOT MUTATE
) -> tuple[list[TaggingResult], list[TaggingResultsGroupedByConcept]]:
    oov_trs: list[TaggingResult] = []
    concept_to_tc_map: dict[Concept, TaggingResultsGroupedByConcept] = {}

    for tr in trs:
        concept_obj = match_label_to_concept_map.get(
            tr.group_id
        )  # NOTE: multiple group tags may point to same concept_obj because they can be name/altLabels

        if not concept_obj:
            oov_trs.append(tr)
            continue

        tc = concept_to_tc_map.setdefault(
            concept_obj,
            TaggingResultsGroupedByConcept(
                concept=concept_obj,
                og_tag_w_phrase_rules_map={},
            ),
        )
        if tr.group_id in tc.og_tag_w_phrase_rules_map:
            raise ValueError(
                f"tr.group_id:{tr.group_id} was already present in "
                f"tc.og_tag_w_phrase_rules_map[tr.group_id]:{tc.og_tag_w_phrase_rules_map[tr.group_id]}"
            )
        tc.og_tag_w_phrase_rules_map[tr.group_id] = tr.phrase_rules_map
        # else:
        #     tc.og_tag_w_phrase_rules_map[tr.group_tag].update(tr.phrase_rules_map)

    return (oov_trs, list(concept_to_tc_map.values()))


def get_descend_worthy_tcs_from_tagged_results(
    initially_tagged_trs: list[TaggingResult],
    match_label_to_concept_map: CaseInsensitiveDict[Concept],
) -> list[TaggingResultsGroupedByConcept]:
    descend_worthy_tcs: list[TaggingResultsGroupedByConcept] = []
    oov_trs, tcs = get_tcs_and_oov_trs_from_trs(
        trs=initially_tagged_trs, match_label_to_concept_map=match_label_to_concept_map
    )
    # non_descend_worthy_trs: list[TaggingResult] = oov_trs
    logger.info(f"initially_tagged_trs:{initially_tagged_trs}")

    """
    TaggedConceptResult
    {
        concept: C
        og_tag_w_phrase_rules_map: {
            C.name: {
                p1: r11
            },
            C.altLabel1: {
                p1: r12, 
                p2: r2      # may get pruned if p2 matches C.name or C.altLabel2 or C.altLabel3
            }
            C.altLabel3: {
                p3: r3
            },
            oov_1: { # gets kicked out by get_tcs_from_trs
                p1: r13,
                p4: r4
            }
        }
    }
    """

    for tc in tcs:
        for og_tag, phrase_rules_map in list(tc.og_tag_w_phrase_rules_map.items()):
            # start filtering out phrases that directly matched the tag
            for phrase in list(phrase_rules_map.keys()):  # p1, p2..
                if phrase in tc.concept.matchLabels:
                    # matchLabels = C.name, C.altLabel1, C.altLabel2,
                    # one of these was the og_tag but cross comparison
                    # allows more pruning
                    phrase_rules_map.pop(phrase)

            if not phrase_rules_map:
                tc.og_tag_w_phrase_rules_map.pop(og_tag)

        if (
            tc.og_tag_w_phrase_rules_map
        ):  # not empty, contains at least one phrase that doesn't exactly match the og tag
            descend_worthy_tcs.append(tc)

    return descend_worthy_tcs


async def get_verified_out_of_vocab_phrases_w_summary(
    subject_unique_id: str,
    field_type: ConceptFieldType,
    chunk_bounds: str,
    extraction_bundle: ConceptExtractionRequestBundle,
    llm_phrase_relationship_gpt_request_map: dict[BatchRequestIDType, GPTBatchRequest],
    llm_phrase_screening_gpt_request_map: dict[BatchRequestIDType, GPTBatchRequest],
    timestamp: datetime,
) -> LLMPhraseRelationshipResults:
    """Return the screened phrase->summary map for a chunk: the relationship pairs
    whose phrase passed screening. This is the exact input set the initial-grounding
    phase splits into groups, so id-embedding (group count) and request creation
    (group content) derive from it identically."""
    llm_phrase_relationship_results = await get_phrase_relationship_result(
        subject_unique_id=subject_unique_id,
        field_type=field_type,
        chunk_bounds=chunk_bounds,
        extraction_bundle=extraction_bundle,
        completed_request_map=llm_phrase_relationship_gpt_request_map,
        timestamp=timestamp,
    )
    llm_phrase_relationship_screening_results = (
        await get_phrase_relationship_screening_result(
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
            f"Phrases {left_only_phrases} found in relationship results but not in screening results for {subject_unique_id}:{field_type} chunk {chunk_bounds}"
        )
    right_only_phrases = (
        llm_phrase_relationship_screening_results.keys()
        - llm_phrase_relationship_results.keys()
    )
    if right_only_phrases:
        raise ValueError(
            f"Phrases {right_only_phrases} found in screening results but were never listed in relationship results for {subject_unique_id}:{field_type} chunk {chunk_bounds}"
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


async def create_missing_phrase_initial_grounding_requests(
    # used for logging and debugging
    subject_unique_id: str,
    subject_name: str,
    field_type: ConceptFieldType,
    # context
    chunked_request_map: ConceptExtractionRequestMap,
    missing_phrase_initial_grounding_req_ids: set[BatchRequestIDType],
    phrase_initial_grounding_prompt: Prompt,
    llm_phrase_relationship_gpt_request_map: dict[BatchRequestIDType, GPTBatchRequest],
    llm_phrase_screening_gpt_request_map: dict[BatchRequestIDType, GPTBatchRequest],
    known_concepts: set[Concept],  # DO NOT MUTATE
    match_label_to_concept_map: CaseInsensitiveDict[Concept],
    # metadata
    deferred_at: datetime,
    llm_model: LLM_Model,
    model_params: GPTModelParams,
    max_pairs_per_request: int,
    eager: bool,
    BATCH_SIZE=100,
) -> list[GPTBatchRequest]:
    logger.info(
        f"create_missing_phrase_initial_grounding_requests: Generating GPTBatchRequests for {subject_unique_id}:{field_type}"
    )
    logger.info(
        f"match_label_to_concept_map keys: {list(match_label_to_concept_map.keys())}"
    )

    batch_requests: list[GPTBatchRequest] = []
    chunk_items = list(
        {
            chunk_bounds: bundle
            for chunk_bounds, bundle in chunked_request_map.items()
            if set(bundle.llm_phrase_initial_grounding_req_ids)
            & missing_phrase_initial_grounding_req_ids
            # because sometimes some breq docs may have already been created
        }.items()
    )

    if (
        not llm_phrase_relationship_gpt_request_map
        or not llm_phrase_screening_gpt_request_map
    ):
        raise ValueError(
            f"create_missing_phrase_initial_grounding_requests: No completed GPTBatchRequests found for {subject_unique_id}:{field_type} in llm_phrase_relationship and llm_phrase_screening gpt_request_maps."
        )

    # Process chunks in batches to yield control periodically
    for i in range(0, len(chunk_items), BATCH_SIZE):
        batch = chunk_items[i : i + BATCH_SIZE]

        # Process current batch
        for chunk_bounds, extraction_bundle in batch:
            verified_out_of_vocab_phrases_w_summary = await get_verified_out_of_vocab_phrases_w_summary(
                subject_unique_id=subject_unique_id,
                field_type=field_type,
                chunk_bounds=chunk_bounds,
                extraction_bundle=extraction_bundle,
                llm_phrase_relationship_gpt_request_map=llm_phrase_relationship_gpt_request_map,
                llm_phrase_screening_gpt_request_map=llm_phrase_screening_gpt_request_map,
                timestamp=deferred_at,
            )
            logger.info(
                f"verified_out_of_vocab_phrases_w_summary:{verified_out_of_vocab_phrases_w_summary}"
            )

            phrase_groups = _split_into_pair_groups(
                verified_out_of_vocab_phrases_w_summary, max_pairs_per_request
            )
            group_req_ids = extraction_bundle.llm_phrase_initial_grounding_req_ids
            if len(group_req_ids) != len(phrase_groups):
                raise ValueError(
                    f"create_missing_phrase_initial_grounding_requests: embedded group count "
                    f"({len(group_req_ids)}) does not match computed group count ({len(phrase_groups)}) "
                    f"for chunk bounds {chunk_bounds} in {subject_unique_id}:{field_type}. Group counts are "
                    f"computed once, upfront, from the same screened phrases, so this should not happen."
                )

            for group_index, group_req_id in enumerate(group_req_ids):
                if group_req_id not in missing_phrase_initial_grounding_req_ids:
                    continue

                phrase_group = phrase_groups[group_index]
                if not phrase_group:
                    # _split_into_pair_groups only ever produces an empty group as the
                    # sole element of a single-group list (zero screened phrases total)
                    if len(phrase_groups) != 1:
                        raise ValueError(
                            f"create_missing_phrase_initial_grounding_requests: unexpected empty "
                            f"phrase group at index {group_index} of {len(phrase_groups)} groups for chunk "
                            f"bounds {chunk_bounds} in {subject_unique_id}:{field_type}. Only a single group should "
                            f"ever be empty (the zero-screened-phrases case)."
                        )
                    # add a dummy response blob with empty dict
                    logger.info(
                        f"No out-of-vocab phrases found in text, for {subject_unique_id}:{field_type}, creating dummy initial grounding request."
                    )
                    new_batch_request = (
                        _create_dummy_completed_phrase_initial_grounding_batch_request(
                            deferred_at=deferred_at,
                            subject_unique_id=subject_unique_id,
                            llm_phrase_initial_grounding_request_id=group_req_id,
                            model_params=model_params,
                            eager=eager,
                        )
                    )
                else:
                    logger.info(
                        f"Passing on candidates {phrase_group} to phrase_initial_grounding phase for "
                        f"{subject_unique_id}:{field_type} chunk {chunk_bounds} group {group_index}"
                    )
                    new_batch_request = create_deferred_phrase_initial_grounding_gpt_request(
                        deferred_at=deferred_at,
                        subject_unique_id=subject_unique_id,
                        llm_phrase_initial_grounding_request_id=group_req_id,
                        phrase_initial_grounding_prompt=phrase_initial_grounding_prompt,
                        field_type=field_type,
                        # context variables
                        subject_name=subject_name,
                        all_concepts=known_concepts,
                        verified_out_of_vocab_phrases_w_summary=phrase_group,
                        # model info
                        eager=eager,
                        gpt_model=llm_model,
                        model_params=model_params,
                    )

                batch_requests.append(new_batch_request)

        # Yield control to event loop after each batch
        await asyncio.sleep(0)

        if (i + BATCH_SIZE) % 500 == 0:
            logger.info(
                f"Created {min(i + BATCH_SIZE, len(chunk_items))}/{len(chunk_items)} "
                f"gpt request for {subject_unique_id}:{field_type}"
            )

    return batch_requests


def _create_dummy_completed_phrase_initial_grounding_batch_request(
    deferred_at: datetime,
    subject_unique_id: str,
    llm_phrase_initial_grounding_request_id: BatchRequestIDType,
    model_params: GPTModelParams,
    eager: bool,
) -> GPTBatchRequest:
    if llm_phrase_initial_grounding_request_id is None:
        raise ValueError(
            "_create_dummy_completed_phrase_initial_grounding_batch_request: llm_phrase_initial_grounding_request_id is None"
        )

    base_gpt_batch_request = create_base_gpt_batch_request(
        deferred_at=deferred_at,
        subject_unique_id=subject_unique_id,
        custom_id=llm_phrase_initial_grounding_request_id,
        # Still carries a block, empty — an absent one has to stay an error.
        context=(
            "No initial grounding needed - nothing passed relationship screening "
            "or no phrases were found in the first place.\n"
            f"{render_phrase_summaries_block({})}"
        ),
        prompt_text="No initial grounding needed - nothing passed relationship screening or no phrases were found in the first place.",
        gpt_model=NO_MODEL,
        model_params=model_params,
        batch_id="Eager" if eager else "dummy_phrase_initial_grounding_batch_id",
    )

    base_gpt_batch_request.response = get_dummy_gpt_batch_response(
        deferred_at=deferred_at,
        request_custom_id=llm_phrase_initial_grounding_request_id,
        dummy_chat_completion_id="dummy_completion_id",
        chat_completion_choice_message=ChatCompletionChoiceMessage(
            role="assistant", content='{"groundings": []}'
        ),
    )

    return base_gpt_batch_request


def create_deferred_phrase_initial_grounding_gpt_request(
    deferred_at: datetime,
    subject_unique_id: str,
    llm_phrase_initial_grounding_request_id: str,
    phrase_initial_grounding_prompt: Prompt,
    field_type: ConceptFieldType,
    # context
    subject_name: str,
    all_concepts: set[Concept],
    verified_out_of_vocab_phrases_w_summary: LLMPhraseRelationshipResults,
    # model info
    gpt_model: LLM_Model,
    eager: bool,
    model_params: GPTModelParams,
) -> GPTBatchRequest:
    logger.info(
        f"create_deferred_phrase_initial_grounding_gpt_request: Generating GPTBatchRequest for {llm_phrase_initial_grounding_request_id}"
    )
    all_concept_labels = [
        label for concept in all_concepts for label in concept.matchLabels
    ]

    context = (
        # f"Manufacturer name: {subject_name}\n\n "
        f"extracted phrases:\n"
        f"{render_phrase_summaries_block(verified_out_of_vocab_phrases_w_summary)}\n\n"
        f"options of {field_type.name} to choose from:\n{all_concept_labels}"
    )

    gpt_batch_request = create_base_gpt_batch_request(
        deferred_at=deferred_at,
        subject_unique_id=subject_unique_id,
        custom_id=llm_phrase_initial_grounding_request_id,
        context=context,
        prompt_text=phrase_initial_grounding_prompt.text,
        gpt_model=gpt_model,
        model_params=model_params.with_response_format(
            LLM_PHRASE_INITIAL_GROUNDING_RESPONSE_SCHEMA
        ),
        batch_id="Eager" if eager else None,
    )
    return gpt_batch_request
