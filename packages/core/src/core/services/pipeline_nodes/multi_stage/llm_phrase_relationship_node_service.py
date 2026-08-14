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
from core.models.extraction_schemas.relationship import (
    LLMPhraseRelationshipResults,
    PhraseRelationshipResponse,
)
from core.models.extraction_schemas.response_format_util import (
    build_gpt_response_format,
)
from core.utils.label_dedupe_util import dedupe_case_insensitive
from llm_providers.models.file_objects.prompt import Prompt
from llm_providers.models.llm_model import (
    LLM_Model,
    NO_MODEL,
)
from llm_providers.models.open_ai.gpt_batch_response_blob import (
    ChatCompletionChoiceMessage,
)
from core.models.deferred_extraction.deferred_phrase_extraction_requests import (
    LLMPhraseExtractionRequestBundle,
    LLMPhraseExtractionRequestMap,
)
from core.models.deferred_extraction.deferred_concept_extraction import (
    ConceptExtractionRequestBundle,
)

from core.models.field_types import ExtractionFieldType
from llm_providers.models.open_ai.gpt_model_params import (
    GPTModelParams,
)
from llm_providers.field_types import BatchRequestIDType

from llm_providers.services.gpt_batch_request.gpt_batch_request_writes import (
    record_response_parse_error_capped,
)
from core.services.phrase_blocks_contract import (
    hold_response_to_sent_phrases,
    render_phrases_block,
)
from llm_providers.services.gpt_batch_request.gpt_batch_request_service import (
    create_base_gpt_batch_request,
    get_dummy_gpt_batch_response,
)
from core.services.brute_search_service import (
    merge_llm_and_brute_search_results,
)
from core.services.pipeline_nodes.multi_stage.llm_phrase_search_node_service import (
    parse_batch_request_result as parse_phrase_search_batch_req_result,
)
from core.services.pipeline_nodes.multi_stage.llm_phrase_recursive_search_node_service import (
    get_all_recursive_round_results,
)

logger = logging.getLogger(__name__)


LLM_PHRASE_RELATIONSHIP_RESPONSE_SCHEMA = build_gpt_response_format(
    PhraseRelationshipResponse, name="phrase_relationship_result"
)


def parse_llm_phrase_relationship_result(
    gpt_response: Optional[str],
) -> LLMPhraseRelationshipResults:
    if not gpt_response:
        logger.error(f"Invalid gpt_response:{gpt_response}")
        raise ValueError(
            "parse_llm_phrase_relationship_result: Empty or invalid response from GPT"
        )

    try:
        parsed = PhraseRelationshipResponse.model_validate_json(gpt_response)
    except ValidationError as e:
        raise ValueError(
            f"parse_llm_phrase_relationship_result: Invalid response from GPT:{gpt_response}"
        ) from e

    raw_gpt_phrase_relationship_result: LLMPhraseRelationshipResults = {}
    for entry in parsed.relationships:
        if entry.phrase in raw_gpt_phrase_relationship_result:
            raise ValueError(
                f"parse_llm_phrase_relationship_result: Duplicate phrase {entry.phrase!r} in relationships response"
            )
        raw_gpt_phrase_relationship_result[entry.phrase] = entry.description

    logger.debug(
        f"raw_gpt_phrase_relationship_result:{raw_gpt_phrase_relationship_result}"
    )

    return raw_gpt_phrase_relationship_result


async def parse_phrase_relationship_group_result(
    subject_unique_id: str,
    field_type: ExtractionFieldType,
    chunk_bounds: str,
    group_req_id: BatchRequestIDType,
    completed_request_map: dict[BatchRequestIDType, GPTBatchRequest],
    timestamp: datetime,
    repairs: Optional[dict[str, str]] = None,
) -> LLMPhraseRelationshipResults:
    """Parse the phrase→description pairs returned by a single relationship group."""
    req_obj = completed_request_map.get(group_req_id)
    if not req_obj:
        raise ValueError(
            f"phrase_relationship_node.parse_batch_request_result: Missing GPTBatchRequest for phrase_relationship request ID {group_req_id} in {subject_unique_id}:{field_type.name}"
        )
    elif not req_obj.response:
        raise ValueError(
            f"phrase_relationship_node.parse_batch_request_result: GPTBatchRequest for phrase_relationship request ID {group_req_id} has no response_blob in {subject_unique_id}:{field_type.name}"
        )

    try:
        phrase_relationship_results = parse_llm_phrase_relationship_result(
            req_obj.response.result
        )
        # The stage where phrase IDENTITY is set. Its response keys become the
        # phrase for screening, for grounding and for the trail join, so a drift
        # here does not crash — it silently renames the phrase, and _provenance
        # then matches no search round. `raise`, because relationship is a TOTAL
        # function of its candidate list: measured over the steelcraft.com run,
        # 381 sent and 381 returned, nothing dropped and nothing invented.
        return hold_response_to_sent_phrases(
            user_message=req_obj.request.body.user_message(),
            response_by_phrase=phrase_relationship_results,
            where=f"{subject_unique_id}:{field_type.name} relationship {group_req_id}",
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
            f"phrase_relationship_node.parse_batch_request_result: Error parsing phrase_relationship results for subject {subject_unique_id} from GPT response: {e}"
        )
        raise


async def get_phrase_relationship_result(
    subject_unique_id: str,
    field_type: ExtractionFieldType,
    chunk_bounds: str,
    extraction_bundle: LLMPhraseExtractionRequestBundle,
    completed_request_map: dict[BatchRequestIDType, GPTBatchRequest],
    timestamp: datetime,
    repairs: Optional[dict[str, str]] = None,
) -> LLMPhraseRelationshipResults:
    """Merge relationship descriptions across every group embedded for the chunk.

    ``repairs`` is forwarded to the per-group hold; see
    ``hold_response_to_sent_phrases``.
    """
    group_req_ids = extraction_bundle.llm_phrase_relationship_req_ids
    if not group_req_ids:
        raise ValueError(
            f"phrase_relationship_node.parse_batch_request_result: phrase_relationship_req_ids is empty for chunk bounds {chunk_bounds} in {subject_unique_id}:{field_type.name}"
        )

    merged_results: LLMPhraseRelationshipResults = {}
    for group_req_id in group_req_ids:
        merged_results.update(
            await parse_phrase_relationship_group_result(
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


async def get_relationship_candidates(
    subject_unique_id: str,
    field_type: ExtractionFieldType,
    chunk_bounds: str,
    extraction_bundle: LLMPhraseExtractionRequestBundle,
    llm_phrase_search_gpt_request_map: dict[BatchRequestIDType, GPTBatchRequest],
    llm_phrase_recursive_search_gpt_request_map: dict[
        BatchRequestIDType, GPTBatchRequest
    ],
    timestamp: datetime,
) -> list[str]:
    """The chunk's full candidate phrase list, exactly as the relationship stage
    consumes it: first search ∪ every recursive round ∪ brute survivors, casefold
    deduped and sorted.

    Single source of truth on purpose. The node embeds one request id per group of
    `max_phrases_per_request` candidates and so has to count them *before* any
    request exists, while request creation needs the phrases themselves; both go
    through here so the two can never disagree about how many groups a chunk has.
    """
    llm_phrase_search_results = await parse_phrase_search_batch_req_result(
        subject_unique_id=subject_unique_id,
        field_type=field_type,
        chunk_bounds=chunk_bounds,
        extraction_bundle=extraction_bundle,
        all_phrase_search_req_responses_map=llm_phrase_search_gpt_request_map,
        deferred_at=timestamp,
    )

    llm_phrase_recursive_search_results = await get_all_recursive_round_results(
        subject_unique_id=subject_unique_id,
        field_type=field_type,
        chunk_bounds=chunk_bounds,
        extraction_bundle=extraction_bundle,
        completed_request_map=llm_phrase_recursive_search_gpt_request_map,
        timestamp=timestamp,
    )

    llm_phrase_search_results |= llm_phrase_recursive_search_results

    # Combine LLM search with brute force results (concept pipelines only) to get the
    # final search results for the chunk, which are used as context for phrase_relationship.
    # We include all brute force results as phrase_relationship candidates even if
    # they weren't identified by the LLM search, because the brute force results were
    # generated with high recall in mind and we don't want to miss out on any potential
    # phrase by filtering them with the LLM search results. Keyword pipelines have no
    # brute force phase
    merged_search_results = merge_llm_and_brute_search_results(
        llm_search_results=llm_phrase_search_results,
        brute_search_results=(
            extraction_bundle.brute
            if type(extraction_bundle) == ConceptExtractionRequestBundle
            else set()
        ),
    )

    # Sorted, not set-ordered: this list is rendered straight into the
    # prompt, and a `set` iterates in an order that changes between
    # processes under hash randomisation. That silently varied the prompt
    # text run to run, which no `seed` can compensate for — two runs over
    # identical upstream results were asking different questions. With
    # grouping the ordering carries more weight still: it also decides which
    # phrases share a request.
    #
    # Casefold dedup is mechanical only ("Tooling"/"tooling"); phrases
    # that differ by more than case are the search prompts' business to
    # not emit twice, and whatever still arrives in duplicate is left for
    # the reconcile-level dedupe.
    return sorted(dedupe_case_insensitive(merged_search_results))


def _split_into_phrase_groups(
    candidates: list[str], group_size: int
) -> list[list[str]]:
    """Split an ordered candidate phrase list into ordered groups of at most
    *group_size* phrases. Always returns at least one (possibly empty) group so the
    existing single-dummy-request-per-chunk fallback keeps working when a chunk
    has no candidate phrases at all."""
    if not candidates:
        return [[]]
    return [
        candidates[i : i + group_size] for i in range(0, len(candidates), group_size)
    ]


async def create_missing_phrase_relationship_requests(
    subject_unique_id: str,
    subject_name: str,
    field_type: ExtractionFieldType,  # used for logging and debugging
    chunked_request_map: LLMPhraseExtractionRequestMap,
    missing_phrase_relationship_req_ids: set[BatchRequestIDType],
    subject_text: str,
    phrase_relationship_prompt: Prompt,
    llm_phrase_search_gpt_request_map: dict[BatchRequestIDType, GPTBatchRequest],
    timestamp: datetime,
    llm_model: LLM_Model,
    eager: bool,
    model_params: GPTModelParams,
    llm_phrase_recursive_search_gpt_request_map: dict[
        BatchRequestIDType, GPTBatchRequest
    ],
    max_phrases_per_request: int,
    BATCH_SIZE=100,
) -> list[GPTBatchRequest]:
    logger.info(
        f"create_missing_phrase_relationship_requests: Generating GPTBatchRequests for {subject_unique_id}:{field_type}"
    )

    batch_requests: list[GPTBatchRequest] = []
    chunk_items = [
        (chunk_bounds, bundle)
        for chunk_bounds, bundle in chunked_request_map.items()
        if set(bundle.llm_phrase_relationship_req_ids)
        & missing_phrase_relationship_req_ids
    ]
    # Create lookup map: custom_id -> GPTBatchRequest
    if not llm_phrase_search_gpt_request_map:
        raise ValueError(
            f"create_missing_phrase_relationship_requests: No completed GPTBatchRequests found for {subject_unique_id}:{field_type} in upstream_completed_batch_req_map"
        )

    # Process chunks in batches to yield control periodically
    for i in range(0, len(chunk_items), BATCH_SIZE):
        batch = chunk_items[i : i + BATCH_SIZE]

        # Process current batch
        for chunk_bounds, extraction_bundle in batch:
            all_search_results = await get_relationship_candidates(
                subject_unique_id=subject_unique_id,
                field_type=field_type,
                chunk_bounds=chunk_bounds,
                extraction_bundle=extraction_bundle,
                llm_phrase_search_gpt_request_map=llm_phrase_search_gpt_request_map,
                llm_phrase_recursive_search_gpt_request_map=llm_phrase_recursive_search_gpt_request_map,
                timestamp=timestamp,
            )
            phrase_groups = _split_into_phrase_groups(
                all_search_results, max_phrases_per_request
            )
            group_req_ids = extraction_bundle.llm_phrase_relationship_req_ids
            if len(group_req_ids) != len(phrase_groups):
                raise ValueError(
                    f"create_missing_phrase_relationship_requests: embedded group count "
                    f"({len(group_req_ids)}) does not match computed group count ({len(phrase_groups)}) "
                    f"for chunk bounds {chunk_bounds} in {subject_unique_id}:{field_type}. Group counts are "
                    f"computed once, upfront, from the same candidate list, so this should not happen."
                )

            start, end = int(chunk_bounds.split(":")[0]), int(
                chunk_bounds.split(":")[1]
            )

            for group_index, group_req_id in enumerate(group_req_ids):
                if group_req_id not in missing_phrase_relationship_req_ids:
                    continue

                phrase_group = phrase_groups[group_index]
                if not phrase_group:
                    # _split_into_phrase_groups only ever produces an empty group as the
                    # sole element of a single-group list (zero candidate phrases total)
                    if len(phrase_groups) != 1:
                        raise ValueError(
                            f"create_missing_phrase_relationship_requests: unexpected empty "
                            f"phrase group at index {group_index} of {len(phrase_groups)} groups for chunk "
                            f"bounds {chunk_bounds} in {subject_unique_id}:{field_type}. Only a single group should "
                            f"ever be empty (the zero-candidate-phrases case)."
                        )
                    # add a dummy response blob with empty dict
                    logger.info(
                        f"No phrases found in text, for {subject_unique_id}:{field_type}, creating dummy phrase_relationship request"
                    )
                    new_batch_request = (
                        _create_dummy_completed_phrase_relationship_batch_request(
                            deferred_at=timestamp,
                            subject_unique_id=subject_unique_id,
                            llm_phrase_relationship_request_id=group_req_id,
                            model_params=model_params,
                            eager=eager,
                        )
                    )
                else:
                    logger.info(
                        f"Passing on candidates {phrase_group} to phrase_relationship phase for "
                        f"{subject_unique_id}:{field_type} chunk {chunk_bounds} group {group_index}"
                    )
                    new_batch_request = create_deferred_phrase_relationship_gpt_request(
                        deferred_at=timestamp,
                        subject_unique_id=subject_unique_id,
                        llm_phrase_relationship_request_id=group_req_id,
                        subject_name=subject_name,
                        subject_text=subject_text[start:end],
                        search_results=phrase_group,
                        phrase_relationship_prompt=phrase_relationship_prompt,
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


def _create_dummy_completed_phrase_relationship_batch_request(
    deferred_at: datetime,
    subject_unique_id: str,
    llm_phrase_relationship_request_id: BatchRequestIDType,
    model_params: GPTModelParams,
    eager: bool,
) -> GPTBatchRequest:
    if llm_phrase_relationship_request_id is None:
        raise ValueError(
            "_create_dummy_completed_phrase_relationship_batch_request: llm_phrase_relationship_request_id is None"
        )

    base_gpt_batch_request = create_base_gpt_batch_request(
        deferred_at=deferred_at,
        subject_unique_id=subject_unique_id,
        custom_id=llm_phrase_relationship_request_id,
        context="No phrase_relationship needed - no phrases found in text.",
        prompt_text="No phrase relationships needed - no phrases found in text brute force or by LLM.",
        gpt_model=NO_MODEL,
        model_params=model_params,
        batch_id="Eager" if eager else "dummy_phrase_relationship_batch_id",
    )

    base_gpt_batch_request.response = get_dummy_gpt_batch_response(
        deferred_at=deferred_at,
        request_custom_id=llm_phrase_relationship_request_id,
        dummy_chat_completion_id="dummy_completion_id",
        chat_completion_choice_message=ChatCompletionChoiceMessage(
            role="assistant", content='{"relationships": []}'
        ),
    )

    return base_gpt_batch_request


def create_deferred_phrase_relationship_gpt_request(
    deferred_at: datetime,
    subject_unique_id: str,
    llm_phrase_relationship_request_id: str,
    subject_name: str,
    subject_text: str,
    search_results: list[str],
    phrase_relationship_prompt: Prompt,
    gpt_model: LLM_Model,
    eager: bool,
    model_params: GPTModelParams,
) -> GPTBatchRequest:
    logger.info(
        f"create_deferred_phrase_relationship_gpt_request: Generating GPTBatchRequest for {llm_phrase_relationship_request_id}"
    )
    # The block, not `{search_results}`: a Python list repr is not JSON, and this
    # is the one context that embeds the raw scraped chunk — which sits BEFORE
    # the block and could otherwise forge the old bare marker. See
    # phrase_blocks_contract on why the format is fenced.
    context = (
        f"the name of the manufacturer in question: {subject_name}\n\n"
        f"scraped text from their website:\n{subject_text}\n\n"
        # Phrases alone: this stage runs before summaries exist to pair them with.
        f"{render_phrases_block(search_results)}"
    )

    gpt_batch_request = create_base_gpt_batch_request(
        deferred_at=deferred_at,
        subject_unique_id=subject_unique_id,
        custom_id=llm_phrase_relationship_request_id,
        context=context,
        prompt_text=phrase_relationship_prompt.text,
        gpt_model=gpt_model,
        model_params=model_params.with_response_format(
            LLM_PHRASE_RELATIONSHIP_RESPONSE_SCHEMA
        ),
        batch_id="Eager" if eager else None,
    )
    return gpt_batch_request
