import asyncio
import json
import logging
import re
import traceback
from collections import Counter
from datetime import datetime
from typing import Optional

from pydantic import ValidationError

from llm_providers.db_models.gpt_batch_request import (
    GPTBatchRequest,
)
from core.models.extraction_schemas.response_format_util import (
    build_gpt_response_format,
)
from core.models.extraction_schemas.search import (
    LLMSearchResults,
    PhraseSearchResponse,
)
from llm_providers.models.file_objects.prompt import Prompt
from llm_providers.models.llm_model import LLM_Model
from core.models.deferred_extraction.deferred_phrase_extraction_requests import (
    LLMPhraseExtractionRequestMap,
    LLMPhraseExtractionRequestBundle,
)
from core.models.field_types import (
    ExtractionFieldType,
)
from llm_providers.field_types import BatchRequestIDType
from llm_providers.models.open_ai.gpt_model_params import (
    GPTModelParams,
)

from llm_providers.services.gpt_batch_request.gpt_batch_request_writes import (
    record_response_parse_error_capped,
)
from llm_providers.services.gpt_batch_request.gpt_batch_request_service import (
    create_base_gpt_batch_request,
)

logger = logging.getLogger(__name__)


# Shared by both llm_search and llm_phrase_recursive_search (identical shape).
LLM_SEARCH_RESPONSE_SCHEMA = build_gpt_response_format(
    PhraseSearchResponse, name="phrase_search_result"
)


class SearchResponseParseError(ValueError):
    """A search or recursive-search response that could not be read as phrases.

    The raw response travels on the exception instead of inside its message.
    Recording a parse error nulls the request's stored response, so the error
    record is the only surviving copy of what the model said and has to keep the
    whole thing — while the message itself, which is what surfaces in the run's
    traceback, stays a stat line. A degenerate response is 124 KB.
    """

    def __init__(self, message: str, *, raw_response: Optional[str]) -> None:
        super().__init__(message)
        self.raw_response = raw_response

    def as_error_record(self) -> str:
        if not self.raw_response:
            return str(self)
        return f"{self}\nRAW RESPONSE:\n{self.raw_response}"


def parse_error_record(error: Exception) -> str:
    """What to persist for a parse failure — the full response where we have it."""
    if isinstance(error, SearchResponseParseError):
        return error.as_error_record()
    return str(error)


# The only shape worth salvaging: an object whose first key is `phrases`, opening
# its array. Anything else that fails validation is a different failure.
_PHRASES_ARRAY_PREFIX = re.compile(r'\s*\{\s*"phrases"\s*:\s*\[')

# A phrase list repeats a little for honest reasons — the same phrase found in
# two sub-windows, a near-duplicate wording — but a model that has fallen into a
# repetition loop returns the same string thousands of times. `set()` absorbs
# both silently, so the ratio is reported here or nowhere.
REPETITION_REPORT_MIN_PHRASES = 50
REPETITION_REPORT_RATIO = 2.0

_ELEMENT_DECODER = json.JSONDecoder()


def _salvage_truncated_phrases(gpt_response: str) -> Optional[list[str]]:
    """Recover the complete elements of a `{"phrases": [...` array that the model
    left unterminated by running into its completion cap.

    Returns None unless the text is exactly that shape — the prefix, a run of
    complete string elements, then either the end of the text or an element cut
    mid-string. A response that closed its array failed validation for some
    other reason, and one holding non-strings is not this schema's answer at
    all; neither is ours to repair, so both keep raising.
    """
    prefix = _PHRASES_ARRAY_PREFIX.match(gpt_response)
    if not prefix:
        return None

    phrases: list[str] = []
    index = prefix.end()
    length = len(gpt_response)
    while True:
        while index < length and gpt_response[index] in ', \t\r\n':
            index += 1
        if index >= length:
            break  # ran out of text between elements
        if gpt_response[index] != '"':
            return None  # a closing "]", or something that is not a string
        try:
            phrase, index = _ELEMENT_DECODER.raw_decode(gpt_response, index)
        except ValueError:
            break  # the element itself was cut mid-string
        if not isinstance(phrase, str):
            return None
        phrases.append(phrase)

    return phrases or None


def _report_repetition(phrases: list[str], where: str) -> None:
    if len(phrases) < REPETITION_REPORT_MIN_PHRASES:
        return
    unique = set(phrases)
    if len(phrases) < REPETITION_REPORT_RATIO * len(unique):
        return
    most_repeated, count = Counter(phrases).most_common(1)[0]
    logger.warning(
        f"parse_llm_search_response: repetitive response{where} — "
        f"{len(phrases)} phrases, {len(unique)} unique; "
        f"{most_repeated!r} returned {count} times"
    )


def parse_llm_search_response(
    gpt_response: Optional[str], where: str = ""
) -> LLMSearchResults:
    """The phrases of one search or recursive-search response.

    A response truncated by the completion cap is salvaged down to its complete
    elements rather than discarded: the loss is the tail the model was still
    writing, and treating the whole answer as unreadable costs the round its
    real content (one 20,000-token runaway held 48 distinct phrases before it
    started repeating itself).
    """
    if not gpt_response:
        raise SearchResponseParseError(
            f"parse_llm_search_response: Empty or invalid response from GPT{where}",
            raw_response=gpt_response,
        )

    try:
        phrases = PhraseSearchResponse.model_validate_json(gpt_response).phrases
    except ValidationError as e:
        salvaged = _salvage_truncated_phrases(gpt_response)
        if salvaged is None:
            raise SearchResponseParseError(
                f"parse_llm_search_response: Invalid response from GPT{where} "
                f"({len(gpt_response)} chars, unsalvageable)",
                raw_response=gpt_response,
            ) from e
        logger.error(
            f"parse_llm_search_response: truncated response{where} "
            f"({len(gpt_response)} chars) — salvaged {len(salvaged)} complete "
            f"phrases, {len(set(salvaged))} unique"
        )
        phrases = salvaged

    _report_repetition(phrases, where)

    llm_results: set[str] = set(phrases)
    logger.debug(f"llm_results:{llm_results}")

    return llm_results


async def parse_search_sub_request_result(
    subject_unique_id: str,
    field_type: ExtractionFieldType,
    llm_phrase_search_request_id: BatchRequestIDType,
    all_phrase_search_req_responses_map: dict[BatchRequestIDType, GPTBatchRequest],
    deferred_at: datetime,
) -> LLMSearchResults:
    """Parse the phrases of ONE sub-window's first-search request. Strict: a
    missing request or response raises, unlike the recursive-round parser."""
    llm_search_req = all_phrase_search_req_responses_map.get(
        llm_phrase_search_request_id
    )
    if not llm_search_req:
        raise ValueError(
            f"search_node.parse_search_sub_request_result: Missing GPTBatchRequest for search request ID {llm_phrase_search_request_id} in {subject_unique_id}:{field_type.name}"
        )
    elif not llm_search_req.response:
        raise ValueError(
            f"search_node.parse_search_sub_request_result: GPTBatchRequest for search request ID {llm_phrase_search_request_id} has no response_blob in {subject_unique_id}:{field_type.name}"
        )

    try:
        llm_search_results = parse_llm_search_response(
            llm_search_req.response.result,
            where=f" for {llm_phrase_search_request_id}",
        )
        return llm_search_results
    except Exception as e:
        await record_response_parse_error_capped(
            gpt_batch_request=llm_search_req,
            error_message=parse_error_record(e),
            timestamp=deferred_at,
            traceback_str=traceback.format_exc(),
        )
        logger.error(
            f"search_node.parse_search_sub_request_result: Error parsing concept search results for subject {subject_unique_id} from GPT response: {e}"
        )
        raise


async def parse_batch_request_result(
    subject_unique_id: str,
    field_type: ExtractionFieldType,
    chunk_bounds: str,
    extraction_bundle: LLMPhraseExtractionRequestBundle,
    all_phrase_search_req_responses_map: dict[  # contains requests of this phase(which can be identified by its particular custom id) across all chunks
        BatchRequestIDType, GPTBatchRequest
    ],
    deferred_at: datetime,
) -> LLMSearchResults:
    """Union of first-search phrases across the chunk's sub-windows.

    This is THE chunk-level view of the first search — every downstream consumer
    (relationship candidates, grounding, stats, dumps) reads through here, which
    is what keeps the sub-window split invisible below the search stages.
    """
    if not extraction_bundle.llm_phrase_search_req_ids:
        raise ValueError(
            f"search_node.parse_batch_request_result: llm_phrase_search_req_ids is empty for chunk bounds {chunk_bounds} in {subject_unique_id}:{field_type.name}"
        )

    llm_search_results: set[str] = set()
    for llm_phrase_search_request_id in extraction_bundle.llm_phrase_search_req_ids:
        llm_search_results |= await parse_search_sub_request_result(
            subject_unique_id=subject_unique_id,
            field_type=field_type,
            llm_phrase_search_request_id=llm_phrase_search_request_id,
            all_phrase_search_req_responses_map=all_phrase_search_req_responses_map,
            deferred_at=deferred_at,
        )
    return llm_search_results


async def create_missing_phrase_search_requests(
    deferred_at: datetime,
    field_type: ExtractionFieldType,  # used for logging and debugging
    missing_search_req_ids: set[BatchRequestIDType],
    chunked_request_map: LLMPhraseExtractionRequestMap,
    subject_unique_id: str,
    subject_text: str,
    search_prompt: Prompt,
    llm_model: LLM_Model,
    model_params: GPTModelParams,
    eager: bool,
    BATCH_SIZE=100,
) -> list[GPTBatchRequest]:

    logger.info(
        f"create_missing_phrase_search_requests: Generating GPTBatchRequest for {subject_unique_id}:{field_type.name}"
    )

    batch_requests: list[GPTBatchRequest] = []
    chunk_items: list[tuple[BatchRequestIDType, str]] = []
    for (
        chunk_bounds,
        extraction_bundle,
    ) in chunked_request_map.items():
        for sub_bounds, search_req_id in zip(
            extraction_bundle.search_sub_bounds,
            extraction_bundle.llm_phrase_search_req_ids,
        ):
            if search_req_id in missing_search_req_ids:
                start = sub_bounds.split(":")[0]
                end = sub_bounds.split(":")[1]
                chunk_items.append(
                    (
                        search_req_id,
                        subject_text[int(start) : int(end)],
                    )
                )

    # Process chunks in batches to yield control periodically
    for i in range(0, len(chunk_items), BATCH_SIZE):
        batch = chunk_items[i : i + BATCH_SIZE]

        # Process current batch
        for llm_search_request_id, chunk_text in batch:
            llm_batch_request = create_base_gpt_batch_request(
                deferred_at=deferred_at,
                subject_unique_id=subject_unique_id,
                custom_id=llm_search_request_id,
                context=chunk_text,
                prompt_text=search_prompt.text,
                gpt_model=llm_model,
                model_params=model_params.with_response_format(
                    LLM_SEARCH_RESPONSE_SCHEMA
                ),
                batch_id="Eager" if eager else None,
            )

            batch_requests.append(llm_batch_request)

        # Yield control to event loop after each batch
        await asyncio.sleep(0)

        if (i + BATCH_SIZE) % 500 == 0:
            logger.info(
                f"Created {min(i + BATCH_SIZE, len(chunk_items))}/{len(chunk_items)} "
                f"gpt request for {subject_unique_id}:{field_type} (Eager: {eager})"
            )

    return batch_requests
