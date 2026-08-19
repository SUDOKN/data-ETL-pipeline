import logging
import re
from datetime import datetime
from typing import Any, Optional
from pymongo.errors import BulkWriteError
from pymongo import UpdateOne

from llm_providers.models.open_ai.gpt_batch_response_blob import (
    GPTBatchResponse,
)
from llm_providers.models.open_ai.gpt_batch_response_blob import (
    GPTBatchResponse,
)
from llm_providers.db_models.gpt_batch import GPTBatch
from llm_providers.db_models.gpt_batch_request import (
    GPTBatchRequest,
)

from llm_providers.field_types import BatchRequestIDType

logger = logging.getLogger(__name__)

# str() cast needed because Pylance infers these as non-hashable Pydantic model types
_RESPONSE_FIELD = "response"


async def _bulk_update(
    log_id: str,
    updated_operations: list[UpdateOne],
    index: int,
) -> dict:
    """
    Bulk updates a single chunk of batch requests.
    Only updates if a document exists with the same request.custom_id.

    Returns:
        Dict with keys: updated_count, write_errors, unexpected_error
    """
    try:
        result = await GPTBatchRequest.get_pymongo_collection().bulk_write(
            updated_operations, ordered=False
        )

        upserted_count = result.upserted_count
        modified_count = result.modified_count

        logger.info(
            f"_bulk_update[{log_id}]: Updated {upserted_count} (must be zero), Modified {modified_count} batch requests."
        )

        return {
            "upserted_count": upserted_count,
            "modified_count": modified_count,
            "write_errors": [],
            "unexpected_error": None,
        }

    except BulkWriteError as bwe:
        write_errors = bwe.details.get("writeErrors", [])
        upserted_count = bwe.details.get("nUpserted", 0)
        modified_count = bwe.details.get("nModified", 0)

        logger.info(
            f"_bulk_update[{log_id}]: Upserted {upserted_count} (must be zero), Modified {modified_count} with {len(write_errors)} errors."
        )

        return {
            "upserted_count": upserted_count,
            "modified_count": modified_count,
            "write_errors": write_errors,
            "unexpected_error": None,
        }

    except Exception as e:
        logger.error(f"_bulk_update[{log_id}]: Unexpected error: {e}")

        return {
            "upserted_count": 0,
            "modified_count": 0,
            "write_errors": [],
            "unexpected_error": f"Chunk {index}: {str(e)}",
        }


async def bulk_record_gpt_batch_responses(
    batch_requests: list[GPTBatchRequest],
    response_blobs: list[GPTBatchResponse],
    timestamp: datetime,
) -> tuple[int, int]:
    """
    Bulk record GPT batch responses by their custom IDs.

    Args:
        gpt_batch_requests: List of GPTBatchRequest objects to update
        response_blobs: List of GPTBatchResponseBlob objects containing the responses to record
        timestamp: Timestamp to set for the updated_at field

    Returns:
        Tuple of (number of successfully updated documents, number of failed updates)
    """
    if not batch_requests or not response_blobs:
        return 0, 0
    elif len(batch_requests) != len(response_blobs):
        raise ValueError(
            f"Length of gpt_batch_requests ({len(batch_requests)}) and response_blobs ({len(response_blobs)}) must be the same."
        )

    custom_id_to_blob = {
        blob.request_custom_id: blob
        for blob in response_blobs
        if blob.request_custom_id
    }

    update_operations = []
    for req in batch_requests:
        blob = custom_id_to_blob.get(req.request.custom_id)
        if blob:
            update_operations.append(
                UpdateOne(
                    {GPTBatchRequest.request.custom_id: req.request.custom_id},
                    {
                        "$set": {
                            _RESPONSE_FIELD: blob.model_dump(exclude={"result"}),
                            GPTBatchRequest.updated_at: timestamp,
                        }
                    },
                    upsert=False,
                )
            )

    if not update_operations:
        logger.warning(
            "bulk_record_gpt_batch_responses: No matching custom IDs found between requests and response blobs."
        )
        return 0, 0

    _, modified_count = await bulk_update_gpt_batch_requests(
        update_one_operations=update_operations,
        log_id="record_responses",
    )

    failed_updates = len(update_operations) - modified_count
    logger.info(
        f"bulk_record_gpt_batch_responses: Recorded responses for {modified_count} requests with {failed_updates} failed updates."
    )

    return modified_count, failed_updates


async def bulk_update_gpt_batch_requests(
    update_one_operations: list[UpdateOne],
    log_id: str,
    chunk_size: int = 5000,
) -> tuple[int, int]:
    """
    Bulk upsert GPT batch requests sequentially to avoid write lock contention.

    Args:
        update_one_operations: List of UpdateOne operations to execute
        log_id: Identifier for logging purposes
        chunk_size: Number of requests to upsert per chunk (default: 5000)

    Returns:
        Tuple of (total_upserted, total_modified)

    Raises:
        Exception: If unexpected errors occur during bulk write, with details of all chunk failures
        BulkWriteError: If write errors occur (collected from all chunks)
    """
    if not update_one_operations:
        return 0, 0

    total_requests = len(update_one_operations)
    total_chunks = (total_requests + chunk_size - 1) // chunk_size

    logger.info(
        f"bulk_update_gpt_batch_requests[{log_id}]: Bulk updating {total_requests} GPT batch requests "
        f"in {total_chunks} chunks of {chunk_size} (sequential processing)"
    )

    chunks = [
        (update_one_operations[i : i + chunk_size], chunk_idx + 1)
        for chunk_idx, i in enumerate(range(0, total_requests, chunk_size))
    ]

    all_write_errors = []
    total_upserted = 0
    total_modified = 0
    unexpected_errors = []

    for chunk, chunk_num in chunks:
        result = await _bulk_update(
            log_id=log_id,
            index=chunk_num,
            updated_operations=chunk,
        )

        total_upserted += result["upserted_count"]
        total_modified += result["modified_count"]
        all_write_errors.extend(result["write_errors"])
        if result["unexpected_error"]:
            unexpected_errors.append(result["unexpected_error"])

    logger.info(
        f"bulk_update_gpt_batch_requests[{log_id}]: Completed upserting batch requests \n"
        f"{total_upserted} inserted, {total_modified} modified, "
        f"{len(all_write_errors)} write errors, "
        f"{len(unexpected_errors)} unexpected errors"
    )

    if unexpected_errors:
        raise Exception(
            f"bulk_update_gpt_batch_requests[{log_id}]: Multiple chunk failures, {'; '.join(unexpected_errors)}"
        )

    if all_write_errors:
        error_details = {
            "writeErrors": all_write_errors,
            "nUpserted": total_upserted,
            "nModified": total_modified,
        }
        raise BulkWriteError(error_details)

    return total_upserted, total_modified


def build_request_body_update_document(req: GPTBatchRequest) -> dict:
    """Build the update half of the upsert that creates a batch request doc, or
    refreshes the parts of an existing one derived from the current pipeline state.

    ``request.body`` is ``$set`` rather than write-once because it is derived from
    the current pipeline state and a re-created request must be able to correct it.
    Everything else is identity or bookkeeping and only lands on insert.

    Every persisted field of ``GPTBatchRequest`` must appear here — a field left
    out is silently never written, and surfaces only as a ``None`` far downstream
    at parse time. ``test_gpt_batch_request_writes`` pins that. Field names are
    literals rather than ``GPTBatchRequest.<field>`` expression fields so this
    stays a pure function: Beanie only attaches those attributes to the class
    once ``init_beanie`` has run against a live database.
    """
    return {
        "$set": {
            "request.body": req.request.body.model_dump(),
            "updated_at": req.updated_at,
        },
        "$setOnInsert": {
            "created_at": req.created_at,
            "batch_id": req.batch_id,
            "subject_unique_id": req.subject_unique_id,
            "num_batches_paired_with": req.num_batches_paired_with,
            "request.custom_id": req.request.custom_id,
            "request.method": req.request.method,
            "request.url": req.request.url,
            "request.input_tokens": req.request.input_tokens,
            "response": (req.response.model_dump() if req.response else None),
            "response_parse_errors": req.response_parse_errors,
        },
    }


async def _upsert_chunk_with_only_request_body(
    chunk: list[GPTBatchRequest],
    chunk_num: int,
    total_chunks: int,
    subject_unique_id: str,
) -> dict:
    """
    Upsert a single chunk of batch requests.
    Only inserts if no document exists with the same request.custom_id.

    Returns:
        Dict with keys: upserted_count, modified_count, write_errors, unexpected_error
    """
    try:
        operations = [
            UpdateOne(
                {GPTBatchRequest.request.custom_id: req.request.custom_id},
                build_request_body_update_document(req),
                upsert=True,
            )
            for req in chunk
        ]

        result = await GPTBatchRequest.get_pymongo_collection().bulk_write(
            operations, ordered=False
        )

        upserted_count = result.upserted_count
        modified_count = result.modified_count

        logger.info(
            f"Chunk {chunk_num}/{total_chunks}: Upserted {upserted_count}, "
            f"Modified {modified_count} batch requests for {subject_unique_id}"
        )

        return {
            "upserted_count": upserted_count,
            "modified_count": modified_count,
            "write_errors": [],
            "unexpected_error": None,
        }

    except BulkWriteError as bwe:
        write_errors = bwe.details.get("writeErrors", [])
        upserted_count = bwe.details.get("nUpserted", 0)
        modified_count = bwe.details.get("nModified", 0)

        logger.info(
            f"Chunk {chunk_num}/{total_chunks}: Upserted {upserted_count}, "
            f"Modified {modified_count} with {len(write_errors)} write errors for {subject_unique_id}"
        )

        if write_errors:
            sample_errors = write_errors[:3]
            for idx, err in enumerate(sample_errors, 1):
                error_code = err.get("code")
                error_msg = err.get("errmsg", "N/A")

                if error_code == 121:
                    logger.info(
                        f"  Write error {idx}/{len(write_errors)}: Code={error_code}, "
                        f"Message={error_msg[:200]}"
                    )
                    err_info = err.get("errInfo", {})
                    if err_info:
                        logger.info(f"    Validation details: {err_info}")
                else:
                    logger.info(
                        f"  Write error {idx}/{len(write_errors)}: Code={error_code}, "
                        f"Message={error_msg[:100]}"
                    )

        return {
            "upserted_count": upserted_count,
            "modified_count": modified_count,
            "write_errors": write_errors,
            "unexpected_error": None,
        }

    except Exception as e:
        logger.error(
            f"Chunk {chunk_num}/{total_chunks}: Unexpected error for {subject_unique_id}: {e}"
        )

        return {
            "upserted_count": 0,
            "modified_count": 0,
            "write_errors": [],
            "unexpected_error": f"Chunk {chunk_num}: {str(e)}",
        }


async def bulk_upsert_gpt_batch_requests_with_only_req_bodies(
    batch_requests: list[GPTBatchRequest],
    subject_unique_id: str,
    chunk_size: int = 5000,
) -> tuple[int, int]:
    """
    Bulk upsert GPT batch requests sequentially to avoid write lock contention.

    Args:
        batch_requests: List of GPTBatchRequest objects to upsert
        subject_unique_id: Subject unique id, for logging purposes
        chunk_size: Number of requests to upsert per chunk (default: 5000)

    Returns:
        Tuple of (total_upserted, total_modified)

    Raises:
        Exception: If unexpected errors occur during bulk write
        BulkWriteError: If write errors occur (collected from all chunks)
    """
    if not batch_requests:
        return 0, 0

    total_requests = len(batch_requests)
    total_chunks = (total_requests + chunk_size - 1) // chunk_size

    logger.info(
        f"Bulk upserting {total_requests} GPT batch requests for {subject_unique_id} "
        f"in {total_chunks} chunks of {chunk_size} (sequential processing)"
    )

    chunks = [
        (batch_requests[i : i + chunk_size], chunk_idx + 1)
        for chunk_idx, i in enumerate(range(0, total_requests, chunk_size))
    ]

    all_write_errors = []
    total_upserted = 0
    total_modified = 0
    unexpected_errors = []

    for chunk, chunk_num in chunks:
        result = await _upsert_chunk_with_only_request_body(
            chunk, chunk_num, total_chunks, subject_unique_id
        )

        total_upserted += result["upserted_count"]
        total_modified += result["modified_count"]
        all_write_errors.extend(result["write_errors"])
        if result["unexpected_error"]:
            unexpected_errors.append(result["unexpected_error"])

    logger.info(
        f"Completed upserting batch requests for {subject_unique_id}: "
        f"{total_upserted} inserted, {total_modified} modified, "
        f"{len(all_write_errors)} write errors, "
        f"{len(unexpected_errors)} unexpected errors"
    )

    if unexpected_errors:
        raise Exception(
            f"Multiple chunk failures for {subject_unique_id}: {'; '.join(unexpected_errors)}"
        )

    if all_write_errors:
        error_details = {
            "writeErrors": all_write_errors,
            "nUpserted": total_upserted,
            "nModified": total_modified,
        }
        raise BulkWriteError(error_details)

    return total_upserted, total_modified


async def pair_batch_request_custom_ids_with_batch(
    timestamp: datetime,
    custom_ids: set[str],
    gpt_batch: GPTBatch,
    chunk_size: int = 5000,
) -> int:
    """
    Pair batch request custom IDs with a batch in chunks to avoid write lock contention.

    Args:
        custom_ids: Set of custom IDs to pair with the batch
        gpt_batch: GPTBatch object to pair with
        chunk_size: Number of custom IDs to process per chunk (default: 5000)

    Returns:
        Total number of modified documents
    """
    logger.info(
        f"pair_batch_request_custom_ids_with_batch: Pairing {len(custom_ids):,} custom IDs with batch:{gpt_batch.external_batch_id}"
    )

    if not custom_ids:
        logger.warning(
            f"pair_batch_request_custom_ids_with_batch: No custom IDs provided to pair with batch:{gpt_batch.external_batch_id}"
        )
        return 0

    update_operations = [
        UpdateOne(
            {GPTBatchRequest.request.custom_id: custom_id},
            {
                "$set": {
                    GPTBatchRequest.batch_id: gpt_batch.external_batch_id,
                    GPTBatchRequest.updated_at: timestamp,
                },
                "$inc": {GPTBatchRequest.num_batches_paired_with: 1},
            },
            upsert=False,
        )
        for custom_id in custom_ids
    ]

    _, modified_count = await bulk_update_gpt_batch_requests(
        update_one_operations=update_operations,
        log_id=f"pair_with_{gpt_batch.external_batch_id}",
        chunk_size=chunk_size,
    )

    logger.info(
        f"pair_batch_request_custom_ids_with_batch: Successfully paired {modified_count:,} requests "
        f"with batch:{gpt_batch.external_batch_id}. num_batches_paired_with incremented for all paired requests."
    )

    return modified_count


async def unpair_all_batch_requests_from_batch(
    timestamp: datetime, gpt_batch: GPTBatch, chunk_size: int = 5000
) -> int:
    """
    Reset batch requests associated with a batch in chunks to avoid write lock contention.

    Args:
        gpt_batch: GPTBatch object whose requests should be reset
        chunk_size: Number of requests to reset per chunk (default: 5000)

    Returns:
        Total number of modified documents
    """
    collection = GPTBatchRequest.get_pymongo_collection()

    docs = await collection.find(
        {GPTBatchRequest.batch_id: gpt_batch.external_batch_id},
        projection={GPTBatchRequest.request.custom_id: 1, "_id": 0},
    ).to_list(length=None)

    custom_ids = [doc["request"]["custom_id"] for doc in docs]

    if not custom_ids:
        logger.info(
            f"No batch requests found to reset for batch {gpt_batch.external_batch_id}"
        )
        return 0

    return await unpair_batch_requests_by_custom_ids(
        timestamp=timestamp,
        custom_ids=set(custom_ids),
        chunk_size=chunk_size,
    )


async def unpair_batch_requests_by_custom_ids(
    timestamp: datetime, custom_ids: set[str], chunk_size: int = 5000
) -> int:
    """
    Reset batch requests associated with a batch in chunks to avoid write lock contention.

    Args:
        custom_ids: Set of custom IDs to reset
        chunk_size: Number of requests to reset per chunk (default: 5000)

    Returns:
        Total number of modified documents
    """
    if not custom_ids:
        return 0

    update_operations = [
        UpdateOne(
            {GPTBatchRequest.request.custom_id: custom_id},
            {
                "$set": {
                    GPTBatchRequest.batch_id: None,
                    _RESPONSE_FIELD: None,
                    GPTBatchRequest.updated_at: timestamp,
                }
            },
            upsert=False,
        )
        for custom_id in custom_ids
    ]

    _, modified_count = await bulk_update_gpt_batch_requests(
        update_one_operations=update_operations,
        log_id="unpair_by_custom_ids",
        chunk_size=chunk_size,
    )

    logger.info(
        f"unpair_batch_requests_by_custom_ids: Unpaired {modified_count:,} requests. "
        f"num_batches_paired_with preserved for retry tracking."
    )

    return modified_count


async def bulk_delete_gpt_batch_requests_by_custom_ids(
    gpt_batch_request_custom_ids: list[BatchRequestIDType],
    subject_unique_id: str,
) -> int:
    """
    Bulk delete GPT batch requests by their custom IDs.

    Args:
        gpt_batch_request_custom_ids: List of GPTBatchRequest custom IDs to delete
        subject_unique_id: Subject unique id, for logging purposes

    Returns:
        Number of deleted documents
    """
    if not gpt_batch_request_custom_ids:
        return 0

    result = await GPTBatchRequest.get_pymongo_collection().delete_many(
        {GPTBatchRequest.request.custom_id: {"$in": gpt_batch_request_custom_ids}},
        hint=[(GPTBatchRequest.request.custom_id, 1)],
    )

    logger.info(
        f"Deleted {result.deleted_count} GPT batch requests for {subject_unique_id}"
    )

    return result.deleted_count


# Field names are literals rather than ``GPTBatchRequest.<field>`` expression
# fields so the filter builder below stays a pure function: Beanie only attaches
# those attributes to the class once ``init_beanie`` has run against a live
# database, and the scoping rules are worth testing without one.
_SUBJECT_FIELD = "subject_unique_id"
_CUSTOM_ID_FIELD = "request.custom_id"


def _custom_id_prefix_range(prefix: str) -> dict[str, str]:
    """Range bounds selecting every custom ID starting with *prefix*.

    ``\uffff`` sorts above every character a custom ID can carry, so the
    half-open range covers the prefix and nothing past it, on the index.
    """
    return {"$gte": prefix, "$lt": prefix + "\uffff"}


def build_scoped_delete_filter(
    subject_unique_id: str,
    field_name: Optional[str],
    stage_request_id_tokens: Optional[list[str]] = None,
) -> tuple[dict[str, Any], bool]:
    """The delete filter for a (subject, field, stages) scope, and whether the
    custom_id index bounds it.

    Custom IDs are laid out ``{subject}>{field}>{stage_token}>...``, which is
    what makes each narrowing expressible on the indexed custom_id:

    - field only: one prefix range, ``{subject}>{field}>``.
    - field and stages: one prefix range per stage, ``{subject}>{field}>{token}>``.
    - stages only: a rooted regex, because the stage segment sits behind a field
      segment this call does not know. Rooted on the subject's literal prefix so
      the index still bounds the scan.
    - neither: the subject alone, which the custom_id index cannot bound — hence
      the returned flag, so the caller does not hint an index into a full walk.

    Every token is matched with its trailing ``>``. Without it,
    ``llm_phrase_relationship`` would also select
    ``llm_phrase_relationship_screening``, quietly deleting a stage the caller
    did not name.
    """
    query_filter: dict[str, Any] = {_SUBJECT_FIELD: subject_unique_id}

    if field_name and stage_request_id_tokens:
        query_filter["$or"] = [
            {
                _CUSTOM_ID_FIELD: _custom_id_prefix_range(
                    f"{subject_unique_id}>{field_name}>{token}>"
                )
            }
            for token in stage_request_id_tokens
        ]
    elif field_name:
        query_filter[_CUSTOM_ID_FIELD] = _custom_id_prefix_range(
            f"{subject_unique_id}>{field_name}>"
        )
    elif stage_request_id_tokens:
        alternation = "|".join(re.escape(token) for token in stage_request_id_tokens)
        query_filter[_CUSTOM_ID_FIELD] = {
            "$regex": f"^{re.escape(subject_unique_id)}>[^>]+>(?:{alternation})>"
        }
    else:
        return query_filter, False

    return query_filter, True


async def bulk_delete_gpt_batch_requests_by_subject_id_and_field(
    subject_unique_id: str,
    field_name: Optional[str],
    stage_request_id_tokens: Optional[list[str]] = None,
) -> int:
    """
    Bulk delete GPT batch requests for a subject, narrowed to a field and/or to
    the pipeline stages that issued them — see ``build_scoped_delete_filter``
    for how a scope becomes a filter.

    This layer stays free of pipeline vocabulary on purpose (`llm_providers`
    does not import `core`): the caller resolves stages to custom-ID tokens, via
    ``core.models.pipeline_nodes.base.pipeline_stage.request_id_tokens_from``.

    Args:
        subject_unique_id: Subject unique id for which to delete batch requests
        field_name: Field name to narrow deletion scope, or None for every field
        stage_request_id_tokens: Custom-ID stage tokens to narrow deletion
            scope, or None for every stage

    Returns:
        Number of deleted documents
    """
    query_filter, index_bounds_the_scan = build_scoped_delete_filter(
        subject_unique_id=subject_unique_id,
        field_name=field_name,
        stage_request_id_tokens=stage_request_id_tokens,
    )

    result = await GPTBatchRequest.get_pymongo_collection().delete_many(
        query_filter,
        hint=(
            [(GPTBatchRequest.request.custom_id, 1)] if index_bounds_the_scan else None
        ),
    )

    logger.debug(
        f"Deleted residual {result.deleted_count} GPT batch requests for {subject_unique_id}"
    )

    return result.deleted_count


async def record_response_parse_error(
    gpt_batch_request: GPTBatchRequest,
    error_message: str,
    timestamp: datetime,
    traceback_str: str,
) -> None:
    """
    Record a response parse error in the GPT batch request.

    Args:
        gpt_batch_request: GPTBatchRequest object to update
        error_message: Error message to record
        timestamp: Timestamp of the error occurrence
        traceback_str: Full traceback string
    """
    gpt_batch_request.batch_id = None
    gpt_batch_request.updated_at = timestamp
    gpt_batch_request.response = None
    gpt_batch_request.response_parse_errors.append(
        {
            "timestamp": timestamp.isoformat(),
            "error_message": error_message,
            "traceback": traceback_str,
        }
    )
    await gpt_batch_request.save()


RESPONSE_PARSE_ERROR_CAP = 3


class RepeatedParseFailure(Exception):
    """A request's responses kept failing parse after every allowed retry.

    Raised INSTEAD of recording: recording clears batch_id and response, which
    re-dispatches the request on the next pass, so a response that fails parse
    every time would otherwise cycle forever. The request is left untouched —
    its error history and last response stay inspectable."""


async def record_response_parse_error_capped(
    gpt_batch_request: GPTBatchRequest,
    error_message: str,
    timestamp: datetime,
    traceback_str: str,
) -> None:
    """``record_response_parse_error``, unless the request has already recorded
    ``RESPONSE_PARSE_ERROR_CAP`` parse errors — then ``RepeatedParseFailure``.

    The cap counts recorded errors, so a request gets CAP re-dispatches (CAP+1
    total attempts) before the failure is allowed to surface as the run's
    error. Retries can genuinely differ — system_fingerprint drift — which is
    why re-dispatching is worth anything at all."""
    if len(gpt_batch_request.response_parse_errors) >= RESPONSE_PARSE_ERROR_CAP:
        raise RepeatedParseFailure(
            f"request {gpt_batch_request.request.custom_id} has failed response "
            f"parsing {RESPONSE_PARSE_ERROR_CAP} time(s) already and is out of "
            f"re-dispatches. Latest error: {error_message}"
        )
    await record_response_parse_error(
        gpt_batch_request=gpt_batch_request,
        error_message=error_message,
        timestamp=timestamp,
        traceback_str=traceback_str,
    )
