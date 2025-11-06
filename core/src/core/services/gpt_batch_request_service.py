from datetime import datetime
import logging
from typing import Optional

from data_etl_app.models.types_and_enums import GenericFieldTypeEnum
from pymongo.errors import BulkWriteError
from pymongo import UpdateOne

from core.models.prompt import Prompt
from core.models.db.gpt_batch import GPTBatch
from core.models.db.gpt_batch_request import GPTBatchRequest

from open_ai_key_app.models.field_types import GPTBatchRequestCustomID
from open_ai_key_app.models.gpt_model import (
    GPTModel,
    ModelParameters,
)
from open_ai_key_app.utils.batch_gpt_util import (
    get_gpt_request_blob,
)

logger = logging.getLogger(__name__)


def create_base_gpt_batch_request(
    deferred_at: datetime,
    custom_id: str,
    context: str,
    prompt: Prompt,
    gpt_model: GPTModel,
    model_params: ModelParameters,
) -> GPTBatchRequest:
    request_blob = get_gpt_request_blob(
        custom_id=custom_id,
        context=context,
        prompt=prompt.text,
        gpt_model=gpt_model,
        model_params=model_params,
    )

    gpt_batch_request = GPTBatchRequest(
        created_at=deferred_at,
        batch_id=None,
        request=request_blob,
    )

    return gpt_batch_request


def is_batch_request_pending(
    gpt_batch_request: GPTBatchRequest,
) -> bool:
    return (
        gpt_batch_request.batch_id
        is None
        # and gpt_batch_request.response_blob is None
    )


async def find_gpt_batch_request_by_custom_id(
    gpt_batch_request_custom_id: GPTBatchRequestCustomID,
) -> Optional[GPTBatchRequest]:
    gpt_batch_request = await GPTBatchRequest.find_one(
        GPTBatchRequest.request.custom_id == gpt_batch_request_custom_id
    )

    return gpt_batch_request


async def find_completed_gpt_batch_request_by_custom_id(
    gpt_batch_request_custom_id: GPTBatchRequestCustomID,
) -> Optional[GPTBatchRequest]:
    if gpt_batch_request_custom_id is None:
        raise ValueError("gpt_batch_request_custom_id cannot be None")

    gpt_batch_request = await GPTBatchRequest.find_one(
        GPTBatchRequest.request.custom_id == gpt_batch_request_custom_id,
        GPTBatchRequest.batch_id != None,
        GPTBatchRequest.response_blob != None,
    )

    return gpt_batch_request


async def find_gpt_batch_requests_by_custom_ids(
    gpt_batch_request_custom_ids: list[GPTBatchRequestCustomID],
) -> dict[GPTBatchRequestCustomID, GPTBatchRequest]:
    if gpt_batch_request_custom_ids is None:
        raise ValueError("gpt_batch_request_custom_ids cannot be None")
    elif len(gpt_batch_request_custom_ids) == 0:
        raise ValueError("gpt_batch_request_custom_ids cannot be empty")
    elif any(cid is None for cid in gpt_batch_request_custom_ids):
        raise ValueError("gpt_batch_request_custom_ids cannot contain None values")

    request_map: dict[GPTBatchRequestCustomID, GPTBatchRequest] = dict()
    async for gpt_req in GPTBatchRequest.find(
        {"request.custom_id": {"$in": gpt_batch_request_custom_ids}}
    ):
        request_map[gpt_req.request.custom_id] = gpt_req

    return request_map


async def find_gpt_batch_request_ids_only(
    gpt_batch_request_custom_ids: list[GPTBatchRequestCustomID | None],
) -> set[GPTBatchRequestCustomID]:
    if gpt_batch_request_custom_ids is None:
        raise ValueError("gpt_batch_request_custom_ids cannot be None")
    elif len(gpt_batch_request_custom_ids) == 0:
        raise ValueError("gpt_batch_request_custom_ids cannot be empty")
    elif any(cid is None for cid in gpt_batch_request_custom_ids):
        raise ValueError("gpt_batch_request_custom_ids cannot contain None values")

    gpt_req_ids_found = set()
    collection = GPTBatchRequest.get_pymongo_collection()

    cursor = collection.find(
        {
            "request.custom_id": {"$in": gpt_batch_request_custom_ids},
        },
        projection={"request.custom_id": 1, "_id": 0},
    )

    async for doc in cursor:
        gpt_req_ids_found.add(doc["request"]["custom_id"])

    return gpt_req_ids_found


async def find_completed_gpt_batch_requests_by_custom_ids(
    gpt_batch_request_custom_ids: list[GPTBatchRequestCustomID],
) -> dict[GPTBatchRequestCustomID, GPTBatchRequest]:
    if gpt_batch_request_custom_ids is None:
        raise ValueError("gpt_batch_request_custom_ids cannot be None")
    elif len(gpt_batch_request_custom_ids) == 0:
        raise ValueError("gpt_batch_request_custom_ids cannot be empty")
    elif any(cid is None for cid in gpt_batch_request_custom_ids):
        raise ValueError("gpt_batch_request_custom_ids cannot contain None values")

    request_map: dict[GPTBatchRequestCustomID, GPTBatchRequest] = dict()
    async for gpt_req in GPTBatchRequest.find(
        {
            "request.custom_id": {"$in": gpt_batch_request_custom_ids},
            "batch_id": {"$ne": None},
            "response_blob": {"$ne": None},
        },
    ):
        request_map[gpt_req.request.custom_id] = gpt_req

    return request_map


async def find_completed_gpt_batch_request_ids_only(
    gpt_batch_request_custom_ids: list[GPTBatchRequestCustomID | None],
) -> set[GPTBatchRequestCustomID]:
    if gpt_batch_request_custom_ids is None:
        raise ValueError("gpt_batch_request_custom_ids cannot be None")
    elif len(gpt_batch_request_custom_ids) == 0:
        raise ValueError("gpt_batch_request_custom_ids cannot be empty")
    elif any(cid is None for cid in gpt_batch_request_custom_ids):
        raise ValueError("gpt_batch_request_custom_ids cannot contain None values")

    gpt_req_ids_found = set()
    collection = GPTBatchRequest.get_pymongo_collection()

    cursor = collection.find(
        {
            "request.custom_id": {"$in": gpt_batch_request_custom_ids},
            "batch_id": {"$ne": None},
            "response_blob": {"$ne": None},
        },
        projection={"request.custom_id": 1, "_id": 0},
    )

    async for doc in cursor:
        gpt_req_ids_found.add(doc["request"]["custom_id"])

    return gpt_req_ids_found


async def _bulk_update_chunk(
    log_id: str,
    chunk_operations: list[UpdateOne],
    chunk_num: int,
    total_chunks: int,
) -> dict:
    """
    Bulk updates a single chunk of batch requests.
    Only updates if a document exists with the same request.custom_id.

    Returns:
        Dict with keys: updated_count, write_errors, unexpected_error
    """
    try:
        result = await GPTBatchRequest.get_pymongo_collection().bulk_write(
            chunk_operations, ordered=False
        )

        upserted_count = result.upserted_count
        modified_count = result.modified_count

        logger.info(
            f"_bulk_update_chunk[{log_id}]: Chunk {chunk_num}/{total_chunks}: "
            f"Upserted {upserted_count} (must be zero), Modified {modified_count} batch requests."
        )

        return {
            "upserted_count": upserted_count,
            "modified_count": modified_count,
            "write_errors": [],
            "unexpected_error": None,
        }

    except BulkWriteError as bwe:
        # Collect write errors (like duplicate keys, though shouldn't happen with upsert)
        write_errors = bwe.details.get("writeErrors", [])
        upserted_count = bwe.details.get("nUpserted", 0)
        modified_count = bwe.details.get("nModified", 0)

        logger.info(
            f"_bulk_update_chunk[{log_id}]: Chunk {chunk_num}/{total_chunks}: "
            f"Upserted {upserted_count} (must be zero), Modified {modified_count} with {len(write_errors)} errors."
        )

        return {
            "upserted_count": upserted_count,
            "modified_count": modified_count,
            "write_errors": write_errors,
            "unexpected_error": None,
        }

    except Exception as e:
        # Unexpected error - entire chunk failed
        logger.error(
            f"_bulk_update_chunk[{log_id}]: Chunk {chunk_num}/{total_chunks}, Unexpected error: {e}"
        )

        return {
            "upserted_count": 0,
            "modified_count": 0,
            "write_errors": [],
            "unexpected_error": f"Chunk {chunk_num}: {str(e)}",
        }


async def bulk_update_gpt_batch_requests(
    update_one_operations: list[UpdateOne],
    log_id: str,
    chunk_size: int = 5000,
) -> tuple[int, int]:
    """
    Bulk upsert GPT batch requests sequentially to avoid write lock contention.

    Args:
        batch_requests: List of GPTBatchRequest objects to upsert
        mfg_etld1: Manufacturer etld1 for logging purposes
        chunk_size: Number of requests to upsert per chunk (default: 5000)

    Returns:
        Tuple of (total_upserted, total_modified)

    Raises:
        Exception: If unexpected errors occur during bulk write
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

    # Prepare all chunks
    chunks = [
        (update_one_operations[i : i + chunk_size], chunk_idx + 1)
        for chunk_idx, i in enumerate(range(0, total_requests, chunk_size))
    ]

    # Track results
    all_write_errors = []
    total_upserted = 0
    total_modified = 0
    unexpected_errors = []

    # Process chunks sequentially (async but not concurrent)
    for chunk, chunk_num in chunks:
        result = await _bulk_update_chunk(
            log_id=log_id,
            chunk_num=chunk_num,
            chunk_operations=chunk,
            total_chunks=total_chunks,
        )

        # Aggregate results
        total_upserted += result["upserted_count"]
        total_modified += result["modified_count"]
        all_write_errors.extend(result["write_errors"])
        if result["unexpected_error"]:
            unexpected_errors.append(result["unexpected_error"])

    # Log summary
    logger.info(
        f"bulk_update_gpt_batch_requests[{log_id}]: Completed upserting batch requests \n"
        f"{total_upserted} inserted, {total_modified} modified, "
        f"{len(all_write_errors)} write errors, "
        f"{len(unexpected_errors)} unexpected errors"
    )

    # Raise exceptions if there were issues (maintains error handling compatibility)
    if unexpected_errors:
        raise Exception(
            f"bulk_update_gpt_batch_requests[{log_id}]: Multiple chunk failures, {'; '.join(unexpected_errors)}"
        )

    if all_write_errors:
        # Re-raise combined BulkWriteError for caller's error classification
        error_details = {
            "writeErrors": all_write_errors,
            "nUpserted": total_upserted,
            "nModified": total_modified,
        }
        raise BulkWriteError(error_details)

    return total_upserted, total_modified


async def pair_batch_request_custom_ids_with_batch(
    custom_ids: set[str], gpt_batch: GPTBatch
) -> int:
    match_filter = {"request.custom_id": {"$in": list(custom_ids)}}
    update_operation = {
        "$set": {
            "batch_id": gpt_batch.external_batch_id,
        }
    }

    result = await GPTBatchRequest.get_pymongo_collection().update_many(
        filter=match_filter, update=update_operation
    )

    logger.info(
        f"Paired {result.modified_count} batch requests with batch {gpt_batch.external_batch_id}"
    )

    return result.modified_count


async def reset_batch_requests_with_batch(gpt_batch: GPTBatch) -> int:
    match_filter = {"batch_id": gpt_batch.external_batch_id}
    update_operation = {
        "$set": {
            "batch_id": None,
            #
            # probably not needed:
            "response_blob": None,
        }
    }

    result = await GPTBatchRequest.get_pymongo_collection().update_many(
        filter=match_filter, update=update_operation
    )

    logger.info(
        f"Reset {result.modified_count} batch requests for batch {gpt_batch.external_batch_id}"
    )

    return result.modified_count


async def _upsert_chunk_with_only_request_body(
    chunk: list[GPTBatchRequest],
    chunk_num: int,
    total_chunks: int,
    mfg_etld1: str,
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
                {"request.custom_id": req.request.custom_id},  # Filter by custom_id
                {
                    "$set": {
                        "request": req.request.model_dump(),
                    }
                },
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
            f"Modified {modified_count} batch requests for {mfg_etld1}"
        )

        return {
            "upserted_count": upserted_count,
            "modified_count": modified_count,
            "write_errors": [],
            "unexpected_error": None,
        }

    except BulkWriteError as bwe:
        # Collect write errors (like duplicate keys, though shouldn't happen with upsert)
        write_errors = bwe.details.get("writeErrors", [])
        upserted_count = bwe.details.get("nUpserted", 0)
        modified_count = bwe.details.get("nModified", 0)

        logger.debug(
            f"Chunk {chunk_num}/{total_chunks}: Upserted {upserted_count}, "
            f"Modified {modified_count} with {len(write_errors)} errors for {mfg_etld1}"
        )

        return {
            "upserted_count": upserted_count,
            "modified_count": modified_count,
            "write_errors": write_errors,
            "unexpected_error": None,
        }

    except Exception as e:
        # Unexpected error - entire chunk failed
        logger.error(
            f"Chunk {chunk_num}/{total_chunks}: Unexpected error for {mfg_etld1}: {e}"
        )

        return {
            "upserted_count": 0,
            "modified_count": 0,
            "write_errors": [],
            "unexpected_error": f"Chunk {chunk_num}: {str(e)}",
        }


async def bulk_upsert_gpt_batch_requests_with_only_req_bodies(
    batch_requests: list[GPTBatchRequest],
    mfg_etld1: str,
    chunk_size: int = 5000,
) -> tuple[int, int]:
    """
    Bulk upsert GPT batch requests sequentially to avoid write lock contention.

    Args:
        batch_requests: List of GPTBatchRequest objects to upsert
        mfg_etld1: Manufacturer etld1 for logging purposes
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
        f"Bulk upserting {total_requests} GPT batch requests for {mfg_etld1} "
        f"in {total_chunks} chunks of {chunk_size} (sequential processing)"
    )

    # Prepare all chunks
    chunks = [
        (batch_requests[i : i + chunk_size], chunk_idx + 1)
        for chunk_idx, i in enumerate(range(0, total_requests, chunk_size))
    ]

    # Track results
    all_write_errors = []
    total_upserted = 0
    total_modified = 0
    unexpected_errors = []

    # Process chunks sequentially (async but not concurrent)
    for chunk, chunk_num in chunks:
        result = await _upsert_chunk_with_only_request_body(
            chunk, chunk_num, total_chunks, mfg_etld1
        )

        # Aggregate results
        total_upserted += result["upserted_count"]
        total_modified += result["modified_count"]
        all_write_errors.extend(result["write_errors"])
        if result["unexpected_error"]:
            unexpected_errors.append(result["unexpected_error"])

    # Log summary
    logger.info(
        f"Completed upserting batch requests for {mfg_etld1}: "
        f"{total_upserted} inserted, {total_modified} modified, "
        f"{len(all_write_errors)} write errors, "
        f"{len(unexpected_errors)} unexpected errors"
    )

    # Raise exceptions if there were issues (maintains error handling compatibility)
    if unexpected_errors:
        raise Exception(
            f"Multiple chunk failures for {mfg_etld1}: {'; '.join(unexpected_errors)}"
        )

    if all_write_errors:
        # Re-raise combined BulkWriteError for caller's error classification
        error_details = {
            "writeErrors": all_write_errors,
            "nUpserted": total_upserted,
            "nModified": total_modified,
        }
        raise BulkWriteError(error_details)

    return total_upserted, total_modified


async def bulk_delete_gpt_batch_requests_by_custom_ids(
    gpt_batch_request_custom_ids: list[GPTBatchRequestCustomID],
    mfg_etld1: str,
) -> int:
    """
    Bulk delete GPT batch requests by their custom IDs.

    Args:
        gpt_batch_request_custom_ids: List of GPTBatchRequest custom IDs to delete
        mfg_etld1: Manufacturer etld1 for logging purposes

    Returns:
        Number of deleted documents
    """
    if not gpt_batch_request_custom_ids:
        return 0

    result = await GPTBatchRequest.get_pymongo_collection().delete_many(
        {"request.custom_id": {"$in": gpt_batch_request_custom_ids}},
        hint=[("request.custom_id", 1)],
    )

    logger.info(f"Deleted {result.deleted_count} GPT batch requests for {mfg_etld1}")

    return result.deleted_count


async def bulk_delete_gpt_batch_requests_by_mfg_etld1_and_field(
    mfg_etld1: str,
    field_type: GenericFieldTypeEnum,
) -> int:
    """
    Bulk delete GPT batch requests associated with a manufacturer etld1.

    Args:
        mfg_etld1: Manufacturer etld1 for which to delete batch requests

    Returns:
        Number of deleted documents
    """
    prefix = f"{mfg_etld1}>{field_type.name}>"

    result = await GPTBatchRequest.get_pymongo_collection().delete_many(
        # {"request.custom_id": re.compile(f"^{mfg_etld1}>{field_type.name}>")},
        {"request.custom_id": {"$gte": prefix, "$lt": prefix + "\uffff"}},
        hint=[("request.custom_id", 1)],
    )

    logger.info(
        f"Deleted residual {result.deleted_count} GPT batch requests for {mfg_etld1}"
    )

    return result.deleted_count


'''
# unused as of now
async def bulk_delete_gpt_batch_requests_by_mfg_etld1(
    mfg_etld1: str,
) -> int:
    """
    Bulk delete GPT batch requests associated with a manufacturer etld1.

    Args:
        mfg_etld1: Manufacturer etld1 for which to delete batch requests

    Returns:
        Number of deleted documents
    """

    result = await GPTBatchRequest.get_pymongo_collection().delete_many(
        {"request.custom_id": re.compile(f"^{mfg_etld1}")},
    )

    logger.info(
        f"Deleted residual {result.deleted_count} GPT batch requests for {mfg_etld1}"
    )

    return result.deleted_count


# unused as of now
async def find_completed_gpt_batch_requests_by_custom_id_prefix(
    custom_id_prefix: str,
) -> list[GPTBatchRequest]:
    gpt_requests = []
    async for gpt_req in GPTBatchRequest.find(
        {
            "custom_id": {"$regex": f"^{custom_id_prefix}"},
            "batch_id": {"$ne": None},
            "response_blob": {"$ne": None},
        },
    ):
        gpt_requests.append(gpt_req)

    return gpt_requests

'''
