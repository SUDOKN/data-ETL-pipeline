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
    gpt_batch_request_custom_ids: list[GPTBatchRequestCustomID],
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


async def get_custom_ids_for_batch(
    gpt_batch: GPTBatch,
) -> set[GPTBatchRequestCustomID]:
    """
    Get all custom IDs for a given GPT batch.

    Args:
        gpt_batch: GPTBatch object to get custom IDs for

    Returns:
        Set of custom IDs for the batch
    """
    logger.info(f"Getting custom IDs for GPT batch {gpt_batch.external_batch_id}")
    collection = GPTBatchRequest.get_pymongo_collection()

    custom_ids = []
    # Fetch all matching documents at once
    docs = await collection.find(
        {"batch_id": gpt_batch.external_batch_id},
        projection={"request.custom_id": 1, "_id": 0},
    ).to_list(
        length=None
    )  # None means fetch all
    logger.info(
        f"Found {len(docs):,} batch requests for batch {gpt_batch.external_batch_id}"
    )

    # Extract custom IDs
    custom_ids = [doc["request"]["custom_id"] for doc in docs]

    return set(custom_ids)


async def pair_batch_request_custom_ids_with_batch(
    custom_ids: set[str], gpt_batch: GPTBatch, chunk_size: int = 5000
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

    # Create UpdateOne operations for each custom_id
    update_operations = [
        UpdateOne(
            {"request.custom_id": custom_id},
            {"$set": {"batch_id": gpt_batch.external_batch_id}},
            upsert=False,  # Doesn't create new documents if filter unmatched
        )
        for custom_id in custom_ids
    ]

    # Use the generic bulk update function
    _, modified_count = await bulk_update_gpt_batch_requests(
        update_one_operations=update_operations,
        log_id=f"pair_with_{gpt_batch.external_batch_id}",
        chunk_size=chunk_size,
    )

    return modified_count


async def unpair_all_batch_requests_from_batch(
    gpt_batch: GPTBatch, chunk_size: int = 5000
) -> int:
    """
    Reset batch requests associated with a batch in chunks to avoid write lock contention.

    Args:
        gpt_batch: GPTBatch object whose requests should be reset
        chunk_size: Number of requests to reset per chunk (default: 5000)

    Returns:
        Total number of modified documents
    """
    # First, get all custom IDs for this batch
    collection = GPTBatchRequest.get_pymongo_collection()

    custom_ids = []
    # Fetch all matching documents at once
    docs = await collection.find(
        {"batch_id": gpt_batch.external_batch_id},
        projection={"request.custom_id": 1, "_id": 0},
    ).to_list(
        length=None
    )  # None means fetch all

    # Extract custom IDs
    custom_ids = [doc["request"]["custom_id"] for doc in docs]

    if not custom_ids:
        logger.info(
            f"No batch requests found to reset for batch {gpt_batch.external_batch_id}"
        )
        return 0

    return await unpair_batch_requests_by_custom_ids(
        custom_ids=set(custom_ids),
        chunk_size=chunk_size,
    )


async def unpair_batch_requests_by_custom_ids(
    custom_ids: set[str], chunk_size: int = 5000
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

    # Create UpdateOne operations for each custom_id
    update_operations = [
        UpdateOne(
            {"request.custom_id": custom_id},
            {"$set": {"batch_id": None, "response_blob": None}},
            upsert=False,  # Doesn't create new documents if filter unmatched
        )
        for custom_id in custom_ids
    ]

    # Use the generic bulk update function
    _, modified_count = await bulk_update_gpt_batch_requests(
        update_one_operations=update_operations,
        log_id=f"unpair_by_custom_ids",
        chunk_size=chunk_size,
    )

    return modified_count


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
                        "request.body": req.request.body.model_dump(),  # Update body on both insert and update
                    },
                    "$setOnInsert": {  # Set these required fields only on insert
                        "created_at": req.created_at,
                        "request.custom_id": req.request.custom_id,
                        "request.method": req.request.method,
                        "request.url": req.request.url,
                        "batch_id": req.batch_id,
                        "response_blob": (
                            req.response_blob.model_dump()
                            if req.response_blob
                            else None
                        ),
                    },
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

        # Log sample errors at INFO level for debugging
        logger.info(
            f"Chunk {chunk_num}/{total_chunks}: Upserted {upserted_count}, "
            f"Modified {modified_count} with {len(write_errors)} write errors for {mfg_etld1}"
        )

        # Log first few error details for debugging
        if write_errors:
            sample_errors = write_errors[:3]  # Show first 3 errors
            for idx, err in enumerate(sample_errors, 1):
                error_code = err.get("code")
                error_msg = err.get("errmsg", "N/A")

                # For validation errors (code 121), log more details
                if error_code == 121:
                    logger.info(
                        f"  Write error {idx}/{len(write_errors)}: Code={error_code}, "
                        f"Message={error_msg[:200]}"
                    )
                    # Try to extract the errInfo which contains validation details
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

    logger.debug(
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
