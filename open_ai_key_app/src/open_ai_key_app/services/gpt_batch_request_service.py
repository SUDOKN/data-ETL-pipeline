from bson import ObjectId
from datetime import datetime
import logging
from typing import Optional
import asyncio
from pymongo.errors import BulkWriteError
from pymongo import ReplaceOne

from open_ai_key_app.models.field_types import GPTBatchRequestCustomID
from open_ai_key_app.models.db.gpt_batch_request import GPTBatchRequest

logger = logging.getLogger(__name__)


async def find_gpt_batch_request_by_custom_id(
    gpt_batch_request_custom_id: GPTBatchRequestCustomID,
) -> GPTBatchRequest:
    gpt_batch_request = await GPTBatchRequest.find_one(
        GPTBatchRequest.request.custom_id == gpt_batch_request_custom_id
    )
    if not gpt_batch_request:
        raise ValueError(
            f"GPTBatchRequest with id {gpt_batch_request_custom_id} not found"
        )

    return gpt_batch_request


async def _upsert_chunk(
    chunk: list[GPTBatchRequest],
    chunk_num: int,
    total_chunks: int,
    mfg_etld1: str,
) -> dict:
    """
    Upsert a single chunk of batch requests.

    Returns:
        Dict with keys: upserted_count, modified_count, write_errors, unexpected_error
    """
    try:
        # CAUTION: Replaces entire document except _id if custom_id matches
        # Use bulk_write with ReplaceOne operations for upsert
        operations = [
            ReplaceOne(
                {"request.custom_id": req.request.custom_id},  # Filter by custom_id
                req.model_dump(
                    by_alias=True, exclude={"id"}
                ),  # Exclude _id to let MongoDB generate it
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


async def bulk_upsert_gpt_batch_requests(
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
        result = await _upsert_chunk(chunk, chunk_num, total_chunks, mfg_etld1)

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
