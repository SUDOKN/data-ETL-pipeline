from datetime import datetime
import logging
from pathlib import Path
from typing import Optional
from openai import OpenAI, OpenAIError
from openai.types import Batch

from core.models.db.gpt_batch import GPTBatch, GPTBatchStatus
from core.models.db.api_key_bundle import APIKeyBundle

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger = logging.getLogger(__name__)


async def find_latest_gpt_batch_by_external_batch_id(
    external_batch_id: str,
) -> Optional[GPTBatch]:
    return await GPTBatch.find_one(GPTBatch.external_batch_id == external_batch_id)


async def upsert_latest_gpt_batch_by_external_batch(
    external_batch: Batch,
    api_key_bundle: APIKeyBundle,
) -> GPTBatch:

    existing_gpt_batch = await find_latest_gpt_batch_by_external_batch_id(
        external_batch_id=external_batch.id
    )
    if not existing_gpt_batch:
        logger.warning(
            f"upsert_latest_gpt_batch_by_external_batch_id: Unrecorded batch detected "
            f"with external_batch.id:{external_batch.id}, inserting new found batch"
        )
        existing_gpt_batch = await insert_gpt_batch_from_response(
            batch_response=external_batch, api_key_label=api_key_bundle.label
        )
    return existing_gpt_batch


async def find_latest_gpt_batch_by_api_key(
    api_key_bundle: APIKeyBundle,
) -> Optional[GPTBatch]:
    return await GPTBatch.find_one(
        GPTBatch.external_batch_id == api_key_bundle.latest_external_batch_id
    )


async def insert_gpt_batch_from_response(
    batch_response: Batch, api_key_label: str
) -> GPTBatch:
    """
    Create and save a GPTBatch document from OpenAI batch response.

    Args:
        batch_response: OpenAI batch response object
        api_key_label: Label of the API key used to create the batch

    Returns:
        GPTBatch document
    """
    gpt_batch = GPTBatch(
        external_batch_id=batch_response.id,
        endpoint=batch_response.endpoint,
        input_file_id=batch_response.input_file_id,
        completion_window=batch_response.completion_window,
        status=batch_response.status,
        output_file_id=batch_response.output_file_id,
        error_file_id=batch_response.error_file_id,
        created_at=datetime.fromtimestamp(batch_response.created_at),
        in_progress_at=(
            datetime.fromtimestamp(batch_response.in_progress_at)
            if batch_response.in_progress_at
            else None
        ),
        expires_at=datetime.fromtimestamp(
            batch_response.expires_at or batch_response.created_at
        ),
        completed_at=(
            datetime.fromtimestamp(batch_response.completed_at)
            if batch_response.completed_at
            else None
        ),
        failed_at=(
            datetime.fromtimestamp(batch_response.failed_at)
            if batch_response.failed_at
            else None
        ),
        expired_at=(
            datetime.fromtimestamp(batch_response.expired_at)
            if batch_response.expired_at
            else None
        ),
        processing_completed_at=None,
        request_counts=(
            batch_response.request_counts.model_dump()
            if batch_response.request_counts
            else {"total": 0, "completed": 0, "failed": 0}
        ),
        metadata=batch_response.metadata,
        api_key_label=api_key_label,
    )

    # Save to database
    await gpt_batch.insert()
    logger.info(f"GPTBatch saved to database: {gpt_batch.external_batch_id}")

    return gpt_batch


async def update_gpt_batch_from_response(
    batch_response: Batch, gpt_batch: GPTBatch
) -> None:
    try:
        # Update fields that may have changed
        gpt_batch.status = batch_response.status
        gpt_batch.output_file_id = batch_response.output_file_id
        gpt_batch.error_file_id = batch_response.error_file_id

        if batch_response.in_progress_at:
            gpt_batch.in_progress_at = datetime.fromtimestamp(
                batch_response.in_progress_at
            )

        if batch_response.completed_at:
            gpt_batch.completed_at = datetime.fromtimestamp(batch_response.completed_at)

        if batch_response.failed_at:
            gpt_batch.failed_at = datetime.fromtimestamp(batch_response.failed_at)

        if batch_response.expired_at:
            gpt_batch.expired_at = datetime.fromtimestamp(batch_response.expired_at)

        # Update request counts
        if batch_response.request_counts:
            gpt_batch.request_counts = batch_response.request_counts.model_dump()

        # Save updates to database
        await gpt_batch.save()

        logger.info(
            f"Updated batch {gpt_batch.external_batch_id}: "
            f"status={gpt_batch.status}, "
            f"requests={gpt_batch.request_counts}"
            f"api_key={gpt_batch.api_key_label}"
        )

    except OpenAIError as e:
        logger.error(f"Error checking batch {gpt_batch.external_batch_id}: {e}")
        raise
    except Exception as e:
        logger.error(
            f"Unexpected error checking batch {gpt_batch.external_batch_id}: {e}",
            exc_info=True,
        )
        raise


async def get_pending_batches_mapped_by_api_key_label() -> dict[str, GPTBatch]:
    pending_batches: list[GPTBatch] = await GPTBatch.find(
        {
            "status": {
                "$in": [GPTBatchStatus.VALIDATING.name, GPTBatchStatus.PROCESSING.name]
            },
            "api_key_label": {"$exists": True},
        }
    ).to_list()

    map: dict[str, GPTBatch] = {}
    for pb in pending_batches:
        if not pb.api_key_label:
            logger.error(f"Pending batch does not have api_key_label!")
        else:
            map[pb.api_key_label] = pb

    return map
