from dataclasses import dataclass
from pathlib import Path
import logging
import asyncio
import threading
from typing import Optional
from concurrent.futures import ThreadPoolExecutor

from core.models.db.gpt_batch import GPTBatch
from core.models.db.api_key_bundle import APIKeyBundle
from core.utils.batch_jsonl_file_writer import JSONLBatchFile
from data_etl_app.utils.chunk_util import split_bytes_on_line_boundaries

from openai import OpenAI, OpenAIError, APIConnectionError, RateLimitError
from openai.types import Batch, Upload


logger = logging.getLogger(__name__)


def download_openai_file(
    client: OpenAI, output_type: str, output_path: Path, openai_file_id: str
) -> bool:
    """
    The output .jsonl file will have one response line for every successful request line in the input file.
    Any failed requests in the batch will have their error information written to an error file
    that can be found via the batch's error_file_id.
    """
    try:
        if output_path.exists():
            file_size = output_path.stat().st_size
            logger.info(
                f"⏭️  Skipping download - file already exists: {output_path} "
                f"({file_size:,} bytes)"
            )
            return True
        else:
            # Create parent directory if it doesn't exist
            # output_path.parent.mkdir(parents=True, exist_ok=True)
            # Download file content
            file_response = client.files.content(openai_file_id)
            with open(output_path, "w", encoding="utf-8") as f:
                f.write(file_response.text)
            logger.info(
                f"✅ Downloaded output file to {output_path} "
                f"({len(file_response.text):,} bytes)"
            )
            return True
    except OpenAIError as e:
        logger.error(f"Error downloading {output_type} file {openai_file_id}: {e}")
        return False
    except Exception as e:
        logger.error(
            f"Unexpected error downloading {output_type} file {openai_file_id}: {e}",
            exc_info=True,
        )
        return False


def delete_uploaded_batch_file_from_openai(client: OpenAI, input_file_id: str) -> bool:
    if not input_file_id:
        logger.warning(f"No input_file_id passed")
        return False

    try:
        logger.info(f"Deleting uploaded input file {input_file_id}")
        resp = client.files.delete(input_file_id)
        # resp is expected to be a dict-like with 'deleted': True
        deleted = bool(
            getattr(
                resp,
                "deleted",
                resp.get("deleted") if isinstance(resp, dict) else False,
            )
        )
        if deleted:
            logger.info(f"Deleted input file {input_file_id}")
            return True
        else:
            logger.warning(f"Deletion response for {input_file_id}: {resp}")
            return False
    except OpenAIError as e:
        logger.error(f"OpenAI error deleting file {input_file_id}: {e}")
        return False
    except Exception as e:
        logger.error(
            f"Unexpected error deleting file {input_file_id}: {e}", exc_info=True
        )
        return False


def create_upload_object(client: OpenAI, filename: str, file_size: int) -> Upload:
    """
    Create an Upload object for multipart upload.

    Args:
        client: OpenAI client instance
        filename: Name of the file to upload
        file_size: Size of the file in bytes

    Returns:
        Upload object
    """
    logger.info(f"Creating upload object for {filename} ({file_size:,} bytes)")
    upload = client.uploads.create(
        purpose="batch",
        filename=filename,
        bytes=file_size,
        mime_type="application/jsonl",
    )
    logger.info(f"Created upload object: {upload.id}")
    return upload


def complete_upload_object(client: OpenAI, upload_id: str, part_ids: list[str]) -> str:
    """
    Complete a multipart upload and get the final file ID.

    Args:
        client: OpenAI client instance
        upload_id: The Upload object ID
        part_ids: List of part IDs in the order they were uploaded

    Returns:
        file_id: The ID of the completed file
    """
    logger.info(f"Completing upload {upload_id} with {len(part_ids)} parts...")
    upload = client.uploads.complete(upload_id=upload_id, part_ids=part_ids)

    file_id = upload.file.id if upload.file else None
    if not file_id:
        raise ValueError(f"No file ID returned from completed upload: {upload}")

    logger.info(f"✅ Upload completed successfully! File ID: {file_id}")
    return file_id


async def upload_file_to_openai_using_parts(
    client: OpenAI,
    jsonl_batch_file: JSONLBatchFile,
) -> Optional[str]:
    """
    Upload a file to OpenAI using the multipart upload API.

    This creates an Upload object, splits the file into parts (up to 64MB each),
    uploads parts in parallel, then completes the upload to get a File ID.

    Args:
        client: OpenAI client instance
        jsonl_batch_file: The JSONL batch file to upload

    Returns:
        The file ID of the completed upload, or None if upload failed
    """
    PART_SIZE = 20 * 1024 * 1024  # 20 MB per part (safe under 64MB limit)

    # Get file path
    file_path = jsonl_batch_file.full_path

    try:
        # Step 1: Read file and split into parts first
        with open(file_path, "rb") as f:
            file_content = f.read()

        # Split the file content into chunks respecting line boundaries
        byte_chunks = split_bytes_on_line_boundaries(
            data=file_content,
            max_chunk_size=PART_SIZE,
            newline_search_window=10000,  # Search last 10KB for newline
        )

        # Calculate ACTUAL total size after splitting (may differ from original file size
        # due to line boundary adjustments)
        total_upload_size = sum(len(chunk) for chunk in byte_chunks)
        original_size = len(file_content)

        if total_upload_size != original_size:
            logger.warning(
                f"Split size mismatch: original={original_size:,} bytes, "
                f"total chunks={total_upload_size:,} bytes "
                f"(diff={total_upload_size - original_size:+,})"
            )

        # Step 2: Create Upload object with the actual total size
        loop = asyncio.get_event_loop()
        upload = create_upload_object(
            client=client,
            filename=jsonl_batch_file.name,
            file_size=total_upload_size,  # Use actual chunk total, not file size
        )

        # Step 3: Prepare parts for upload
        parts = [
            {"data": chunk, "part_number": i} for i, chunk in enumerate(byte_chunks)
        ]

        # Limit concurrent uploads to avoid overwhelming OpenAI's API
        # OpenAI may throttle concurrent part uploads from the same client
        MAX_CONCURRENT_UPLOADS = 2

        logger.info(
            f"Uploading {len(parts)} parts with max {MAX_CONCURRENT_UPLOADS} concurrent "
            f"(total size: {total_upload_size:,} bytes)..."
        )

        # Shared cancellation flag to stop all upload threads immediately
        cancel_event = threading.Event()

        def upload_part_wrapper(part_dict: dict) -> str:
            return add_upload_part_to_upload_object(
                client=client,
                upload_id=upload.id,
                data_part=part_dict["data"],
                part_number=part_dict["part_number"],
                cancel_event=cancel_event,
            )

        with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_UPLOADS) as executor:
            upload_tasks = [
                loop.run_in_executor(executor, upload_part_wrapper, part)
                for part in parts
            ]

            # Use asyncio.FIRST_EXCEPTION to detect first failure
            done, pending = await asyncio.wait(
                upload_tasks, return_when=asyncio.FIRST_EXCEPTION
            )

            # Check if any completed task failed
            failed = False
            first_exception = None
            for task in done:
                try:
                    task.result()  # This will raise if the task failed
                except Exception as e:
                    failed = True
                    if first_exception is None:
                        first_exception = e
                    break

            # If any task failed, signal all threads to stop and abort
            if failed:
                logger.error(
                    f"Upload failure detected: {first_exception}. "
                    f"Signaling all parts to stop (pending={len(pending)}, done={len(done)})..."
                )

                # Signal all threads to stop immediately
                cancel_event.set()

                # Cancel the upload on OpenAI side ASAP to prevent further API calls
                try:
                    client.uploads.cancel(upload.id)
                    logger.info(f"Cancelled upload {upload.id}")
                except Exception as cancel_error:
                    logger.error(f"Error cancelling upload {upload.id}: {cancel_error}")

                # Cancel all asyncio tasks
                if pending:
                    for task in pending:
                        task.cancel()

                # Wait for all tasks to finish/cancel (with timeout to prevent hanging)
                try:
                    await asyncio.wait_for(
                        asyncio.wait(pending, return_when=asyncio.ALL_COMPLETED),
                        timeout=5.0,
                    )
                except asyncio.TimeoutError:
                    logger.warning(f"Some tasks did not complete within timeout")

                return None

            # All tasks completed successfully (no pending tasks when using FIRST_EXCEPTION)
            # Collect all results - all should be successful at this point
            part_results = []
            for task in upload_tasks:
                try:
                    part_results.append(task.result())
                except Exception as e:
                    # This shouldn't happen since we checked earlier, but handle it
                    logger.error(f"Unexpected error collecting results: {e}")
                    # Signal cancellation and abort
                    cancel_event.set()
                    try:
                        client.uploads.cancel(upload.id)
                    except Exception as cancel_error:
                        logger.error(
                            f"Error cancelling upload {upload.id}: {cancel_error}"
                        )
                    return None

        # All parts uploaded successfully
        logger.info(f"Uploaded {len(part_results)} parts successfully")

        # Step 4: Complete the upload
        file_id = complete_upload_object(
            client=client,
            upload_id=upload.id,
            part_ids=part_results,  # All are part IDs (strings)
        )

        return file_id

    except Exception as e:
        logger.error(
            f"Error uploading file {jsonl_batch_file.name}: {e}",
            exc_info=True,
        )
        return None


def add_upload_part_to_upload_object(
    client: OpenAI,
    upload_id: str,
    data_part: bytes,
    part_number: int,
    max_retries: int = 5,  # Increased from 3 to handle transient connection issues
    cancel_event: Optional[threading.Event] = None,
) -> str:
    """
    Add a part to an Upload object with retry logic.

    Each Part can be at most 64 MB. Parts can be uploaded in parallel.

    Args:
        client: OpenAI client instance
        upload_id: The Upload object ID
        data_part: The bytes to upload for this part
        part_number: The part number (for logging)
        max_retries: Maximum number of retry attempts (default: 5)
        cancel_event: Optional threading.Event to signal cancellation

    Returns:
        part_id: The ID of the uploaded part
    """
    import time
    from openai import APIConnectionError, RateLimitError

    last_exception = None

    for attempt in range(max_retries):
        # Check if cancellation was requested
        if cancel_event and cancel_event.is_set():
            logger.info(
                f"Part {part_number} upload cancelled by cancel_event (attempt {attempt + 1})"
            )
            raise Exception(
                f"Upload cancelled for part {part_number} (upload already failed)"
            )

        try:
            if attempt > 0:
                logger.warning(
                    f"Retry attempt {attempt + 1}/{max_retries} for part {part_number}"
                )
            else:
                logger.debug(
                    f"Uploading part {part_number} ({len(data_part):,} bytes) to {upload_id}"
                )

            upload_part = client.uploads.parts.create(
                upload_id=upload_id,
                data=data_part,
            )

            logger.debug(f"Part {part_number} uploaded: {upload_part.id}")
            return upload_part.id

        except (APIConnectionError, RateLimitError) as e:
            # These are retryable errors - connection issues or rate limits
            last_exception = e
            error_type = "rate limit" if isinstance(e, RateLimitError) else "connection"
            logger.warning(
                f"Part {part_number} {error_type} error (attempt {attempt + 1}/{max_retries}): {e}"
            )

            if attempt < max_retries - 1:
                # Check cancellation before sleeping
                if cancel_event and cancel_event.is_set():
                    logger.info(f"Part {part_number} cancelled during retry backoff")
                    raise Exception(
                        f"Upload cancelled for part {part_number} during retry"
                    )

                # Exponential backoff with jitter: base 2^attempt + random 0-1 seconds
                import random

                backoff = (2**attempt) + random.random()
                logger.info(f"Waiting {backoff:.1f}s before retry...")
                time.sleep(backoff)
            else:
                logger.error(
                    f"Failed to upload part {part_number} after {max_retries} attempts ({error_type} error)"
                )

        except Exception as e:
            # Non-retryable error (e.g., upload_not_pending, invalid request)
            last_exception = e
            logger.error(
                f"Non-retryable error uploading part {part_number} (attempt {attempt + 1}/{max_retries}): {e}"
            )

            # Don't retry on upload_not_pending or similar errors
            if "upload_not_pending" in str(e) or "cancelled" in str(e).lower():
                logger.info(f"Upload already cancelled, stopping part {part_number}")
                raise Exception(f"Upload cancelled for part {part_number}") from e

            # For other errors, retry with shorter backoff
            if attempt < max_retries - 1:
                if cancel_event and cancel_event.is_set():
                    logger.info(f"Part {part_number} cancelled during retry backoff")
                    raise Exception(
                        f"Upload cancelled for part {part_number} during retry"
                    )

                backoff = 1.0
                logger.info(f"Waiting {backoff}s before retry...")
                time.sleep(backoff)
            else:
                logger.error(
                    f"Failed to upload part {part_number} after {max_retries} attempts"
                )

    # If we get here, all retries failed
    raise Exception(
        f"Failed to upload part {part_number} after {max_retries} attempts"
    ) from last_exception


def find_latest_batch_of_api_key_bundle(
    client: OpenAI,
    api_key_bundle: APIKeyBundle,
) -> Optional[Batch]:
    """
    Query OpenAI API to find the latest batch ID for this API key.
    Returns the most recent batch's external_batch_id or None if no batches exist.
    """
    try:
        # client = OpenAI(api_key=api_key_bundle.key)

        # List batches, sorted by created_at descending (most recent first)
        batches_response = client.batches.list(limit=1)  # Only need the most recent one

        # Get the first (most recent) batch if any exist
        if batches_response.data:
            latest_batch: Batch = batches_response.data[0]
            return latest_batch

        return None

    except Exception as e:
        logger.error(
            f"Error fetching latest batch for key {api_key_bundle.label}: {e}",
            exc_info=True,
        )
