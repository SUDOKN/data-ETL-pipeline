"""
Upload batch JSONL files to OpenAI and create batches.

This script:
1. Reads all batch_*.jsonl files from batch_requests_output folder
2. Uploads each file to OpenAI
3. Creates a batch for each uploaded file
4. Saves batch info to GPTBatch collection
5. Updates GPTBatchRequest documents with batch_id
6. Moves processed files to uploaded_batch_requests folder
"""

import asyncio
import argparse
import logging
import json
import shutil
from datetime import datetime, UTC
from pathlib import Path
from typing import Optional, Any
import httpx

from openai import OpenAI, OpenAIError, APIConnectionError

from core.dependencies.load_core_env import load_core_env
from scraper_app.dependencies.load_scraper_env import load_scraper_env
from open_ai_key_app.dependencies.load_open_ai_app_env import load_open_ai_app_env
from data_etl_app.dependencies.load_data_etl_env import load_data_etl_env

# Load environment variables
load_core_env()
load_scraper_env()
load_data_etl_env()
load_open_ai_app_env()

from core.utils.mongo_client import init_db
from core.models.db.gpt_batch import GPTBatch
from core.models.db.gpt_batch_request import GPTBatchRequest

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger = logging.getLogger(__name__)

# TODO: Fill this list with your OpenAI API keys
OPENAI_API_KEYS = {
    "sudokn.tool": "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",
    "sudokn.tool2": "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",
    "sudokn.tool3": "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",
    # "sudokn.tool4": "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",
    "sudokn.tool5": "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",
    "sudokn.tool6": "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",
    "sudokn.tool7": "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",
    # "sudokn.tool20": "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",
    # "sudokn.tool1-o": "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",
    # "sudokn.tool2-o": "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",
    # "sudokn.tool3-o": "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",
    # "sudokn.tool4-o": "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",
    # "sudokn.tool21-o": "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",
    # "sudokn.tool22-o": "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",
    # "sudokn.tool24-o": "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",
    # "sudokn.tool25-o": "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",
    # # plus series
    # "sudokn.tool+1": "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",
    # "sudokn.tool+2": "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",
    # "sudokn.tool+3": "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",
    # "sudokn.tool+4": "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",
    # "sudokn.tool+5": "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",
    # "sudokn.tool2+1": "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",
    # "sudokn.tool2+2": "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",
    # "sudokn.tool2+3": "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",
    # "sudokn.tool2+4": "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",
    # "sudokn.tool2+5": "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",
    # "sudokn.tool3+1": "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",
}


class BatchUploader:
    """Handles uploading batch files and creating batches."""

    def __init__(self, api_keys: dict[str, str], max_uploads_per_key: int = 2):
        self.api_keys = api_keys
        self.api_key_labels = list(api_keys.keys())
        self.current_key_index = 0
        self.client: Optional[OpenAI] = None
        self.max_uploads_per_key = max_uploads_per_key
        self._init_client()

        # Track uploads per API key
        self.uploads_per_key: dict[str, int] = {
            label: 0 for label in self.api_key_labels
        }

        # Statistics
        self.total_files_processed = 0
        self.total_files_uploaded = 0
        self.total_files_failed = 0
        self.total_batches_created = 0

    def _init_client(self):
        """Initialize OpenAI client with current API key and extended timeouts for large file uploads."""
        current_label = self.api_key_labels[self.current_key_index]
        api_key = self.api_keys[current_label]

        # Increased timeouts for very large file uploads (up to 200MB)
        # Assuming ~5-10 MB/s upload speed:
        # - 200MB file could take 20-40 seconds at 5MB/s
        # - Add safety margin for network variability
        timeout = httpx.Timeout(
            connect=60.0,  # time to establish TCP/TLS connection
            read=1800.0,  # 30 minutes - time waiting for server response after upload
            write=1800.0,  # 30 minutes - time allowed to upload request body (for 200MB files)
            pool=30.0,  # time to get connection from pool
        )

        self.client = OpenAI(  # type: ignore
            api_key=api_key,
            timeout=timeout,
        )
        logger.info(
            f"Initialized OpenAI client with key '{current_label}' "
            f"(#{self.current_key_index + 1}/{len(self.api_key_labels)}) "
            f"with timeouts: connect=60s, read/write=1800s (30min)"
        )

    def _rotate_to_next_key(self):
        """
        Rotate to the next API key, cycling back to the beginning if needed.
        """
        self.current_key_index = (self.current_key_index + 1) % len(self.api_key_labels)
        self._init_client()

    def _find_next_available_key(self) -> bool:
        """
        Find the next API key that hasn't reached the upload limit.

        Returns:
            True if an available key was found, False if all keys are at limit
        """
        starting_index = self.current_key_index
        checked_count = 0

        while checked_count < len(self.api_key_labels):
            current_label = self.api_key_labels[self.current_key_index]
            uploads_count = self.uploads_per_key[current_label]

            if uploads_count < self.max_uploads_per_key:
                logger.info(
                    f"Selected key '{current_label}' "
                    f"(uploads: {uploads_count}/{self.max_uploads_per_key})"
                )
                return True

            logger.info(
                f"Key '{current_label}' has reached upload limit "
                f"({uploads_count}/{self.max_uploads_per_key}), trying next key"
            )
            self._rotate_to_next_key()
            checked_count += 1

        logger.error(
            f"All {len(self.api_key_labels)} API keys have reached the upload limit "
            f"of {self.max_uploads_per_key} files per key"
        )
        return False

    def get_current_key_label(self) -> str:
        """Get the label of the current API key."""
        if self.current_key_index < len(self.api_key_labels):
            return self.api_key_labels[self.current_key_index]
        return "unknown"

    async def _create_gpt_batch_from_response(
        self, batch_response, api_key_label: str
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

    async def _check_and_upsert_existing_batch(
        self, batch_obj, current_key_label: str
    ) -> GPTBatch:
        """
        Check if a batch exists in database and upsert it.

        Args:
            batch_obj: OpenAI batch object
            current_key_label: Label of the API key used

        Returns:
            GPTBatch document
        """
        logger.info(f"Checking database for existing batch: {batch_obj.id}")
        gpt_batch = await GPTBatch.find_one({"external_batch_id": batch_obj.id})

        if gpt_batch:
            logger.info(
                f"Batch already exists in database: {gpt_batch.external_batch_id}"
            )
            # Update the batch with latest status
            gpt_batch.status = batch_obj.status
            gpt_batch.request_counts = (
                batch_obj.request_counts.model_dump()
                if batch_obj.request_counts
                else {"total": 0, "completed": 0, "failed": 0}
            )
            await gpt_batch.save()
            logger.info(
                f"Updated existing batch in database: {gpt_batch.external_batch_id}"
            )
        else:
            # Create new database entry for existing batch
            logger.info(f"Creating database entry for existing batch: {batch_obj.id}")
            gpt_batch = await self._create_gpt_batch_from_response(
                batch_obj, current_key_label
            )

        return gpt_batch

    async def _find_existing_batch_for_file(self, file_id: str) -> Optional[Any]:
        """
        Find existing batch for a given file ID.

        Args:
            file_id: OpenAI file ID

        Returns:
            Batch object if found, None otherwise
        """
        logger.info("Checking for existing batch...")
        assert self.client is not None
        existing_batches = self.client.batches.list(limit=100)
        for batch_obj in existing_batches.data:
            if batch_obj.input_file_id == file_id:
                logger.info(f"Found existing batch: {batch_obj.id} for file {file_id}")
                return batch_obj
        return None

    async def _check_file_in_current_key(
        self, file_path: Path, current_key_label: str
    ) -> Optional[Any]:
        """
        Check if file already exists in current API key.

        Args:
            file_path: Path to the file
            current_key_label: Label of current API key

        Returns:
            File object if found, None otherwise
        """
        logger.info(
            f"Checking for existing file in current key '{current_key_label}'..."
        )
        assert self.client is not None
        existing_files = self.client.files.list(purpose="batch")

        for file_obj in existing_files.data:
            logger.debug(f"Existing file: {file_obj.id} ({file_obj.filename})")
            if file_obj.filename == file_path.name:
                logger.info(f"Found existing file: {file_obj.id} ({file_obj.filename})")
                return file_obj
        return None

    async def _upload_file_to_openai(
        self, file_path: Path, current_key_label: str
    ) -> Any:
        """
        Upload a file to OpenAI.

        Args:
            file_path: Path to the file to upload
            current_key_label: Label of current API key

        Returns:
            Uploaded file object
        """
        logger.info(f"Uploading file to OpenAI: {file_path.name}...")
        assert self.client is not None
        with open(file_path, "rb") as f:
            batch_input_file = self.client.files.create(file=f, purpose="batch")
        logger.info(
            f"File uploaded successfully: {batch_input_file.id} ({file_path.name}) "
            f"using key: {current_key_label}"
        )
        return batch_input_file

    async def upload_and_create_batch(
        self,
        file_path: Path,
        metadata: dict,
    ) -> Optional[GPTBatch]:
        """
        Upload a file and create a batch, with retry on connection errors and key rotation.
        Retries connection errors on the SAME key before rotating to the next key.

        Args:
            file_path: Path to the JSONL file
            metadata: Metadata to attach to the batch

        Returns:
            GPTBatch object if successful, None if failed
        """
        # Check if batch already exists in database with same original_filename
        original_filename = metadata.get("original_filename")
        if original_filename:
            logger.info(
                f"Checking database for existing batch with filename: {original_filename}"
            )
            existing_batch = await GPTBatch.find_one(
                {"metadata.original_filename": original_filename}
            )
            if existing_batch:
                logger.info(
                    f"⏭️  Skipping upload - batch already exists in database "
                    f"(batch_id: {existing_batch.external_batch_id}, "
                    f"status: {existing_batch.status})"
                )
                # Don't rotate key since we didn't use it
                return existing_batch

        # Log file size to help diagnose timeout issues
        try:
            size_bytes = file_path.stat().st_size
            logger.info(
                f"File {file_path.name} size: {size_bytes:,} bytes ({size_bytes/1024/1024:.2f} MB)"
            )
        except Exception:
            pass

        logger.info(
            f"No existing batch found in database - checking all API keys for existing file"
        )

        # Check across ALL API keys for existing file
        existing_file_info = None
        for idx, (key_label, api_key) in enumerate(self.api_keys.items()):
            try:
                logger.debug(
                    f"Checking key {idx + 1}/{len(self.api_keys)}: {key_label}"
                )
                timeout = httpx.Timeout(
                    connect=60.0,
                    read=300.0,
                    write=300.0,
                    pool=30.0,
                )
                temp_client = OpenAI(api_key=api_key, timeout=timeout)
                existing_files = temp_client.files.list(purpose="batch")

                for file_obj in existing_files.data:
                    if file_obj.filename == file_path.name:
                        logger.info(
                            f"✓ Found existing file in key '{key_label}': "
                            f"{file_obj.id} ({file_obj.filename})"
                        )
                        existing_file_info = {
                            "file_obj": file_obj,
                            "api_key": api_key,
                            "key_label": key_label,
                            "key_index": idx,
                        }
                        break

                if existing_file_info:
                    break

            except Exception as e:
                logger.warning(f"Error checking key '{key_label}': {e}")
                continue

        if existing_file_info:
            logger.info(
                f"Using existing file from key '{existing_file_info['key_label']}': "
                f"{existing_file_info['file_obj'].id}"
            )
            # Switch to the key that has the file
            self.current_key_index = existing_file_info["key_index"]
            self._init_client()
            batch_input_file = existing_file_info["file_obj"]
            current_key_label = existing_file_info["key_label"]

            # Check if batch exists for this file
            logger.info("Checking for existing batch for this file...")
            assert self.client is not None
            existing_batch = await self._find_existing_batch_for_file(
                batch_input_file.id
            )

            if existing_batch:
                gpt_batch = await self._check_and_upsert_existing_batch(
                    existing_batch, current_key_label
                )

                # Rotate to next key after success
                logger.info("Rotating to next API key for load balancing")
                self._rotate_to_next_key()
                return gpt_batch

            # File exists but no batch - create batch with this file
            logger.info(f"File exists but no batch found - creating batch")
        else:
            logger.info(
                f"No existing file found across any API keys - will upload new file"
            )

        max_key_rotations = len(self.api_keys)  # Try all keys
        per_key_retries = 3  # Retry connection errors 3 times per key
        base_backoff = 2.0  # Base backoff in seconds

        key_rotation_count = 0

        while key_rotation_count < max_key_rotations:
            # Find next available key that hasn't reached upload limit
            if not self._find_next_available_key():
                logger.error(
                    f"Cannot upload {file_path.name} - all API keys have reached "
                    f"the upload limit of {self.max_uploads_per_key} files per key"
                )
                return None

            current_key_label = self.get_current_key_label()
            attempt = 0

            while attempt < per_key_retries:
                try:
                    logger.info(
                        f"Uploading file: {file_path.name} "
                        f"(key: {current_key_label}, "
                        f"key {key_rotation_count + 1}/{max_key_rotations}, "
                        f"attempt {attempt + 1}/{per_key_retries})"
                    )

                    # Ensure client is initialized
                    assert self.client is not None, "OpenAI client not initialized"

                    # Only check current key for existing file if we didn't already find one across all keys
                    if not existing_file_info:
                        # Check if file already exists with the same name in current key
                        existing_file = await self._check_file_in_current_key(
                            file_path, current_key_label
                        )

                        # Upload file if it doesn't exist
                        if existing_file:
                            batch_input_file = existing_file
                            logger.info(
                                f"Using existing file: {batch_input_file.id} ({file_path.name})"
                            )
                        else:
                            batch_input_file = await self._upload_file_to_openai(
                                file_path, current_key_label
                            )
                    else:
                        # We already have the file from the cross-key check
                        batch_input_file = existing_file_info["file_obj"]
                        logger.info(
                            f"Using file from cross-key check: {batch_input_file.id}"
                        )

                    # Check if batch already exists for this file
                    existing_batch = await self._find_existing_batch_for_file(
                        batch_input_file.id
                    )

                    if existing_batch:
                        gpt_batch = await self._check_and_upsert_existing_batch(
                            existing_batch, current_key_label
                        )

                        # Rotate to next key after success
                        logger.info("Rotating to next API key for load balancing")
                        self._rotate_to_next_key()
                        return gpt_batch

                    # Create new batch
                    logger.info(f"Creating batch for file: {batch_input_file.id}")

                    # Add API key label to metadata
                    metadata_with_key = {**metadata, "api_key_label": current_key_label}

                    # Validate metadata - OpenAI limits metadata to 16 key-value pairs
                    # and each key/value must be strings with max 512 characters
                    if len(metadata_with_key) > 16:
                        logger.warning(
                            f"Metadata has {len(metadata_with_key)} keys, "
                            "OpenAI allows max 16. Truncating..."
                        )
                        # Keep only the most important fields
                        metadata_with_key = {
                            "original_filename": metadata_with_key.get(
                                "original_filename", ""
                            ),
                            "run_folder": metadata_with_key.get("run_folder", ""),
                            "num_requests": metadata_with_key.get("num_requests", "0"),
                            "api_key_label": current_key_label,
                        }

                    # Ensure all values are strings and truncate if needed
                    validated_metadata = {}
                    for key, value in metadata_with_key.items():
                        str_value = str(value)
                        if len(str_value) > 512:
                            logger.warning(
                                f"Metadata key '{key}' value too long ({len(str_value)} chars), "
                                "truncating to 512 chars"
                            )
                            str_value = str_value[:512]
                        validated_metadata[key] = str_value

                    logger.info(f"Validated metadata: {validated_metadata}")

                    batch_response = self.client.batches.create(
                        input_file_id=batch_input_file.id,
                        endpoint="/v1/chat/completions",
                        completion_window="24h",
                        metadata=validated_metadata,
                    )

                    logger.info(
                        f"Batch created successfully: {batch_response.id} "
                        f"using key: {current_key_label}"
                    )

                    # Create GPTBatch document using helper method
                    gpt_batch = await self._create_gpt_batch_from_response(
                        batch_response, current_key_label
                    )

                    self.total_batches_created += 1

                    # Increment upload count for this key
                    self.uploads_per_key[current_key_label] += 1
                    logger.info(
                        f"Upload count for key '{current_key_label}': "
                        f"{self.uploads_per_key[current_key_label]}/{self.max_uploads_per_key}"
                    )

                    # Rotate to next key after successful upload for load balancing
                    logger.info("Rotating to next API key for load balancing")
                    self._rotate_to_next_key()

                    return gpt_batch

                except APIConnectionError as e:
                    # Connection error - retry on same key with backoff
                    attempt += 1
                    backoff_time = base_backoff * (2 ** (attempt - 1))
                    logger.warning(
                        f"Connection error with key '{current_key_label}' "
                        f"(attempt {attempt}/{per_key_retries}): {e}"
                    )
                    if attempt < per_key_retries:
                        logger.info(
                            f"Retrying same key after {backoff_time:.1f}s backoff..."
                        )
                        await asyncio.sleep(backoff_time)
                    else:
                        logger.error(
                            f"Connection failed after {per_key_retries} attempts with key '{current_key_label}'"
                        )
                    continue

                except OpenAIError as e:
                    # Other OpenAI API errors - rotate to next key immediately
                    logger.error(
                        f"OpenAI API error with key '{current_key_label}' "
                        f"(#{self.current_key_index + 1}/{len(self.api_key_labels)}): {e}"
                    )
                    break  # Break inner loop to rotate key

                except Exception as e:
                    logger.error(
                        f"Unexpected error uploading {file_path.name} "
                        f"with key '{current_key_label}': {e}",
                        exc_info=True,
                    )
                    return None

            # After exhausting retries on current key, rotate to next
            key_rotation_count += 1
            if key_rotation_count < max_key_rotations:
                logger.info(
                    f"Switching to next API key "
                    f"(key {key_rotation_count + 1}/{max_key_rotations})"
                )
                self._rotate_to_next_key()
            else:
                logger.error(
                    f"All {max_key_rotations} API keys failed for {file_path.name}"
                )
                return None

        return None


async def reset_failed_batch_requests(batch_id: str) -> int:
    """
    Reset GPTBatchRequest documents for a failed batch by setting batch_id and request_sent_at to None.
    This allows these requests to be retried in a new batch.

    Args:
        batch_id: The batch ID of the failed batch

    Returns:
        Number of documents reset
    """
    # Use pymongo collection directly for bulk update
    collection = GPTBatchRequest.get_pymongo_collection()
    result = await collection.update_many(
        {"batch_id": batch_id},
        {
            "$set": {
                "batch_id": None,
            }
        },
    )

    reset_count = result.modified_count
    if reset_count > 0:
        logger.info(
            f"Reset {reset_count:,} GPTBatchRequest documents for failed batch: {batch_id}"
        )

    return reset_count


async def check_existing_files_and_batches(api_keys: dict[str, str]):
    """
    Check existing files and batches for each API key.
    Also upserts batches into the database and resets failed batch requests.

    For batches with failed/expired/cancelled status:
    - Updates the batch status in the database
    - Resets associated GPTBatchRequest documents (sets batch_id and request_sent_at to None)
    - This allows failed requests to be retried in a new batch

    Args:
        api_keys: Dictionary of API key labels to API keys
    """
    print("\n" + "=" * 70)
    print("CHECKING EXISTING FILES AND BATCHES")
    print("=" * 70)

    total_files = 0
    total_batches = 0
    total_batches_upserted = 0

    for idx, (label, api_key) in enumerate(api_keys.items(), 1):
        print(f"\n{'-' * 70}")
        print(f"API Key {idx}/{len(api_keys)}: {label}")
        print(f"{'-' * 70}")

        try:
            # Initialize client for this key
            timeout = httpx.Timeout(
                connect=60.0,
                read=300.0,  # 5 minutes for listing operations
                write=300.0,  # 5 minutes
                pool=30.0,
            )
            client = OpenAI(api_key=api_key, timeout=timeout)

            # List files
            logger.info(f"Fetching files for {label}...")
            files_response = client.files.list(purpose="batch")
            files = list(files_response.data)
            total_files += len(files)

            print(f"\n📁 Files ({len(files)}):")
            if files:
                for file_obj in files:
                    created_at = datetime.fromtimestamp(file_obj.created_at).strftime(
                        "%Y-%m-%d %H:%M:%S"
                    )
                    size_mb = file_obj.bytes / (1024 * 1024) if file_obj.bytes else 0
                    print(f"  - {file_obj.filename}")
                    print(f"    ID: {file_obj.id}")
                    print(f"    Size: {size_mb:.2f} MB")
                    print(f"    Created: {created_at}")
                    print(f"    Status: {file_obj.status}")
            else:
                print("  No files found")

            # List batches
            logger.info(f"Fetching batches for {label}...")
            batches_response = client.batches.list(limit=100)
            batches = list(batches_response.data)
            total_batches += len(batches)

            print(f"\n🔄 Batches ({len(batches)}):")
            if batches:
                # Group batches by status
                status_counts = {}
                for batch in batches:
                    status_counts[batch.status] = status_counts.get(batch.status, 0) + 1

                print(f"  Status breakdown:")
                for status, count in sorted(status_counts.items()):
                    print(f"    {status}: {count}")

                print(f"\n  Recent batches:")
                for batch in batches[:10]:  # Show first 10
                    created_at = datetime.fromtimestamp(batch.created_at).strftime(
                        "%Y-%m-%d %H:%M:%S"
                    )
                    metadata = batch.metadata or {}
                    original_filename = metadata.get("original_filename", "N/A")

                    print(f"  - Batch ID: {batch.id}")
                    print(f"    File: {original_filename}")
                    print(f"    Input File ID: {batch.input_file_id}")
                    print(f"    Status: {batch.status}")
                    print(f"    Created: {created_at}")
                    if batch.request_counts:
                        counts = batch.request_counts
                        print(
                            f"    Requests: {counts.total} total, {counts.completed} completed, {counts.failed} failed"
                        )

                if len(batches) > 10:
                    print(f"  ... and {len(batches) - 10} more batches")

                # Upsert batches into database
                logger.info(
                    f"Upserting {len(batches)} batches into database for {label}..."
                )
                batches_upserted_count = 0
                failed_batches_reset_count = 0
                total_requests_reset = 0

                for batch in batches:
                    try:
                        # Check if batch already exists
                        existing_batch = await GPTBatch.find_one(
                            {"external_batch_id": batch.id}
                        )

                        # Check if batch has failed status
                        is_failed = batch.status in ["failed", "expired", "cancelled"]
                        was_previously_not_failed = False

                        if existing_batch:
                            # Track if status changed to failed
                            was_previously_not_failed = existing_batch.status not in [
                                "failed",
                                "expired",
                                "cancelled",
                            ]

                            # Update existing batch
                            existing_batch.status = batch.status
                            existing_batch.output_file_id = batch.output_file_id
                            existing_batch.error_file_id = batch.error_file_id
                            existing_batch.in_progress_at = (
                                datetime.fromtimestamp(batch.in_progress_at)
                                if batch.in_progress_at
                                else None
                            )
                            existing_batch.completed_at = (
                                datetime.fromtimestamp(batch.completed_at)
                                if batch.completed_at
                                else None
                            )
                            existing_batch.failed_at = (
                                datetime.fromtimestamp(batch.failed_at)
                                if batch.failed_at
                                else None
                            )
                            existing_batch.expired_at = (
                                datetime.fromtimestamp(batch.expired_at)
                                if batch.expired_at
                                else None
                            )
                            existing_batch.request_counts = (
                                batch.request_counts.model_dump()
                                if batch.request_counts
                                else {"total": 0, "completed": 0, "failed": 0}
                            )
                            # Update api_key_label if it was missing
                            if not existing_batch.api_key_label:
                                existing_batch.api_key_label = label

                            await existing_batch.save()
                            logger.debug(f"Updated batch {batch.id} in database")
                        else:
                            # Create new batch
                            gpt_batch = GPTBatch(
                                external_batch_id=batch.id,
                                endpoint=batch.endpoint,
                                input_file_id=batch.input_file_id,
                                completion_window=batch.completion_window,
                                status=batch.status,
                                output_file_id=batch.output_file_id,
                                error_file_id=batch.error_file_id,
                                created_at=datetime.fromtimestamp(batch.created_at),
                                in_progress_at=(
                                    datetime.fromtimestamp(batch.in_progress_at)
                                    if batch.in_progress_at
                                    else None
                                ),
                                expires_at=datetime.fromtimestamp(
                                    batch.expires_at or batch.created_at
                                ),
                                completed_at=(
                                    datetime.fromtimestamp(batch.completed_at)
                                    if batch.completed_at
                                    else None
                                ),
                                failed_at=(
                                    datetime.fromtimestamp(batch.failed_at)
                                    if batch.failed_at
                                    else None
                                ),
                                expired_at=(
                                    datetime.fromtimestamp(batch.expired_at)
                                    if batch.expired_at
                                    else None
                                ),
                                processing_completed_at=None,
                                request_counts=(
                                    batch.request_counts.model_dump()
                                    if batch.request_counts
                                    else {"total": 0, "completed": 0, "failed": 0}
                                ),
                                metadata=batch.metadata,
                                api_key_label=label,
                            )
                            await gpt_batch.insert()
                            logger.debug(f"Inserted new batch {batch.id} into database")

                        batches_upserted_count += 1
                        total_batches_upserted += 1

                        # If batch is failed and either newly failed or newly discovered, reset associated requests
                        if is_failed and (
                            not existing_batch or was_previously_not_failed
                        ):
                            logger.warning(
                                f"Batch {batch.id} has failed status: {batch.status}"
                            )
                            reset_count = await reset_failed_batch_requests(batch.id)
                            if reset_count > 0:
                                failed_batches_reset_count += 1
                                total_requests_reset += reset_count
                                logger.info(
                                    f"Reset {reset_count} requests for failed batch {batch.id}"
                                )

                    except Exception as e:
                        logger.error(
                            f"Error upserting batch {batch.id}: {e}", exc_info=True
                        )

                print(f"  ✅ Upserted {batches_upserted_count} batches into database")
                if failed_batches_reset_count > 0:
                    print(
                        f"  ⚠️  Reset {total_requests_reset} requests from {failed_batches_reset_count} failed batch(es)"
                    )
            else:
                print("  No batches found")

        except Exception as e:
            logger.error(f"Error checking {label}: {e}", exc_info=True)
            print(f"  ❌ Error: {e}")

    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Total API keys checked: {len(api_keys)}")
    print(f"Total files found: {total_files}")
    print(f"Total batches found: {total_batches}")
    print(f"Total batches upserted to database: {total_batches_upserted}")
    print("=" * 70)


async def count_requests_in_file(file_path: Path) -> int:
    """Count the number of requests in a JSONL file."""
    count = 0
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                count += 1
    return count


async def extract_custom_ids_from_file(file_path: Path) -> list[str]:
    """Extract all custom_id values from a JSONL file."""
    custom_ids = []
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                request = json.loads(line)
                custom_id = request.get("custom_id")
                if custom_id:
                    custom_ids.append(custom_id)
    return custom_ids


async def update_batch_requests_with_batch_id(
    custom_ids: list[str], batch_id: str
) -> int:
    """
    Update GPTBatchRequest documents with batch_id and request_sent_at.

    Returns:
        Number of documents updated
    """
    # Use pymongo collection directly for bulk update
    collection = GPTBatchRequest.get_pymongo_collection()
    result = await collection.update_many(
        {"request.custom_id": {"$in": custom_ids}},
        {
            "$set": {
                "batch_id": batch_id,
            }
        },
    )

    updated_count = result.modified_count
    logger.info(
        f"Updated {updated_count:,} GPTBatchRequest documents with batch_id: {batch_id}"
    )

    return updated_count


async def move_processed_folder(
    run_folder: Path,
    uploaded_dir: Path,
):
    """
    Move entire run folder to uploaded_batch_requests folder.

    Args:
        run_folder: Path to the run folder containing batch files
        uploaded_dir: Destination directory for uploaded files
    """
    uploaded_dir.mkdir(parents=True, exist_ok=True)

    destination = uploaded_dir / run_folder.name

    # If destination already exists, remove it first
    if destination.exists():
        shutil.rmtree(destination)
        logger.info(f"Removed existing folder: {destination}")

    # Move the entire folder
    shutil.move(str(run_folder), str(destination))
    logger.info(f"Moved entire folder {run_folder.name} to {uploaded_dir}")


async def process_batch_files(
    input_dir: Path,
    uploaded_dir: Path,
    limit: Optional[int] = None,
):
    """
    Process all .jsonl files in timestamped folders within the input directory.

    Args:
        input_dir: Directory containing timestamped folders with batch files
        uploaded_dir: Directory to move uploaded folders to
        limit: Optional limit on number of folders to process
    """
    # Find all subdirectories (timestamped folders)
    run_folders = sorted(
        [
            d
            for d in input_dir.iterdir()
            if d.is_dir() and d.name != "uploaded_batch_requests"
        ]
    )

    if not run_folders:
        logger.warning(f"No run folders found in {input_dir}")
        return

    if limit:
        run_folders = run_folders[:limit]

    logger.info(f"Found {len(run_folders)} run folder(s) to process")

    # Initialize uploader
    if not OPENAI_API_KEYS:
        logger.error("No API keys configured. Please add keys to OPENAI_API_KEYS list.")
        return

    uploader = BatchUploader(OPENAI_API_KEYS)

    # Process each folder
    for folder_idx, run_folder in enumerate(run_folders, 1):
        logger.info("=" * 70)
        logger.info(
            f"Processing folder {folder_idx}/{len(run_folders)}: {run_folder.name}"
        )
        logger.info("=" * 70)

        # Load metadata from this folder
        metadata_file = run_folder / "batch_metadata.json"
        batch_file_metadata = {}
        if metadata_file.exists():
            with open(metadata_file, "r", encoding="utf-8") as f:
                batch_file_metadata = json.load(f)
            logger.info(f"Loaded batch metadata from {metadata_file.name}")
        else:
            logger.warning(f"Metadata file not found in {run_folder.name}")

        # Find all .jsonl files in this folder (excluding .ndjson files)
        batch_files = sorted(run_folder.glob("*.jsonl"))

        if not batch_files:
            logger.warning(f"No .jsonl files found in {run_folder.name}")
            continue

        logger.info(f"Found {len(batch_files)} batch file(s) in {run_folder.name}")

        folder_success = True  # Track if all files in folder succeeded

        # Process each file in this folder
        for file_idx, batch_file in enumerate(batch_files, 1):
            logger.info("-" * 70)
            logger.info(
                f"Processing file {file_idx}/{len(batch_files)}: {batch_file.name}"
            )
            logger.info("-" * 70)

            uploader.total_files_processed += 1

            try:
                # Count requests in file
                num_requests = await count_requests_in_file(batch_file)
                logger.info(f"File contains {num_requests:,} requests")

                # Get metadata for this specific file
                file_metadata = batch_file_metadata.get(batch_file.name, {})
                num_manufacturers = file_metadata.get("manufacturers", 0)
                num_tokens = file_metadata.get("tokens", 0)

                # Prepare metadata - OpenAI requires all metadata values to be strings
                metadata = {
                    "original_filename": batch_file.name,
                    "run_folder": run_folder.name,
                    "num_requests": str(num_requests),
                    "num_manufacturers": str(num_manufacturers),
                    "num_tokens": str(num_tokens),
                    "uploaded_at": datetime.now(UTC).isoformat(),
                }

                logger.info(f"Metadata: {metadata}")

                # Upload and create batch
                gpt_batch = await uploader.upload_and_create_batch(batch_file, metadata)

                if gpt_batch:
                    uploader.total_files_uploaded += 1

                    # Extract custom IDs and update GPTBatchRequest documents
                    logger.info("Extracting custom IDs from file...")
                    custom_ids = await extract_custom_ids_from_file(batch_file)
                    logger.info(f"Found {len(custom_ids)} custom IDs")

                    # Update GPTBatchRequest documents
                    updated_count = await update_batch_requests_with_batch_id(
                        custom_ids, gpt_batch.external_batch_id
                    )

                    logger.info(
                        f"✅ Successfully processed {batch_file.name} "
                        f"(batch_id: {gpt_batch.external_batch_id}, "
                        f"updated {updated_count} requests)"
                    )
                else:
                    uploader.total_files_failed += 1
                    folder_success = False
                    logger.error(f"❌ Failed to process {batch_file.name}")

            except Exception as e:
                uploader.total_files_failed += 1
                folder_success = False
                logger.error(
                    f"❌ Error processing {batch_file.name}: {e}", exc_info=True
                )

        # Move entire folder if all files succeeded
        if folder_success:
            logger.info(f"All files in {run_folder.name} processed successfully")
            await move_processed_folder(run_folder, uploaded_dir)
            logger.info(f"✅ Moved {run_folder.name} to uploaded_batch_requests")
        else:
            logger.warning(f"Some files in {run_folder.name} failed, folder not moved")

    # Log final summary
    logger.info("=" * 70)
    logger.info("UPLOAD COMPLETE")
    logger.info("=" * 70)
    logger.info(f"Folders processed: {len(run_folders)}")
    logger.info(f"Files processed: {uploader.total_files_processed}")
    logger.info(f"Files uploaded successfully: {uploader.total_files_uploaded}")
    logger.info(f"Files failed: {uploader.total_files_failed}")
    logger.info(f"Batches created: {uploader.total_batches_created}")
    logger.info(f"Uploaded folders moved to: {uploaded_dir}")
    logger.info("")
    logger.info("Upload distribution per API key:")
    for key_label, count in uploader.uploads_per_key.items():
        logger.info(f"  {key_label}: {count}/{uploader.max_uploads_per_key} files")
    logger.info("=" * 70)


async def async_main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Upload batch files to OpenAI and create batches"
    )
    parser.add_argument(
        "--input-dir",
        type=str,
        default="./batch_requests_output",
        help="Input directory containing batch_*.jsonl files (default: ./batch_requests_output)",
    )
    parser.add_argument(
        "--uploaded-dir",
        type=str,
        default="./batch_requests_output/uploaded_batch_requests",
        help="Directory to move uploaded files to (default: ./batch_requests_output/uploaded_batch_requests)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Maximum number of files to process (for testing)",
    )
    parser.add_argument(
        "--yes",
        "-y",
        action="store_true",
        help="Skip confirmation prompt and proceed automatically",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Check existing files and batches for each API key without uploading",
    )

    args = parser.parse_args()

    # Check if API keys are configured
    if not OPENAI_API_KEYS:
        print("\n⚠️  WARNING: No API keys configured!")
        print(
            "Please add your OpenAI API keys to the OPENAI_API_KEYS list in the script."
        )
        return

    # If check mode, initialize MongoDB and then check
    if args.check:
        # Initialize MongoDB
        logger.info("Initializing MongoDB connection...")
        await init_db(
            max_pool_size=5,
            min_pool_size=1,
            max_idle_time_ms=3600000,  # 1 hour
            server_selection_timeout_ms=30000,
            connect_timeout_ms=30000,
            socket_timeout_ms=3600000,  # 1 hour for long-running operations
        )
        await check_existing_files_and_batches(OPENAI_API_KEYS)
        return

    # Convert paths to Path objects
    input_dir = Path(args.input_dir)
    uploaded_dir = Path(args.uploaded_dir)

    # Check if input directory exists
    if not input_dir.exists():
        logger.error(f"Input directory does not exist: {input_dir}")
        return

    # Count run folders
    run_folders = [
        d
        for d in input_dir.iterdir()
        if d.is_dir() and d.name != "uploaded_batch_requests"
    ]
    if not run_folders:
        logger.warning(f"No run folders found in {input_dir}")
        return

    # Initialize MongoDB
    logger.info("Initializing MongoDB connection...")
    await init_db(
        max_pool_size=5,
        min_pool_size=1,
        max_idle_time_ms=3600000,  # 1 hour
        server_selection_timeout_ms=30000,
        connect_timeout_ms=30000,
        socket_timeout_ms=3600000,  # 1 hour for long-running operations
    )

    # Show configuration
    print("\n" + "=" * 70)
    print("BATCH FILE UPLOAD")
    print("=" * 70)
    print(f"Input directory: {input_dir}")
    print(f"Uploaded directory: {uploaded_dir}")
    print(f"Run folders found: {len(run_folders)}")
    if args.limit:
        print(f"Limit: {args.limit} (testing mode)")
        print(f"Will process: {min(args.limit, len(run_folders))} folder(s)")
    else:
        print(f"Will process: {len(run_folders)} folder(s) (ALL)")
    print(f"API keys configured: {len(OPENAI_API_KEYS)}")
    print("=" * 70)

    # Ask for confirmation (unless --yes flag is set)
    if not args.yes:
        response = input("\nDo you want to proceed? (yes/no): ").strip().lower()
        if response not in ["yes", "y"]:
            logger.info("Upload cancelled by user")
            print("Upload cancelled.")
            return
    else:
        logger.info("Auto-proceeding with --yes flag")

    print("\nStarting upload...\n")

    # Process batch files
    await process_batch_files(
        input_dir=input_dir,
        uploaded_dir=uploaded_dir,
        limit=args.limit,
    )


def main():
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
