from datetime import datetime
import asyncio
import json
import logging
from pathlib import Path
from typing import Optional

from core.models.gpt_batch_request_blob import GPTBatchRequestBlob
from core.models.jsonl_batch_file import (
    FileContentLimitReachedException,
    JSONLBatchFile,
)
from core.utils.time_util import get_timestamp_str

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


class MaxFilesReachedException(Exception):
    """Raised when the maximum number of batch files has been reached."""

    pass


class BatchRequestJSONLFileWriter:
    """Handles writing batch requests to JSONL files with constraints.

    Thread-safe for concurrent access using asyncio.Lock.
    """

    def __init__(
        self,
        output_dir: Path,
        run_timestamp: datetime,
        max_files: Optional[int],
        max_requests_per_file: int,
        max_tokens_per_file: int,
        max_file_size_in_bytes: int,
        common_prefix: str = "batch_requests",
    ):
        self.run_timestamp_str = get_timestamp_str(run_timestamp)
        self.output_dir = output_dir / self.run_timestamp_str
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.max_files = max_files
        self.max_requests_per_file = max_requests_per_file
        self.max_tokens_per_file = max_tokens_per_file
        self.max_file_size_in_bytes = max_file_size_in_bytes
        self.common_prefix = common_prefix

        self.files = []
        self.current_file_index = -1
        self.result_summary = {}
        self._lock = asyncio.Lock()  # Thread-safe lock for concurrent writes
        self._add_new_file()

    @property
    def current_file(self) -> JSONLBatchFile:
        return self.files[self.current_file_index]

    def _add_new_file(self):
        self.current_file_index += 1
        self.files.append(
            JSONLBatchFile(
                output_dir=self.output_dir,
                common_prefix=self.common_prefix,
                file_index=self.current_file_index,
                timestamp_str=self.run_timestamp_str,
                max_requests=self.max_requests_per_file,
                max_tokens=self.max_tokens_per_file,
                max_size_in_bytes=self.max_file_size_in_bytes,
            )
        )

    def _start_new_file(self):
        if self.current_file:
            self.current_file.close_pointer()
            file_summary = self.current_file.get_summary()
            self.result_summary[self.current_file.name] = file_summary
            logger.info(
                f"Closed {self.current_file.name}: "
                f"{file_summary.request_count:,} requests, {file_summary.total_tokens:,} tokens, "
                f"{file_summary.unique_items} unique items, {file_summary.unique_lines} unique lines"
                f"{self.current_file.size_in_bytes / (1024 * 1024):.2f} MB"
            )

        # Check if we've reached the max number of files
        if self.max_files is not None and self.current_file_index + 1 >= self.max_files:
            raise MaxFilesReachedException(
                f"Reached maximum number of files: {self.max_files}"
            )

        self._add_new_file()

    def _serialize_request(self, request_blob: GPTBatchRequestBlob) -> str:
        """Serialize a request blob to JSON string (without input_tokens)."""
        request_dict = request_blob.model_dump()
        request_dict["body"].pop("input_tokens", None)
        # Use separators for consistent, compact JSON output
        return json.dumps(request_dict, separators=(",", ":"), sort_keys=False)

    def _can_add_requests_of_single_item(
        self, item_id: str, request_blobs: list[GPTBatchRequestBlob]
    ) -> bool:
        """Check if all requests in a list can be added to the current batch file."""
        if not request_blobs:
            logger.debug(
                f"can_add_item_requests: No requests to add for {item_id}; returning True."
            )
            return True

        # Calculate the total size and tokens for all requests

        total_item_tokens = 0
        total_size_in_bytes = 0
        for req_blob in request_blobs:
            total_item_tokens += req_blob.body.input_tokens
            json_str = self._serialize_request(req_blob)
            total_size_in_bytes += JSONLBatchFile.get_json_line_size_in_bytes(json_str)

        # Check if adding all requests would exceed limits
        can_fit = self.current_file.can_batch_file_fit_item(
            item_tokens=total_item_tokens,
            item_request_count=len(request_blobs),
            item_size_in_bytes=total_size_in_bytes,
        )

        if not can_fit:
            logger.warning(
                f"Cannot fit item {item_id} into current file. "
                f"Item: {len(request_blobs)} requests, {total_item_tokens:,} tokens, {total_size_in_bytes:,} bytes. "
                f"Current file: {self.current_file.total_requests} requests, {self.current_file.total_tokens:,} tokens, {self.current_file.size_in_bytes:,} bytes. "
                f"Limits: {self.max_requests_per_file:,} requests, {self.max_tokens_per_file:,} tokens, {self.max_file_size_in_bytes:,} bytes."
            )

        return can_fit

    def write_item_request_blobs(
        self, item_id: str, request_blobs: list[GPTBatchRequestBlob]
    ):
        """Write all requests for a single item to the current batch file.

        Note: This is a synchronous wrapper that should be called with await
        in an async context for thread safety.
        """
        # logger.info(f"Writing {len(request_blobs):,} requests for item {item_id}")
        if not request_blobs:
            logger.debug(
                f"write_item_request_blobs: No requests to write for {item_id}; skipping."
            )
            return

        # if not self._can_add_requests_of_single_item(item_id, request_blobs):
        #     self._start_new_file()  # updates self.current_file to a new file

        i = 0
        n = len(request_blobs)
        while i < n:
            try:
                req_blob = request_blobs[i]
                is_last_item_line = i == n - 1
                self.current_file.add_json_line(
                    item_id=item_id,
                    line_id=req_blob.custom_id,
                    json_line=self._serialize_request(req_blob),
                    tokens=req_blob.body.input_tokens,
                    is_last_item_line=is_last_item_line,
                )
                i += 1
            except FileContentLimitReachedException as e:
                logger.warning(
                    f"File limits reached when adding request {req_blob.custom_id} for item {item_id}: {e}. "
                    f"Starting new file and retrying."
                )
                self._start_new_file()
            except Exception as e:
                logger.error(
                    f"Error writing request {req_blob.custom_id} for item {item_id}: {e}"
                )
                raise e

    async def write_item_request_blobs_async(
        self, item_id: str, request_blobs: list[GPTBatchRequestBlob]
    ):
        """Thread-safe async version of write_item_request_blobs.

        Use this method when processing manufacturers in parallel to ensure
        safe concurrent writes to batch files.
        """
        async with self._lock:
            self.write_item_request_blobs(item_id, request_blobs)

    def delete_files(self):
        """Delete all created batch files from disk."""
        for batch_file in self.files:
            batch_file.delete_file()
