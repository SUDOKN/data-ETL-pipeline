import asyncio
import logging
from datetime import datetime
from dataclasses import dataclass
from pathlib import Path
from openai import OpenAI, OpenAIError, APIConnectionError
from typing import Optional, Callable, Awaitable

from core.utils.batch_jsonl_file_writer import JSONLBatchFile
from core.models.db.gpt_batch import GPTBatch
from core.services.gpt_batch_service import (
    insert_gpt_batch_from_response,
    upsert_latest_gpt_batch_by_external_batch,
)
from core.models.jsonl_batch_file import (
    JSONLBatchFile,
)
from core.models.db.api_key_bundle import APIKeyBundle
from core.utils.time_util import get_current_time, get_timestamp_str
from open_ai_key_app.utils.openai_file_util import (
    download_openai_file,
    delete_uploaded_batch_file_from_openai,
)


logger = logging.getLogger(__name__)


@dataclass
class BatchDownloadOutput:
    output_file_path: Path
    error_file_path: Path | None

    def delete_batch_file_from_openai_and_move_output(
        self, client: OpenAI, input_file_id: str, finished_batches_dir: Path
    ):
        logger.warning(
            f"Deleting {input_file_id} from openai, moving {self.output_file_path} and {self.error_file_path} to {finished_batches_dir}"
        )
        delete_uploaded_batch_file_from_openai(
            client=client, input_file_id=input_file_id
        )
        output_dest = finished_batches_dir / self.output_file_path.name
        self.output_file_path.rename(output_dest)
        if self.error_file_path:
            error_dest = finished_batches_dir / self.error_file_path.name
            self.error_file_path.rename(error_dest)


# Type alias for the callbacks
BatchCompletionCallback = Callable[
    [APIKeyBundle, datetime, GPTBatch, BatchDownloadOutput, OpenAI], Awaitable[None]
]
BatchFailedCallback = Callable[
    [APIKeyBundle, datetime, GPTBatch, OpenAI], Awaitable[None]
]
BatchExpiredCallback = Callable[
    [APIKeyBundle, datetime, GPTBatch, OpenAI], Awaitable[None]
]


class BatchFileSatellite:
    download_dir: Path
    on_batch_completed: BatchCompletionCallback
    on_batch_failed: BatchFailedCallback
    on_batch_expired: BatchExpiredCallback

    def __init__(
        self,
        output_dir: Path,
        on_batch_completed: BatchCompletionCallback,
        on_batch_failed: BatchFailedCallback,
        on_batch_expired: BatchExpiredCallback,
    ) -> None:
        self.download_dir = output_dir
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self.on_batch_completed = on_batch_completed
        self.on_batch_failed = on_batch_failed
        self.on_batch_expired = on_batch_expired

    async def try_uploading_new_batch_file(
        self,
        client: OpenAI,
        api_key_bundle: APIKeyBundle,
        jsonl_batch_file: JSONLBatchFile,
    ) -> Optional[GPTBatch]:
        per_key_retries = 3  # Retry connection errors 3 times per key
        base_backoff = 5.0  # Base backoff in seconds
        attempt = 0

        while attempt < per_key_retries:
            try:
                logger.info(
                    f"Uploading file: {jsonl_batch_file.name} "
                    f"(key: {api_key_bundle.label}, "
                    f"attempt {attempt + 1}/{per_key_retries})"
                )
                with open(jsonl_batch_file.full_path, "rb") as f:
                    batch_input_file = client.files.create(file=f, purpose="batch")
                logger.info(
                    f"File uploaded successfully: {batch_input_file.id} ({jsonl_batch_file.full_path.name}) "
                    f"using key: {api_key_bundle.label}"
                )

                logger.info(
                    f"Creating batch for input file: {batch_input_file.id} "
                    f"(key: {api_key_bundle.label})"
                )
                summary = jsonl_batch_file.get_summary()
                batch_response = client.batches.create(
                    input_file_id=batch_input_file.id,
                    endpoint="/v1/chat/completions",
                    completion_window="24h",
                    metadata={
                        "original_filename": jsonl_batch_file.name,
                        "num_manufacturers": str(summary.unique_items),
                        "total_requests": str(summary.request_count),
                        "total_tokens": str(summary.total_tokens),
                        "api_key_label": api_key_bundle.label,
                    },
                )
                logger.info(
                    f"Batch created successfully: {batch_response.id} "
                    f"using key: {api_key_bundle.label}"
                )
                return await insert_gpt_batch_from_response(
                    batch_response, api_key_bundle.label
                )
            except APIConnectionError as e:
                # Connection error - retry on same key with backoff
                attempt += 1
                backoff_time = base_backoff * (2 ** (attempt - 1))
                logger.warning(
                    f"Connection error with key '{api_key_bundle.label}' "
                    f"(attempt {attempt}/{per_key_retries}): {e}"
                )
                if attempt < per_key_retries:
                    logger.info(
                        f"Retrying same key after {backoff_time:.1f}s backoff..."
                    )
                    await asyncio.sleep(backoff_time)
                else:
                    logger.error(
                        f"Connection failed after {per_key_retries} attempts with key '{api_key_bundle.label}'"
                    )
                    await api_key_bundle.apply_cooldown(
                        cooldown_for_seconds=10 * 60
                    )  # 10 minutes cooldown

                continue

            except OpenAIError as e:
                logger.error(
                    f"OpenAI API error with key '{api_key_bundle.label}' {e}",
                    exc_info=True,
                )
                await api_key_bundle.apply_cooldown(
                    cooldown_for_seconds=5 * 60
                )  # 5 minutes cooldown
                break  # Do not retry on other OpenAI errors

            except Exception as e:
                logger.error(
                    f"Unexpected error uploading {jsonl_batch_file.name} "
                    f"with key '{api_key_bundle.label}': {e}",
                    exc_info=True,
                )
                await api_key_bundle.apply_cooldown(
                    cooldown_for_seconds=5 * 60
                )  # 5 minutes cooldown
                break

    def download_batch_output(
        self, client: OpenAI, gpt_batch: GPTBatch, timestamp: datetime
    ) -> BatchDownloadOutput:
        if gpt_batch.status != "completed":
            raise ValueError(
                f"Batch {gpt_batch.id} is not completed. Current status: {gpt_batch.status}"
            )

        # timestamp_str = get_timestamp_str(timestamp=timestamp)
        # download_folder = self.download_dir

        # download the output file
        if not gpt_batch.output_file_id:
            raise ValueError(
                f"Can't download outbut because gpt_batch.output_file_id is {gpt_batch.output_file_id}"
            )

        output_filename = f"{gpt_batch.external_batch_id}_output.jsonl"
        output_file_path = self.download_dir / output_filename
        download_openai_file(
            client=client,
            output_type="output",
            output_path=output_file_path,
            openai_file_id=gpt_batch.output_file_id,
        )

        error_file_path = None
        if gpt_batch.error_file_id:
            error_filename = f"{gpt_batch.external_batch_id}_error.jsonl"
            error_file_path = self.download_dir / error_filename
            download_openai_file(
                client=client,
                output_type="error",
                output_path=error_file_path,
                openai_file_id=gpt_batch.error_file_id,
            )

        return BatchDownloadOutput(
            output_file_path=output_file_path, error_file_path=error_file_path
        )

    async def sync_batch(self, client: OpenAI, api_key_bundle: APIKeyBundle) -> None:
        """
        Update batch file record.
        If status is errored, mark API key for cooldown.
        If status is completed, download the result file.
        """
        if not api_key_bundle.latest_external_batch_id:
            raise ValueError(
                f"sync_batch: Nothing to sync, api_key_bundle.latest_external_batch_id is {api_key_bundle.latest_external_batch_id}"
            )
        timestamp = get_current_time()
        try:
            batch_response = client.batches.retrieve(
                api_key_bundle.latest_external_batch_id
            )

            logger.info(
                f"sync_batch: Batch {api_key_bundle.latest_external_batch_id} {batch_response.status}. "
            )
            gpt_batch = await upsert_latest_gpt_batch_by_external_batch(
                external_batch=batch_response, api_key_bundle=api_key_bundle
            )
            if gpt_batch.is_processing_complete():
                logger.info(
                    f"sync_batch: Batch {api_key_bundle.label}:{api_key_bundle.latest_external_batch_id} is already processed"
                )
                await api_key_bundle.mark_batch_inactive(
                    updated_at=timestamp
                )  # makes latest_external_batch_id None
                return
            else:
                logger.info(
                    f"sync_batch: Batch {api_key_bundle.label}:{api_key_bundle.latest_external_batch_id} is due for processing"
                )

            if batch_response.status == "failed":
                logger.info(
                    f"sync_batch: Invoking failed callback for batch {api_key_bundle.label}:{api_key_bundle.latest_external_batch_id}"
                )
                await self.on_batch_failed(api_key_bundle, timestamp, gpt_batch, client)
            elif batch_response.status == "completed":
                logger.info(
                    f"sync_batch: Batch {api_key_bundle.label}:{gpt_batch.external_batch_id} completed!"
                )
                download_result: BatchDownloadOutput = self.download_batch_output(
                    client=client, gpt_batch=gpt_batch, timestamp=timestamp
                )

                # Notify the observer (e.g.: BatchFileStation)
                if self.on_batch_completed and download_result:
                    logger.info(
                        f"sync_batch: Invoking completion callback for batch {api_key_bundle.label}:{gpt_batch.external_batch_id}"
                    )
                    await self.on_batch_completed(
                        api_key_bundle,
                        timestamp,
                        gpt_batch,
                        download_result,
                        client,
                    )
            elif batch_response.status == "expired":
                # shouldn't happen, we should have downloaded the file already in a previous sync
                # if file not already downloaded, then reset batch requests
                logger.info(
                    f"sync_batch: Invoking expired callback for batch {api_key_bundle.label}:{gpt_batch.external_batch_id}"
                )
                await self.on_batch_expired(
                    api_key_bundle, timestamp, gpt_batch, client
                )
            else:
                logger.info(
                    f"sync_batch: Batch {api_key_bundle.label}:{gpt_batch.external_batch_id} ({batch_response.status}) not ready yet. "
                    f"API key must already have an active batch [has_active_batch:{api_key_bundle.has_active_batch()}]"
                )
                # no need to apply cooldown, key must already have been marked in use.
        except OpenAIError as e:
            logger.error(
                f"OpenAI API error syncing batch {api_key_bundle.latest_external_batch_id}: {e}",
                exc_info=True,
            )
        except Exception as e:
            logger.error(
                f"Unexpected error syncing batch {api_key_bundle.latest_external_batch_id}: {e}",
                exc_info=True,
            )
