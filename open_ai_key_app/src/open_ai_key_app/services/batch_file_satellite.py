import asyncio
import logging
from datetime import datetime
from dataclasses import dataclass
from pathlib import Path
from openai import OpenAI, OpenAIError, APIConnectionError
from openai.types import Batch
from typing import Optional, Callable, Awaitable

from core.utils.batch_jsonl_file_writer import JSONLBatchFile
from core.models.db.gpt_batch import GPTBatch, GPTBatchMetadata
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
    upload_file_to_openai_using_parts,
)
from open_ai_key_app.utils.openai_batch_util import fetch_all_batches


logger = logging.getLogger(__name__)


@dataclass
class BatchDownloadOutput:
    output_file_path: Path
    error_file_path: Path | None

    def delete_batch_file_from_openai_and_move_output(
        self, client: OpenAI, input_file_id: str, finished_batches_dir: Path
    ):
        try:
            logger.warning(
                f"[{client.api_key}]: Deleting {input_file_id} from openai, moving {self.output_file_path} and {self.error_file_path} to {finished_batches_dir}"
            )
            delete_uploaded_batch_file_from_openai(
                client=client, input_file_id=input_file_id
            )
            output_dest = finished_batches_dir / self.output_file_path.name
            self.output_file_path.rename(output_dest)
            if self.error_file_path:
                error_dest = finished_batches_dir / self.error_file_path.name
                self.error_file_path.rename(error_dest)
        except Exception as e:
            logger.error(
                f"Error deleting batch file {input_file_id} from OpenAI or moving downloaded files: {e}",
                exc_info=True,
            )


class BatchFileSatellite:
    download_dir: Path

    def __init__(
        self,
        output_dir: Path,
    ) -> None:
        self.download_dir = output_dir
        self.download_dir.mkdir(parents=True, exist_ok=True)

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
                batch_input_file_id = await upload_file_to_openai_using_parts(
                    client=client, jsonl_batch_file=jsonl_batch_file
                )
                if not batch_input_file_id:
                    raise ValueError("upload_file_to_openai_using_parts returned None")
                    # will catch below and retry

                logger.info(
                    f"File uploaded successfully: {batch_input_file_id} ({jsonl_batch_file.full_path.name}) "
                    f"using key: {api_key_bundle.label}"
                )

                logger.info(
                    f"Creating batch for input file: {batch_input_file_id} "
                    f"(key: {api_key_bundle.label})"
                )
                summary = jsonl_batch_file.get_summary()
                metadata = GPTBatchMetadata(
                    original_filename=jsonl_batch_file.name,
                    num_manufacturers=summary.unique_items,
                    total_requests=summary.request_count,
                    total_tokens=summary.total_tokens,
                    api_key_label=api_key_bundle.label,
                )
                batch_response = client.batches.create(
                    input_file_id=batch_input_file_id,
                    endpoint="/v1/chat/completions",
                    completion_window="24h",
                    metadata={
                        k: str(v) for k, v in metadata.model_dump().items()
                    },  # Convert all values to strings
                )
                logger.info(
                    f"Batch created successfully: {batch_response.id} "
                    f"using key: {api_key_bundle.label}"
                )
                return await insert_gpt_batch_from_response(
                    batch_response=batch_response,
                    metadata=metadata,
                    api_key_label=api_key_bundle.label,
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
        self, client: OpenAI, gpt_batch: GPTBatch
    ) -> BatchDownloadOutput:
        # download the output file
        if not gpt_batch.output_file_id:
            raise ValueError(
                f"download_batch_output: Can't download output because gpt_batch.output_file_id is {gpt_batch.output_file_id}"
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

    async def get_synced_gpt_batches(
        self, client: OpenAI, api_key_bundle: APIKeyBundle
    ) -> list[GPTBatch]:
        all_batches: list[Batch] = fetch_all_batches(client=client)

        synced_gpt_batches = await asyncio.gather(
            *[
                upsert_latest_gpt_batch_by_external_batch(
                    external_batch=batch,
                    api_key_bundle=api_key_bundle,
                )
                for batch in all_batches
            ]
        )

        return synced_gpt_batches
