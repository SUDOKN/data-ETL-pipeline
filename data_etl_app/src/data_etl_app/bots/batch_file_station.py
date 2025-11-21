import asyncio
import httpx
import json
import logging
from dataclasses import dataclass
from pymongo import UpdateOne
from datetime import datetime
from pathlib import Path
from typing import Optional
from openai import OpenAI

from core.dependencies.load_core_env import load_core_env
from scraper_app.dependencies.load_scraper_env import load_scraper_env
from open_ai_key_app.dependencies.load_open_ai_app_env import load_open_ai_app_env
from data_etl_app.dependencies.load_data_etl_env import load_data_etl_env

# Load environment variables (entry point)
load_core_env()
load_scraper_env()
load_data_etl_env()
load_open_ai_app_env()


from core.models.db.api_key_bundle import APIKeyBundle
from core.models.db.gpt_batch import GPTBatch, GPTBatchStatus
from core.services.gpt_batch_request_service import (
    bulk_update_gpt_batch_requests,
    get_custom_ids_for_batch,
    pair_batch_request_custom_ids_with_batch,
    unpair_all_batch_requests_from_batch,
)
from core.services.api_key_service import (
    get_all_api_key_bundles,
)
from core.services.manufacturer_service import find_manufacturers_by_etld1s
from core.utils.time_util import get_current_time

from data_etl_app.services.batch_file_generator import (
    BatchFileGenerationResult,
    iterate_df_manufacturers_and_write_batch_files,
)
from data_etl_app.utils.gpt_batch_request_util import (
    parse_individual_batch_req_response_raw,
)
from data_etl_app.services.batch_file_satellite import (
    BatchFileSatellite,
    BatchDownloadOutput,
)
from data_etl_app.services.manufacturer_extraction_orchestrator import (
    ManufacturerExtractionOrchestrator,
)


logger = logging.getLogger(__name__)

OUTPUT_DIR_DEFAULT = "../../../../batch_data"
MAX_MANUFACTURER_TOKENS = 200_000
MAX_REQUESTS_PER_FILE = 50_000
MAX_FILE_SIZE_MB = 190  # 190MB in MB

FINISHED_BATCHES_DIR_DEFAULT = Path(OUTPUT_DIR_DEFAULT + "/finished_batches")

# Note: This filter only checks token size, not whether manufacturers have pending requests.
# Some manufacturers passing this filter may have no pending requests (all completed/in-progress).
# The batch_file_generator handles this by skipping manufacturers with no pending requests.
DF_MFG_BATCH_FILTER = {
    "scraped_text_file_num_tokens": {"$lt": MAX_MANUFACTURER_TOKENS},
}


@dataclass
class BatchFileStationStats:
    batches_created: int = 0
    batches_uploaded: int = 0
    batches_downloaded: int = 0
    batches_succeeded: int = 0
    batches_failed: int = 0
    batches_expired: int = 0
    mfg_completed: int = 0


@dataclass
class SingleBatchStats:
    total_output_lines: int = 0
    failed_output_parses: int = 0
    total_error_lines: int = 0
    failed_error_parses: int = 0
    upserted: int = 0  # must remain zero lol
    updated: int = 0
    output_errors: int = 0
    error_file_errors: int = 0


class BatchFileStation:
    _instance: "BatchFileStation | None" = None
    stats: BatchFileStationStats
    mfg_intake_orchestrator: ManufacturerExtractionOrchestrator
    satellite: BatchFileSatellite

    def __init__(self):
        if BatchFileStation._instance is not None:
            raise RuntimeError(
                "BatchFileStation is a singleton. Use BatchFileStation.get_instance() instead."
            )
        self.mfg_intake_orchestrator = ManufacturerExtractionOrchestrator()
        self.satellite = BatchFileSatellite(
            output_dir=Path(FINISHED_BATCHES_DIR_DEFAULT)
        )
        self.stats = BatchFileStationStats()
        BatchFileStation._instance = self

    @classmethod
    def get_instance(cls) -> "BatchFileStation":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    async def start_loop(self, poll_interval_seconds):
        """
        Start the satellite's polling loop.
        When batches complete, handle_batch_completed will be called automatically.
        """
        logger.info("Starting BatchFileStation...")
        await self.poll_sync_and_upload_new_batches(poll_interval_seconds)

    async def _finish_gpt_batch_processing(
        self,
        done_at: datetime,
        client: OpenAI,
        gpt_batch: GPTBatch,
        api_key_bundle: APIKeyBundle,
        batch_download_output: Optional[BatchDownloadOutput],
    ):
        await api_key_bundle.remove_tokens_in_use(gpt_batch.metadata.total_tokens)
        await gpt_batch.mark_our_processing_complete(processing_completed_at=done_at)

        if batch_download_output:
            batch_download_output.delete_batch_file_from_openai_and_move_output(
                client=client,
                input_file_id=gpt_batch.input_file_id,
                finished_batches_dir=FINISHED_BATCHES_DIR_DEFAULT,
            )

        logger.info(f"Latest BatchFileStationStats:\n{self.stats}")

    async def handle_batch_failed(
        self,
        client: OpenAI,
        api_key_bundle: APIKeyBundle,
        timestamp: datetime,
        gpt_batch: GPTBatch,
    ):
        self.stats.batches_failed += 1
        await api_key_bundle.apply_cooldown(30 * 60)  # 30 mins
        await unpair_all_batch_requests_from_batch(gpt_batch=gpt_batch)
        await self._finish_gpt_batch_processing(
            done_at=timestamp,
            client=client,
            gpt_batch=gpt_batch,
            api_key_bundle=api_key_bundle,
            batch_download_output=None,
        )

    async def handle_batch_completed_or_expired(
        self,
        client: OpenAI,
        api_key_bundle: APIKeyBundle,
        downloaded_at: datetime,
        gpt_batch: GPTBatch,
        batch_download_output: BatchDownloadOutput,
    ):
        """
        1. Updates batch requests based on results in the passed JSONL result file.
        2. Then pushes each manufacturer into extraction pipeline. This may either reconcile
        the mfg completely or create more batch requests that can be picked up in a later scan.
        3. Find available API keys and create batches per key following individual quota limits.
        4. Upload batches
        """
        now = get_current_time()
        self.stats.batches_downloaded += 1
        log_id = f"{api_key_bundle.label}-{gpt_batch.external_batch_id}"
        update_operations = []
        batch_stats = SingleBatchStats()
        all_custom_ids: set[str] = await get_custom_ids_for_batch(gpt_batch)
        logger.info(
            f"{log_id}: Expecting {len(all_custom_ids):,} custom_ids in output/error files.\n"
            # f"all_custom_ids:{all_custom_ids}"
        )
        output_custom_ids: set[str] = set()
        # error_custom_ids: set[str] = set()
        unique_mfg_etld1s: set[str] = set()

        with open(f"{batch_download_output.output_file_path}", "r") as f:
            for line_num, line in enumerate(f):
                try:
                    batch_stats.total_output_lines += 1
                    raw_result = json.loads(line.strip())
                    custom_id: str = str(raw_result.get("custom_id"))
                    if not custom_id:
                        logger.warning(f"Line {line_num}: Missing custom_id, skipping")
                        batch_stats.failed_output_parses += 1
                        continue

                    response_blob = parse_individual_batch_req_response_raw(
                        raw_result, gpt_batch.external_batch_id
                    )

                    all_custom_ids.remove(custom_id)
                    output_custom_ids.add(custom_id)
                    mfg_etld1 = custom_id.split(">")[0]
                    unique_mfg_etld1s.add(mfg_etld1)

                    operation = UpdateOne(
                        {"request.custom_id": custom_id},  # Filter
                        {
                            "$set": {
                                "batch_id": gpt_batch.external_batch_id,
                                "response_blob": response_blob.model_dump(
                                    exclude={"result"}
                                ),
                            }
                        },
                        upsert=False,  # Doesn't create new documents if filter unmatched
                    )
                    update_operations.append(operation)

                except json.JSONDecodeError as e:
                    logger.error(f"Line {line_num}: JSON decode error - {e}")
                    batch_stats.failed_output_parses += 1
                except Exception as e:
                    logger.error(
                        f"Line {line_num}: Error processing result - {e}", exc_info=True
                    )
                    batch_stats.output_errors += 1

        if all_custom_ids:
            logger.error(
                f"{log_id}: Missing {len(all_custom_ids)} expected custom_ids in output/error files: "
                f"output[{len(output_custom_ids)}], Resetting their batch_id to None."
            )
            for custom_id in all_custom_ids:
                update_operations.append(
                    UpdateOne(
                        {"request.custom_id": custom_id},  # Filter
                        {"$set": {"batch_id": None}},
                        upsert=False,  # Doesn't create new documents if filter unmatched
                    )
                )

        if not update_operations:
            logger.warning(
                "No valid update operations found in batch results. "
                f"Skipping to creating new batch and upload."
            )
            await self._finish_gpt_batch_processing(
                done_at=now,
                client=client,
                gpt_batch=gpt_batch,
                api_key_bundle=api_key_bundle,
                batch_download_output=batch_download_output,
            )

        batch_stats.upserted, batch_stats.updated = (
            await bulk_update_gpt_batch_requests(  # this raises if any error occurs with any chunk in the bulk
                update_one_operations=update_operations,
                log_id=log_id,
            )
        )

        logger.info(f"{log_id}: Processed batch results, stats:\n" f"{batch_stats}")

        semaphore = asyncio.Semaphore(100)

        async def bounded_process(mfg):
            async with semaphore:
                await self.mfg_intake_orchestrator.process_manufacturer(
                    timestamp=downloaded_at,
                    mfg=mfg,
                )

        tasks = [
            bounded_process(mfg)
            for mfg in await find_manufacturers_by_etld1s(list(unique_mfg_etld1s))
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        exception_count = sum(1 for result in results if isinstance(result, Exception))
        self.stats.mfg_completed += len(results) - exception_count
        logger.info(
            f"{log_id}: Processed {len(results)} manufacturers. "
            f"Exceptions: {exception_count}, Completed: {self.stats.mfg_completed}"
        )

        self.stats.batches_succeeded += 1
        await self._finish_gpt_batch_processing(
            done_at=now,
            client=client,
            gpt_batch=gpt_batch,
            api_key_bundle=api_key_bundle,
            batch_download_output=batch_download_output,
        )
        await api_key_bundle.apply_cooldown(10 * 60)  # 10 mins cooldown

    async def process_batch(
        self,
        client: OpenAI,
        api_key_bundle: APIKeyBundle,
        gpt_batch: GPTBatch,
        timestamp: datetime,
    ):
        if gpt_batch.status in [
            GPTBatchStatus.FAILED,
            GPTBatchStatus.CANCELLING,
            GPTBatchStatus.CANCELLED,
        ]:
            logger.info(
                f"process_batch: Invoking failed callback for batch {api_key_bundle.label}:{gpt_batch.external_batch_id}"
            )
            await self.handle_batch_failed(
                client=client,
                api_key_bundle=api_key_bundle,
                timestamp=timestamp,
                gpt_batch=gpt_batch,
            )
        elif gpt_batch.status in [GPTBatchStatus.COMPLETED, GPTBatchStatus.EXPIRED]:
            logger.info(
                f"process_batch: Batch {api_key_bundle.label}:{gpt_batch.external_batch_id} completed!"
            )

            download_result: BatchDownloadOutput = self.satellite.download_batch_output(
                client=client, gpt_batch=gpt_batch
            )
            logger.info(
                f"process_batch: Downloaded output:{download_result} for batch {api_key_bundle.label}:{gpt_batch.external_batch_id}"
            )

            if download_result:
                logger.info(
                    f"sync_batch: Invoking completion callback for batch {api_key_bundle.label}:{gpt_batch.external_batch_id}"
                )
                await self.handle_batch_completed_or_expired(
                    client=client,
                    api_key_bundle=api_key_bundle,
                    downloaded_at=timestamp,
                    gpt_batch=gpt_batch,
                    batch_download_output=download_result,
                )
            else:
                logger.error(
                    f"process_batch: Failed to download batch output for {api_key_bundle.label}:{gpt_batch.external_batch_id}"
                )
        else:
            # Batch still in progress or validating or finalising etc.
            logger.info(
                f"process_batch: Batch {api_key_bundle.label}:{gpt_batch.external_batch_id} is still in progress with status {gpt_batch.status}."
            )

    async def poll_sync_and_upload_new_batches(self, poll_interval_seconds: int):
        logger.info(
            f"poll_sync_and_upload_new_batches: Starting batch upload loop (interval: {poll_interval_seconds}s)"
        )
        while True:
            try:
                now = get_current_time()
                api_key_bundles: list[APIKeyBundle] = await get_all_api_key_bundles()
                logger.info(f"fetched {len(api_key_bundles)} API key bundles.")
                for api_key_bundle in api_key_bundles:
                    # if api_key_bundle.label != "sudokn.tool7":
                    #     continue
                    logger.info(
                        f"poll_sync_and_upload_new_batches: Iterating API key bundle: {api_key_bundle.label}"
                    )
                    client = OpenAI(
                        api_key=api_key_bundle.key,
                        timeout=httpx.Timeout(
                            connect=60.0,  # time to establish TCP/TLS connection
                            read=1800.0,  # 30 minutes - time waiting for server response after upload
                            write=1800.0,  # 30 minutes - time allowed to upload request body (for 200MB files)
                            pool=30.0,  # time to get connection from pool
                        ),
                    )

                    if not api_key_bundle.is_available_now(now):
                        time_left = api_key_bundle.available_at - now
                        minutes, seconds = divmod(int(time_left.total_seconds()), 60)
                        logger.warning(
                            f"create_new_batches_and_upload: {api_key_bundle.label} is unavailable at the moment={now}, will be available in {minutes} mins {seconds} secs."
                        )
                        continue

                    # status: "validating", "in_progress", "finalising", etc.
                    synced_gpt_batches: list[GPTBatch] = (
                        await self.satellite.get_synced_gpt_batches(
                            client=client, api_key_bundle=api_key_bundle
                        )
                    )

                    api_key_bundle.tokens_in_use = 0  # reset before recounting
                    at_least_one_incomplete = False
                    for (
                        gpt_batch
                    ) in synced_gpt_batches:  # hopefully there is only one each time
                        if not gpt_batch.is_our_processing_complete():
                            logger.info(
                                f"poll_sync_and_upload_new_batches: Processing synced batch {api_key_bundle.label}:{gpt_batch.external_batch_id} with status {gpt_batch.status}"
                            )
                            api_key_bundle.tokens_in_use += (
                                gpt_batch.metadata.total_tokens
                            )
                            await self.process_batch(  # if completed/expired, process_batch will free up tokens_in_use
                                client=client,
                                api_key_bundle=api_key_bundle,
                                gpt_batch=gpt_batch,
                                timestamp=now,
                            )
                            at_least_one_incomplete = True

                    await api_key_bundle.save()  # save the updated tokens_in_use
                    if at_least_one_incomplete:
                        logger.info(
                            f"poll_sync_and_upload_new_batches: {api_key_bundle.label} had at least one incomplete batch that was processed, "
                            f"some cooldown may have been applied. Will wait for next iteration to create new batches."
                        )
                        continue

                    if api_key_bundle.tokens_in_use > 0:
                        logger.info(
                            f"create_new_batches_and_upload: {api_key_bundle.label} has {api_key_bundle.tokens_in_use} tokens in use, "
                            f"will wait before creating new batches."
                        )
                        continue

                    # proceed to generate batch files and try uploading
                    batch_file_generation_result: BatchFileGenerationResult = (
                        await iterate_df_manufacturers_and_write_batch_files(
                            timestamp=now,
                            query_filter=DF_MFG_BATCH_FILTER,
                            output_dir=Path(OUTPUT_DIR_DEFAULT),
                            max_requests_per_file=MAX_REQUESTS_PER_FILE,
                            max_tokens_per_file=api_key_bundle.batch_queue_limit,
                            max_file_size_in_bytes=MAX_FILE_SIZE_MB * 1024 * 1024,
                            max_files=1,
                            parallel_processing=True,  # Enable parallel processing
                            max_concurrent_manufacturers=100,  # Process 100 manufacturers concurrently
                        )
                    )
                    jsonl_batch_file = batch_file_generation_result.batch_request_jsonl_file_writer.files[
                        0
                    ]
                    if not jsonl_batch_file.unique_line_ids:
                        logger.error(
                            f"poll_sync_and_upload_new_batches: Batch file generation created empty file"
                        )
                        batch_file_generation_result.batch_request_jsonl_file_writer.delete_files()
                        continue

                    self.stats.batches_created += 1
                    new_gpt_batch = await self.satellite.try_uploading_new_batch_file(
                        client=client,
                        api_key_bundle=api_key_bundle,
                        jsonl_batch_file=jsonl_batch_file,
                    )
                    batch_file_generation_result.batch_request_jsonl_file_writer.delete_files()
                    if not new_gpt_batch:
                        logger.info(
                            f"create_new_batches_and_upload: Upload failed for {api_key_bundle.label}"
                        )
                        continue

                    await api_key_bundle.add_tokens_in_use(
                        new_gpt_batch.metadata.total_tokens
                    )
                    self.stats.batches_uploaded += 1
                    # Try pairing custom_ids with batch, retry once if it fails
                    for attempt in range(2):
                        try:
                            num_paired = await pair_batch_request_custom_ids_with_batch(
                                custom_ids=batch_file_generation_result.batch_request_jsonl_file_writer.files[
                                    0
                                ].unique_line_ids,
                                gpt_batch=new_gpt_batch,
                            )
                            logger.info(
                                f"poll_sync_and_upload_new_batches: Paired {num_paired} requests with batch {api_key_bundle.label}:{new_gpt_batch.external_batch_id} on attempt {attempt+1}"
                            )
                            break
                        except Exception as e:
                            logger.error(
                                f"OMGG: pair_batch_request_custom_ids_with_batch failed (attempt {attempt+1}): {e}"
                            )
                            if attempt == 1:
                                raise
                    # If the above call fails, we might send the same custom ids in the next batch
                    # pray it succeeds

                    logger.info(
                        f"poll_sync_and_upload_new_batches: Completed batch upload for {api_key_bundle.label}, with new batch:\n{new_gpt_batch}"
                    )
                # for loop ends
                logger.info(
                    f"poll_sync_and_upload_new_batches: Sleeping for {poll_interval_seconds} seconds..."
                )
                await asyncio.sleep(poll_interval_seconds)
            except Exception as e:
                logger.error(f"Error in polling loop: {e}", exc_info=True)
                await asyncio.sleep(poll_interval_seconds)


async def async_main():
    from core.dependencies.aws_clients import (
        initialize_core_aws_clients,
        cleanup_core_aws_clients,
    )
    from data_etl_app.dependencies.aws_clients import (
        initialize_data_etl_aws_clients,
        cleanup_data_etl_aws_clients,
    )

    from core.utils.mongo_client import init_db

    await init_db(
        max_pool_size=200,
        min_pool_size=50,
        socket_timeout_ms=300000,  # 5 minutes for bulk operations
        server_selection_timeout_ms=60000,  # 30 seconds
        connect_timeout_ms=60000,  # 30 seconds
    )
    await initialize_core_aws_clients()
    await initialize_data_etl_aws_clients()

    log_level = "INFO"
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    logger = logging.getLogger(__name__)
    logger.info(f"Starting batch file station with log level: {log_level}")
    batch_file_station = BatchFileStation.get_instance()
    POLL_INTERVAL = 5 * 60  # 1 mins
    try:
        await batch_file_station.start_loop(POLL_INTERVAL)
    finally:
        # Clean up AWS clients
        await cleanup_data_etl_aws_clients()
        await cleanup_core_aws_clients()


def main():
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
