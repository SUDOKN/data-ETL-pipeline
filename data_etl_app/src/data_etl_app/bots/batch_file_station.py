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
from core.models.db.gpt_batch import GPTBatch
from core.services.gpt_batch_request_service import (
    bulk_update_gpt_batch_requests,
    pair_batch_request_custom_ids_with_batch,
    reset_batch_requests_with_batch,
)
from core.services.api_key_service import (
    get_all_api_key_bundles,
)
from core.services.manufacturer_service import find_manufacturers_by_etld1s
from core.utils.time_util import get_current_time

from open_ai_key_app.utils.openai_file_util import find_latest_batch_of_api_key_bundle

from data_etl_app.services.batch_file_generator import (
    BatchFileGenerationResult,
    iterate_df_manufacturers_and_write_batch_files,
)

# from data_etl_app.scripts.create_batch_files import (
#     MAX_FILE_SIZE_MB,
#     MAX_MANUFACTURER_TOKENS,
#     MAX_REQUESTS_PER_FILE,
#     OUTPUT_DIR_DEFAULT,
# )
from data_etl_app.utils.gpt_batch_request_util import (
    parse_individual_batch_req_response_raw,
)
from data_etl_app.bots.batch_file_satellite import (
    BatchFileSatellite,
    BatchDownloadOutput,
)
from data_etl_app.scripts.batch_request_orchestrator import (
    ManufacturerExtractionOrchestrator,
    process_single_manufacturer,
)


logger = logging.getLogger(__name__)

OUTPUT_DIR_DEFAULT = "../../../../batch_data"
MAX_MANUFACTURER_TOKENS = 200_000
MAX_TOKENS_PER_FILE = 20_000_000
MAX_REQUESTS_PER_FILE = 40_000
MAX_FILE_SIZE_MB = 120  # 120MB in MB

FINISHED_BATCHES_DIR_DEFAULT = Path(OUTPUT_DIR_DEFAULT + "/finished_batches")
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
            output_dir=Path(FINISHED_BATCHES_DIR_DEFAULT),
            on_batch_completed=self.handle_batch_completed,
            on_batch_failed=self.handle_batch_failed,
            on_batch_expired=self.handle_batch_expired,
        )
        self.stats = BatchFileStationStats()
        BatchFileStation._instance = self

    @classmethod
    def get_instance(cls) -> "BatchFileStation":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    async def start_loop(self, poll_interval_seconds: int = 300):
        """
        Start the satellite's polling loop.
        When batches complete, handle_batch_completed will be called automatically.
        """
        logger.info("Starting BatchFileStation...")
        """
        await asyncio.gather(
            self.satellite.poll_and_sync_all_batches(poll_interval_seconds),
            self.iterate_available_keys_and_upload_new_batches(poll_interval_seconds),
        )
        """
        await self.poll_sync_and_upload_new_batches(poll_interval_seconds)

    async def finish_gpt_batch_processing(
        self,
        done_at: datetime,
        client: OpenAI,
        gpt_batch: GPTBatch,
        api_key_bundle: APIKeyBundle,
        batch_download_output: Optional[BatchDownloadOutput],
    ):
        if batch_download_output:
            batch_download_output.delete_batch_file_from_openai_and_move_output(
                client=client,
                input_file_id=gpt_batch.input_file_id,
                finished_batches_dir=FINISHED_BATCHES_DIR_DEFAULT,
            )

        if api_key_bundle.latest_external_batch_id != gpt_batch.external_batch_id:
            raise ValueError(
                f"Error: Current batch does not match passed batch!! "
                f"self.latest_external_batch_id={api_key_bundle.latest_external_batch_id}\n"
                f"gpt_batch.external_batch_id={gpt_batch.external_batch_id}\n"
            )

        await api_key_bundle.mark_batch_inactive(
            updated_at=done_at
        )  # makes latest_external_batch_id None

        await gpt_batch.mark_processing_complete(processing_completed_at=done_at)

        logger.info(f"Latest BatchFileStationStats:\n{self.stats}")

    async def handle_batch_failed(
        self,
        api_key_bundle: APIKeyBundle,
        timestamp: datetime,
        gpt_batch: GPTBatch,
        client: OpenAI,
    ) -> None:
        self.stats.batches_failed += 1
        await api_key_bundle.apply_cooldown(10 * 60)  # 10 mins
        await reset_batch_requests_with_batch(gpt_batch=gpt_batch)
        await self.finish_gpt_batch_processing(
            done_at=timestamp,
            client=client,
            gpt_batch=gpt_batch,
            api_key_bundle=api_key_bundle,
            batch_download_output=None,
        )

    async def handle_batch_expired(
        self,
        api_key_bundle: APIKeyBundle,
        timestamp: datetime,
        gpt_batch: GPTBatch,
        client: OpenAI,
    ) -> None:
        self.stats.batches_expired += 1
        await reset_batch_requests_with_batch(gpt_batch=gpt_batch)
        await self.finish_gpt_batch_processing(
            done_at=timestamp,
            client=client,
            gpt_batch=gpt_batch,
            api_key_bundle=api_key_bundle,
            batch_download_output=None,
        )

    async def handle_batch_completed(
        self,
        api_key_bundle: APIKeyBundle,
        downloaded_at: datetime,
        gpt_batch: GPTBatch,
        batch_download_output: BatchDownloadOutput,
        client: OpenAI,
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
        batch_stats = {
            "total_lines": 0,
            "failed_parses": 0,
            "upserted": 0,  # must remain zero lol
            "updated": 0,
            "errors": 0,
        }
        unique_mfg_etld1s: set[str] = set()

        with open(f"{batch_download_output.output_file_path}", "r") as f:
            for line_num, line in enumerate(f):
                try:
                    batch_stats["total_lines"] += 1
                    raw_result = json.loads(line.strip())
                    custom_id: str = str(raw_result.get("custom_id"))
                    if not custom_id:
                        logger.warning(f"Line {line_num}: Missing custom_id, skipping")
                        batch_stats["failed_parses"] += 1
                        continue

                    mfg_etld1 = custom_id.split(">")[0]
                    unique_mfg_etld1s.add(mfg_etld1)

                    response_blob = parse_individual_batch_req_response_raw(
                        raw_result, gpt_batch.external_batch_id
                    )
                    operation = UpdateOne(
                        {"request.custom_id": custom_id},  # Filter
                        {
                            "$set": {
                                "response_blob": response_blob.model_dump(
                                    exclude={"result"}
                                )
                            }
                        },
                        upsert=False,  # Doesn't create new documents if filter unmatched
                    )
                    update_operations.append(operation)
                except json.JSONDecodeError as e:
                    logger.error(f"Line {line_num}: JSON decode error - {e}")
                    batch_stats["failed_parses"] += 1
                    update_operations.append(
                        UpdateOne(
                            {"request.custom_id": custom_id},  # Filter
                            {"$set": {"batch_id": None}},
                            upsert=False,  # Doesn't create new documents if filter unmatched
                        )
                    )
                except Exception as e:
                    logger.error(f"Line {line_num}: Error processing result - {e}")
                    batch_stats["errors"] += 1
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
            return await self.finish_gpt_batch_processing(
                done_at=now,
                client=client,
                gpt_batch=gpt_batch,
                api_key_bundle=api_key_bundle,
                batch_download_output=batch_download_output,
            )

        batch_stats["upserted"], batch_stats["updated"] = (
            await bulk_update_gpt_batch_requests(
                update_one_operations=update_operations,
                log_id=log_id,
            )
        )

        logger.info(f"{log_id}: Processed batch results, stats:\n" f"{batch_stats}")

        semaphore = asyncio.Semaphore(100)

        async def bounded_process(mfg):
            async with semaphore:
                await process_single_manufacturer(
                    orchestrator=self.mfg_intake_orchestrator,
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
        return await self.finish_gpt_batch_processing(
            done_at=now,
            client=client,
            gpt_batch=gpt_batch,
            api_key_bundle=api_key_bundle,
            batch_download_output=batch_download_output,
        )

    async def poll_sync_and_upload_new_batches(
        self, poll_interval_seconds: int = 10 * 60
    ):
        logger.info(
            f"poll_sync_and_upload_new_batches: Starting batch upload loop (interval: {poll_interval_seconds}s)"
        )

        while True:
            try:
                now = get_current_time()
                api_key_bundles: list[APIKeyBundle] = await get_all_api_key_bundles()

                for api_key_bundle in api_key_bundles:
                    # if api_key_bundle.label in ["sudokn.tool2", "sudokn.tool3"]:
                    if api_key_bundle.label in ["sudokn.tool"]:
                        logger.info("temp skip")
                        continue

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

                    if api_key_bundle.has_active_batch():
                        # ask satellite to sync
                        await self.satellite.sync_batch(
                            client=client,
                            api_key_bundle=api_key_bundle,
                        )
                    else:
                        # api_key_bundle has no active batch
                        # api_key_bundle.latest_external_batch_id == None

                        # For added fault tolerance, check if there is a unrecorded batch already uploaded but not synced
                        # (FT-A):
                        #    because what if for some reason the call api_key_bundle.update_latest_external_batch_id below
                        #    failed at a previous iteration and a new batch was already created but maybe not processed
                        #    we would want to resume processing rather than generating a new batch
                        #    and uploading it with the key (which will probably be blocked lol)
                        latest_batch = find_latest_batch_of_api_key_bundle(
                            client=client, api_key_bundle=api_key_bundle
                        )
                        if latest_batch:
                            # the call api_key_bundle.update_latest_external_batch_id might have failed
                            await api_key_bundle.update_latest_external_batch_id(
                                updated_at=now, external_batch_id=latest_batch.id
                            )
                            await self.satellite.sync_batch(
                                client=client,
                                api_key_bundle=api_key_bundle,
                            )

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
                            )
                        )
                        jsonl_batch_file = batch_file_generation_result.batch_request_jsonl_file_writer.files[
                            0
                        ]
                        if not jsonl_batch_file.unique_ids:
                            logger.error(
                                f"poll_sync_and_upload_new_batches: Batch file generation created empty file"
                            )
                            continue

                        self.stats.batches_created += 1
                        new_gpt_batch = (
                            await self.satellite.try_uploading_new_batch_file(
                                client=client,
                                api_key_bundle=api_key_bundle,
                                jsonl_batch_file=jsonl_batch_file,
                            )
                        )
                        if not new_gpt_batch:
                            logger.info(
                                f"create_new_batches_and_upload: Upload failed for {api_key_bundle.label}"
                            )
                            continue

                        self.stats.batches_uploaded += 1
                        await pair_batch_request_custom_ids_with_batch(
                            custom_ids=batch_file_generation_result.batch_request_jsonl_file_writer.files[
                                0
                            ].unique_ids,
                            gpt_batch=new_gpt_batch,
                        )
                        # If the above call fails, we might send the same custom ids in the next batch
                        # pray pairing succeeds

                        await api_key_bundle.update_latest_external_batch_id(
                            updated_at=now,
                            external_batch_id=new_gpt_batch.external_batch_id,
                        )
                        # if the above call fails, the api_key would have been unknowingly used to upload a batch
                        # and will get picked again by find_inactive_api_keys(), then see FT-A

                        batch_file_generation_result.batch_request_jsonl_file_writer.delete_files()

                # for loop ends
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

    await init_db()
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
    try:
        await batch_file_station.start_loop(10)
    finally:
        # Clean up AWS clients
        await cleanup_data_etl_aws_clients()
        await cleanup_core_aws_clients()


def main():
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
