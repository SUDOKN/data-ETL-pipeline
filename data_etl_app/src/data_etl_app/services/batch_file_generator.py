import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional


from core.models.base_files import CSVFile
from core.models.db.deferred_manufacturer import DeferredManufacturer
from core.services.manufacturer_service import (
    find_manufacturer_by_etld1,
)
from core.services.gpt_batch_request_service import (
    find_gpt_batch_requests_by_custom_ids,
)
from core.services.deferred_manufacturer_service import (
    get_deferred_manufacturer_by_etld1_scraped_file_version,
    get_embedded_gpt_request_ids,
)
from core.utils.batch_jsonl_file_writer import (
    BatchRequestJSONLFileWriter,
    MaxFilesReachedException,
)
from core.utils.time_util import get_timestamp_str
from data_etl_app.services.manufacturer_extraction_orchestrator import (
    ManufacturerExtractionOrchestrator,
)

logger = logging.getLogger(__name__)


async def _process_single_deferred_manufacturer(
    df_mfg_doc: dict,
    timestamp: datetime,
    mfg_orchestrator: ManufacturerExtractionOrchestrator,
    batch_request_jsonl_file_writer: BatchRequestJSONLFileWriter,
    df_mfgs_with_orphan_custom_ids_file: CSVFile,
    use_async_write: bool = False,
) -> int:
    """
    Process a single deferred manufacturer and write its batch requests.

    Returns:
        Number of pending requests written (0 or 1 to indicate if manufacturer had pending requests)
    """
    logger.debug(f"Processing DeferredManufacturer {df_mfg_doc['mfg_etld1']}")
    deferred_mfg = DeferredManufacturer(**df_mfg_doc)
    custom_ids = get_embedded_gpt_request_ids(deferred_mfg)
    logger.debug(
        f"Found {len(custom_ids):,} embedded GPTBatchRequest IDs in DeferredManufacturer {deferred_mfg.mfg_etld1}"
    )
    if not custom_ids:
        logger.warning(
            f"DeferredManufacturer {deferred_mfg.mfg_etld1} has no embedded GPT request IDs; skipping."
        )
        return 0
    all_requests = await find_gpt_batch_requests_by_custom_ids(list(custom_ids))

    found_custom_ids: set[str] = set(all_requests.keys())
    missing_custom_ids = list(custom_ids - found_custom_ids)
    if missing_custom_ids:
        logger.warning(
            f"DeferredManufacturer {deferred_mfg.mfg_etld1} has "
            f"{len(missing_custom_ids):,} orphan GPTBatchRequests,\n"
            f"missing_custom_ids={missing_custom_ids}"
        )
        try:
            mfg = await find_manufacturer_by_etld1(deferred_mfg.mfg_etld1)
            assert (
                mfg is not None
            ), f"Manufacturer not found for {deferred_mfg.mfg_etld1}"
            await mfg_orchestrator.process_manufacturer(
                timestamp=timestamp,
                mfg=mfg,
            )
        except Exception as e:
            logger.error(
                f"Error processing Manufacturer {deferred_mfg.mfg_etld1} to handle orphan GPTBatchRequests: {e}",
                exc_info=True,
            )
            return 0

        deferred_mfg = await get_deferred_manufacturer_by_etld1_scraped_file_version(
            mfg_etld1=deferred_mfg.mfg_etld1,
            scraped_text_file_version_id=deferred_mfg.scraped_text_file_version_id,
        )

        # If it was deleted, skip the 2nd pass check
        if deferred_mfg is None:
            logger.info(
                f"DeferredManufacturer {df_mfg_doc['mfg_etld1']} was deleted after processing; skipping 2nd pass check"
            )
            return 0

    custom_ids = get_embedded_gpt_request_ids(deferred_mfg)
    logger.debug(
        f"Found {len(custom_ids):,} embedded GPTBatchRequest IDs in DeferredManufacturer {deferred_mfg.mfg_etld1} in 2nd pass"
    )
    if not custom_ids:
        logger.warning(
            f"DeferredManufacturer {deferred_mfg.mfg_etld1} has no embedded GPT request IDs in 2nd pass; skipping."
        )
        return 0

    all_requests = await find_gpt_batch_requests_by_custom_ids(list(custom_ids))

    found_custom_ids: set[str] = set(all_requests.keys())
    missing_custom_ids = list(custom_ids - found_custom_ids)
    if missing_custom_ids:
        logger.warning(
            f"DeferredManufacturer {deferred_mfg.mfg_etld1} still has "
            f"{len(missing_custom_ids):,} orphan GPTBatchRequests in 2nd pass!!"
            f"missing_custom_ids={missing_custom_ids}"
        )
        df_mfgs_with_orphan_custom_ids_file.add_csv_row(
            [deferred_mfg.mfg_etld1, str(len(missing_custom_ids))]
        )

    pending_request_blobs = [
        req.request for req in all_requests.values() if req.is_batch_request_pending()
    ]
    if not pending_request_blobs:
        logger.debug(
            f"No pending GPTBatchRequests for DeferredManufacturer {deferred_mfg.mfg_etld1}; skipping."
        )
        return 0

    logger.debug(
        f"DeferredManufacturer {deferred_mfg.mfg_etld1}: "
        f"{len(pending_request_blobs):,} pending GPTBatchRequests to write"
    )

    if use_async_write:
        await batch_request_jsonl_file_writer.write_item_request_blobs_async(
            item_id=deferred_mfg.mfg_etld1, request_blobs=pending_request_blobs
        )
    else:
        batch_request_jsonl_file_writer.write_item_request_blobs(
            item_id=deferred_mfg.mfg_etld1, request_blobs=pending_request_blobs
        )

    return 1


@dataclass
class BatchFileGenerationResult:
    batch_request_jsonl_file_writer: BatchRequestJSONLFileWriter
    df_mfgs_with_orphan_custom_ids_file: CSVFile

    @property
    def final_summary(self) -> dict:
        return {
            **self.batch_request_jsonl_file_writer.result_summary,
            "df_mfgs_with_orphan_custom_ids_file": {
                "file_path": str(self.df_mfgs_with_orphan_custom_ids_file.full_path),
                "total_mfgs": self.df_mfgs_with_orphan_custom_ids_file.total_rows
                - 1,  # exclude header
            },
        }


async def iterate_df_manufacturers_and_write_batch_files(
    timestamp: datetime,
    query_filter: dict,
    output_dir: Path,
    max_requests_per_file: int,
    max_tokens_per_file: int,
    max_file_size_in_bytes: int,
    max_files: Optional[int],
    max_manufacturers: Optional[int] = None,
    parallel_processing: bool = False,
    max_concurrent_manufacturers: int = 50,
) -> BatchFileGenerationResult:
    # result = BatchFileGenerationResult(
    #     files=[], df_mfgs_with_orphan_custom_ids=CSVFile(output_dir=output_dir, )
    # )
    mfg_orchestrator = ManufacturerExtractionOrchestrator()

    batch_request_jsonl_file_writer = BatchRequestJSONLFileWriter(
        output_dir=output_dir,
        run_timestamp=timestamp,
        max_files=max_files,
        max_requests_per_file=max_requests_per_file,
        max_tokens_per_file=max_tokens_per_file,
        max_file_size_in_bytes=max_file_size_in_bytes,
    )
    df_mfgs_with_orphan_custom_ids_file = CSVFile(
        output_dir=batch_request_jsonl_file_writer.output_dir,
        prefix="df_mfgs_with_orphan_custom_ids",
        timestamp_str=get_timestamp_str(timestamp),
        headers=["mfg_etld1", "orphan_custom_ids_count"],
    )

    try:
        logger.info(
            f"Starting iteration over DeferredManufacturers "
            f"(parallel_processing={parallel_processing}, max_concurrent={max_concurrent_manufacturers})"
        )
        count = 0

        if parallel_processing:
            # Parallel processing mode with batched task execution
            semaphore = asyncio.Semaphore(max_concurrent_manufacturers)
            batch_size = max_concurrent_manufacturers  # Process in batches
            current_batch = []
            total_processed = 0
            max_files_reached = False

            async def process_with_semaphore(df_mfg_doc: dict):
                async with semaphore:
                    return await _process_single_deferred_manufacturer(
                        df_mfg_doc=df_mfg_doc,
                        timestamp=timestamp,
                        mfg_orchestrator=mfg_orchestrator,
                        batch_request_jsonl_file_writer=batch_request_jsonl_file_writer,
                        df_mfgs_with_orphan_custom_ids_file=df_mfgs_with_orphan_custom_ids_file,
                        use_async_write=True,  # Use async write for thread safety
                    )

            async with (
                DeferredManufacturer.get_pymongo_collection()
                .find(query_filter)
                .sort("scraped_text_file_num_tokens", 1)  # 1 = ascending
                .sort("updated_at", -1)  # -1 = newest first
            ) as cursor:
                async for df_mfg_doc in cursor:
                    task = asyncio.create_task(process_with_semaphore(df_mfg_doc))
                    current_batch.append(task)

                    # When batch is full, process it and check for MaxFilesReachedException
                    if len(current_batch) >= batch_size:
                        logger.info(
                            f"Processing batch of {len(current_batch)} manufacturers..."
                        )
                        results = await asyncio.gather(
                            *current_batch, return_exceptions=True
                        )

                        # Check results for MaxFilesReachedException
                        for i, result in enumerate(results):
                            if isinstance(result, MaxFilesReachedException):
                                logger.info(
                                    f"MaxFilesReachedException in batch at task {i}: {result}"
                                )
                                max_files_reached = True
                                break
                            elif isinstance(result, Exception):
                                logger.error(
                                    f"Error processing manufacturer {total_processed + i}: {result}",
                                    exc_info=result,
                                )
                            elif isinstance(result, int):
                                count += result  # result is 0 or 1

                        total_processed += len(current_batch)
                        current_batch = []

                        # Stop if max files reached
                        if max_files_reached:
                            logger.info("Max files reached, stopping cursor iteration")
                            break

                    # Check if we've hit max_manufacturers limit
                    if (
                        max_manufacturers
                        and (total_processed + len(current_batch)) >= max_manufacturers
                    ):
                        logger.info(
                            f"Reached limit of {max_manufacturers:,} DeferredManufacturers; stopping collection."
                        )
                        break

            # Process any remaining tasks in the final batch
            if current_batch and not max_files_reached:
                logger.info(
                    f"Processing final batch of {len(current_batch)} manufacturers..."
                )
                results = await asyncio.gather(*current_batch, return_exceptions=True)

                for i, result in enumerate(results):
                    if isinstance(result, MaxFilesReachedException):
                        logger.info(
                            f"MaxFilesReachedException in final batch at task {i}: {result}"
                        )
                        max_files_reached = True
                        break
                    elif isinstance(result, Exception):
                        logger.error(
                            f"Error processing manufacturer {total_processed + i}: {result}",
                            exc_info=result,
                        )
                    elif isinstance(result, int):
                        count += result

            if max_files_reached:
                raise MaxFilesReachedException(
                    "Maximum number of files reached during parallel processing"
                )

            logger.info(
                f"Parallel processing complete: {count:,} manufacturers with pending requests"
            )

        else:
            # Sequential processing mode (original implementation)
            async with (
                DeferredManufacturer.get_pymongo_collection()
                .find(query_filter)
                .sort("scraped_text_file_num_tokens", 1)  # 1 = ascending
                .sort("updated_at", 1)  # 1 = oldest first
            ) as cursor:
                async for df_mfg_doc in cursor:
                    try:
                        result = await _process_single_deferred_manufacturer(
                            df_mfg_doc=df_mfg_doc,
                            timestamp=timestamp,
                            mfg_orchestrator=mfg_orchestrator,
                            batch_request_jsonl_file_writer=batch_request_jsonl_file_writer,
                            df_mfgs_with_orphan_custom_ids_file=df_mfgs_with_orphan_custom_ids_file,
                            use_async_write=False,  # Sync write is fine for sequential
                        )
                        count += result

                        if count % 100 == 0:
                            logger.info(
                                f"Wrote {count:,} manufacturers with pending requests so far..."
                            )

                        if max_manufacturers and count >= max_manufacturers:
                            logger.info(
                                f"Reached limit of {max_manufacturers:,} DeferredManufacturers to process; stopping."
                            )
                            break
                    except MaxFilesReachedException:
                        # Re-raise to be caught by outer try-except
                        raise
    except MaxFilesReachedException as e:
        logger.info(f"Stopping: {e}")
    except Exception as e:
        logger.error(f"Error during batch file generation: {e}")
        raise e
    finally:
        batch_request_jsonl_file_writer.current_file.close_pointer()  # all other prev files closed by writer
        df_mfgs_with_orphan_custom_ids_file.close_pointer()
        return BatchFileGenerationResult(
            batch_request_jsonl_file_writer=batch_request_jsonl_file_writer,
            df_mfgs_with_orphan_custom_ids_file=df_mfgs_with_orphan_custom_ids_file,
        )
