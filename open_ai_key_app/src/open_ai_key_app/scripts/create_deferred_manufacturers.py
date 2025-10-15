import time
import argparse
import asyncio
import logging
from datetime import datetime
from typing import Optional

from core.dependencies.load_core_env import load_core_env
from scraper_app.dependencies.load_scraper_env import load_scraper_env
from open_ai_key_app.dependencies.load_open_ai_app_env import load_open_ai_app_env
from data_etl_app.dependencies.load_data_etl_env import load_data_etl_env

# Load environment variables
load_core_env()
load_scraper_env()
load_data_etl_env()
load_open_ai_app_env()

from core.dependencies.aws_clients import (
    initialize_core_aws_clients,
    cleanup_core_aws_clients,
)
from data_etl_app.dependencies.aws_clients import (
    initialize_data_etl_aws_clients,
    cleanup_data_etl_aws_clients,
)
from data_etl_app.utils.chunk_util import shutdown_chunk_thread_pool

from core.utils.mongo_client import init_db
from core.models.db.manufacturer import Manufacturer
from open_ai_key_app.models.db.deferred_manufacturer import DeferredManufacturer

logger = logging.getLogger(__name__)

from core.utils.time_util import get_current_time
from core.services.manufacturer_service import find_manufacturer_by_etld1
from open_ai_key_app.services.deferred_manufacturer_service import (
    upsert_deferred_manufacturer,
)
from scraper_app.models.scraped_text_file import ScrapedTextFile


def classify_error(error: Optional[Exception]) -> str:
    """
    Classify error type for tracking.

    Returns:
        "s3_version" for S3 version errors
        "duplicate_key" for MongoDB duplicate key errors
        "other" for all other errors
    """
    if not error:
        return "other"

    error_str = str(error)

    if "NoSuchVersion" in error_str:
        return "s3_version"
    elif "duplicate key error" in error_str or "E11000" in error_str:
        return "duplicate_key"
    else:
        return "other"


async def process_single_manufacturer(
    deferred_at: datetime,
    mfg: Manufacturer,
    scraped_text_file: "ScrapedTextFile",
    existing_deferred_mfg: Optional["DeferredManufacturer"] = None,
) -> tuple[str, bool, bool, Optional[Exception]]:
    """
    Process a single manufacturer with pre-downloaded scraped text file.

    Args:
        deferred_at: Timestamp for deferred processing
        mfg: Manufacturer to process
        scraped_text_file: Pre-downloaded scraped text file from S3
        existing_deferred_mfg: Pre-fetched deferred manufacturer (for batch optimization)

    Returns:
        (etld1, success, updated, exception)
    """
    try:
        _deferred_manufacturer, updated = await upsert_deferred_manufacturer(
            timestamp=deferred_at,
            manufacturer=mfg,
            existing_deferred_manufacturer=existing_deferred_mfg,
            scraped_text_file=scraped_text_file,
        )
        logger.info(f"✅ Processed {mfg.etld1}: updated={updated}")
        return (mfg.etld1, True, updated, None)
    except Exception as e:
        logger.error(f"❌ Failed to process {mfg.etld1}: {e}", exc_info=True)
        return (mfg.etld1, False, False, e)


async def process_manufacturers_with_dynamic_concurrency(
    query_filter: dict,
    max_concurrent: int,
    batch_size: int,
    stats_interval: int,
    prefetch_threshold: int,
    limit: Optional[int],
):
    """
    Process manufacturers with dynamic concurrency using sliding window batching.

    Uses an aggressive preemptive batch fetching strategy:
    - Fetches manufacturers in large batches (default: 300)
    - Preemptively fetches next batch when 2/3 consumed (default: 200 remaining)
    - Triggers prefetch only once per batch to avoid spam
    - Starts with 2 batches prefetched to ensure queue stays full
    - Avoids long-lived MongoDB cursors that can timeout
    - Maintains high concurrency without overwhelming the database

    Args:
        query_filter: MongoDB query to filter manufacturers
        max_concurrent: Maximum number of manufacturers to process concurrently
        limit: Maximum number of manufacturers to process (optional, for testing)
        stats_interval: Number of manufacturers between statistics reports (default: 100)
        batch_size: Number of manufacturers to fetch per batch (default: 300)
        prefetch_threshold: Trigger next batch fetch when remaining drops to this (default: 200)
    """

    start_time = time.perf_counter()

    deferred_at = get_current_time()

    # Get total count using pymongo collection directly
    collection = Manufacturer.get_pymongo_collection()
    total_count = await collection.count_documents(query_filter)

    if limit:
        total_count = min(total_count, limit)

    logger.info(
        f"Starting dynamic concurrent processing: {total_count} manufacturers, "
        f"max_concurrent={max_concurrent}, batch_size={batch_size}, "
        f"prefetch_threshold={prefetch_threshold}"
    )

    # Semaphore to control concurrency
    semaphore = asyncio.Semaphore(max_concurrent)

    # Tracking variables
    processed = 0
    success_count = 0
    updated_count = 0  # Track manufacturers that were actually updated
    failure_count = 0
    s3_version_error_count = 0
    duplicate_key_error_count = 0
    other_error_count = 0
    failed_etld1s = []
    s3_version_error_etld1s = []
    duplicate_key_error_etld1s = []
    other_error_etld1s = []

    # Time tracking for statistics
    last_stats_count = 0
    last_stats_time = start_time

    # Lock for thread-safe counter updates
    stats_lock = asyncio.Lock()

    # Buffer for errors (write periodically, not per error)
    duplicate_key_error_buffer = []
    other_error_buffer = []

    # Open error and stats log file
    log_path = f"processing_log_{int(time.time())}.log"
    log_file = open(log_path, "w", encoding="utf-8")
    log_file.write(f"Processing started at {datetime.now()}\n")
    log_file.write(f"Max concurrent: {max_concurrent}\n")
    log_file.write(f"Stats interval: {stats_interval}\n")
    log_file.write("=" * 70 + "\n\n")
    log_file.flush()
    logger.info(f"Writing processing log to: {log_path}")

    async def process_with_semaphore(
        mfg: Manufacturer,
        scraped_text_file: ScrapedTextFile,
        existing_deferred_mfg: Optional[DeferredManufacturer],
    ):
        """Process a single manufacturer with semaphore control (S3 already downloaded)"""
        nonlocal processed, success_count, updated_count, failure_count
        nonlocal s3_version_error_count, duplicate_key_error_count, other_error_count
        nonlocal last_stats_count, last_stats_time

        async with semaphore:
            mfg_start_time = asyncio.get_event_loop().time()
            etld1, success, updated, error = await process_single_manufacturer(
                deferred_at, mfg, scraped_text_file, existing_deferred_mfg
            )
            elapsed = asyncio.get_event_loop().time() - mfg_start_time

            # Update stats in thread-safe manner
            async with stats_lock:
                processed += 1

                if success:
                    success_count += 1
                    if updated:
                        updated_count += 1
                else:
                    failure_count += 1
                    failed_etld1s.append(etld1)

                    # Classify error type
                    error_type = classify_error(error)

                    if error_type == "s3_version":
                        s3_version_error_count += 1
                        s3_version_error_etld1s.append(etld1)
                    elif error_type == "duplicate_key":
                        duplicate_key_error_count += 1
                        duplicate_key_error_etld1s.append(etld1)
                        # Buffer duplicate key errors separately (only log etld1)
                        duplicate_key_error_buffer.append(etld1)
                    else:
                        other_error_count += 1
                        other_error_etld1s.append(etld1)
                        # Buffer other errors with full message and type
                        error_details = f"{type(error).__name__}: {str(error)}"
                        other_error_buffer.append((etld1, error_details))

                # Log progress every 10 manufacturers or on failure
                if processed % 10 == 0 or not success:
                    logger.info(
                        f"Progress: {processed}/{total_count} "
                        f"(✅ {success_count} | 🔄 {updated_count} updated | ❌ {failure_count}: "
                        f"S3: {s3_version_error_count}, DupKey: {duplicate_key_error_count}, Other: {other_error_count}) "
                        f"[last: {etld1} in {elapsed:.1f}s]"
                    )

                # Log detailed statistics at intervals
                if processed % stats_interval == 0:
                    current_time = time.perf_counter()
                    batch_count = processed - last_stats_count
                    batch_time = current_time - last_stats_time
                    overall_elapsed = current_time - start_time

                    # Calculate ETA based on current throughput
                    remaining = total_count - processed
                    current_rate = processed / overall_elapsed  # mfg/s
                    eta_seconds = remaining / current_rate if current_rate > 0 else 0
                    eta_minutes = eta_seconds / 60
                    eta_hours = eta_minutes / 60

                    stats_text = (
                        f"\n{'='*70}\n"
                        f"📊 STATISTICS: Processed {processed}/{total_count} manufacturers\n"
                        f"{'='*70}\n"
                        f"Last {batch_count} batch:\n"
                        f"  - Total time: {batch_time:.2f}s\n"
                        f"  - Avg per mfg: {batch_time/batch_count:.2f}s\n"
                        f"  - Throughput: {batch_count/batch_time:.2f} mfg/s\n"
                        f"Overall (all {processed}):\n"
                        f"  - Total time: {overall_elapsed:.2f}s\n"
                        f"  - Avg per mfg: {overall_elapsed/processed:.2f}s\n"
                        f"  - Throughput: {processed/overall_elapsed:.2f} mfg/s\n"
                        f"Success rate: {success_count}/{processed} ({100*success_count/processed:.1f}%)\n"
                        f"Updated: {updated_count}/{success_count} ({100*updated_count/success_count:.1f}%)\n"
                        f"Estimated completion:\n"
                        f"  - Remaining: {remaining} manufacturers\n"
                        f"  - ETA: {eta_hours:.1f}h ({eta_minutes:.1f}m or {eta_seconds:.0f}s)\n"
                        f"{'='*70}\n"
                    )

                    logger.info(stats_text)

                    # Write stats and errors to file
                    log_file.write(f"\n{datetime.now()}\n")
                    log_file.write(stats_text)

                    # Write buffered duplicate key errors since last interval
                    if duplicate_key_error_buffer:
                        log_file.write(
                            f"\nDuplicate Key Errors since last interval ({len(duplicate_key_error_buffer)}):\n"
                        )
                        for etld1 in duplicate_key_error_buffer:
                            log_file.write(f"  - {etld1}\n")
                        duplicate_key_error_buffer.clear()  # Clear buffer after writing

                    # Write buffered other errors since last interval
                    if other_error_buffer:
                        log_file.write(
                            f"\nOther Errors since last interval ({len(other_error_buffer)}):\n"
                        )
                        for etld1, error_msg in other_error_buffer:
                            log_file.write(f"  - {etld1}:\n    {error_msg}\n")
                        other_error_buffer.clear()  # Clear buffer after writing

                    log_file.write("\n")
                    log_file.flush()

                    # Update for next interval
                    last_stats_count = processed
                    last_stats_time = current_time

            return etld1, success, error

    # Sliding window batch processing
    skip = 0
    manufacturers_queue = []  # Queue of manufacturers ready to process
    tasks = []  # Active processing tasks
    fetch_lock = asyncio.Lock()  # Prevent concurrent fetches
    is_fetching = False
    total_fetched = 0

    # Projection to only fetch needed fields (performance optimization)
    # projection = {
    #     "_id": 1,
    #     "etld1": 1,
    #     "scraped_text_file_version_id": 1,
    #     "is_manufacturer": 1,
    #     "is_contract_manufacturer": 1,
    #     "is_product_manufacturer": 1,
    #     "addresses": 1,
    #     "business_desc": 1,
    #     "products": 1,
    #     "certificates": 1,
    #     "industries": 1,
    #     "process_caps": 1,
    #     "material_caps": 1,
    # }

    async def fetch_next_batch():
        """Fetch the next batch of manufacturers from MongoDB"""
        nonlocal skip, is_fetching, total_fetched

        async with fetch_lock:
            if is_fetching:
                return  # Already fetching
            is_fetching = True

        try:
            # Calculate how many to fetch
            remaining = total_count - total_fetched
            current_batch_size = min(batch_size, remaining)

            if current_batch_size <= 0:
                logger.info("No more manufacturers to fetch")
                return

            logger.info(
                f"Fetching batch: skip={skip}, limit={current_batch_size} "
                f"(fetched: {total_fetched}/{total_count})"
            )

            # Fetch batch with projection (only needed fields)
            # Sort by scraped_text_file_num_tokens ascending (smallest first)
            batch_docs = (
                await collection.find(query_filter)
                .sort("scraped_text_file_num_tokens", 1)
                .skip(skip)
                .limit(current_batch_size)
                .to_list(length=current_batch_size)
            )

            if not batch_docs:
                logger.info("No documents returned from MongoDB")
                return

            logger.info(f"Fetched {len(batch_docs)} manufacturers from MongoDB")

            # Convert dicts to Manufacturer objects
            batch_manufacturers = [Manufacturer(**doc) for doc in batch_docs]

            # Batch fetch existing deferred manufacturers for this batch
            # Only fetch those with at least one field missing (same as manufacturer filter)
            batch_keys = [
                (m.etld1, m.scraped_text_file_version_id) for m in batch_manufacturers
            ]
            existing_deferred_mfgs = await DeferredManufacturer.find(
                {
                    "$and": [
                        {
                            "$or": [
                                {
                                    "mfg_etld1": etld1,
                                    "scraped_text_file_version_id": version_id,
                                }
                                for etld1, version_id in batch_keys
                            ]
                        },
                        # {
                        #     "$or": [
                        #         # Missing classification fields
                        #         {"is_manufacturer": None},
                        #         {"is_contract_manufacturer": None},
                        #         {"is_product_manufacturer": None},
                        #         # Missing extraction fields
                        #         {"addresses": None},
                        #         {"business_desc": None},
                        #         {"products": None},
                        #         {"certificates": None},
                        #         {"industries": None},
                        #         {"process_caps": None},
                        #         {"material_caps": None},
                        #     ]
                        # },
                    ]
                }
            ).to_list(length=None)

            # Convert dicts to DeferredManufacturer objects
            # existing_deferred_mfgs = [
            #     DeferredManufacturer(**doc) for doc in existing_deferred_docs
            # ]

            # Create lookup map
            deferred_mfg_map = {
                (dm.mfg_etld1, dm.scraped_text_file_version_id): dm
                for dm in existing_deferred_mfgs
            }

            logger.info(
                f"Fetched {len(existing_deferred_mfgs)} existing deferred "
                f"manufacturers for batch"
            )

            # Download S3 files in parallel for this batch (outside semaphore!)
            logger.info(
                f"Downloading S3 files for {len(batch_manufacturers)} manufacturers in parallel..."
            )
            s3_download_tasks = [
                ScrapedTextFile.download_from_s3_and_create(
                    m.etld1, m.scraped_text_file_version_id
                )
                for m in batch_manufacturers
            ]
            s3_results = await asyncio.gather(
                *s3_download_tasks, return_exceptions=True
            )

            # Add to queue only manufacturers where S3 download succeeded
            s3_success_count = 0
            s3_failure_count = 0
            for mfg, result in zip(batch_manufacturers, s3_results):
                # Handle both cases: exception object OR (scraped_file, exception) tuple
                if isinstance(result, Exception):
                    # Unexpected exception raised during S3 download (shouldn't happen but handle it)
                    s3_failure_count += 1
                    error_type = classify_error(result)
                    error_details = f"{type(result).__name__}: {str(result)}"
                    logger.warning(
                        f"S3 download raised exception for {mfg.etld1} (type: {error_type}): {error_details}"
                    )
                    async with stats_lock:
                        nonlocal processed, failure_count, s3_version_error_count, s3_version_error_etld1s
                        nonlocal failed_etld1s, other_error_count, other_error_etld1s
                        processed += 1
                        failure_count += 1
                        failed_etld1s.append(mfg.etld1)
                        error_type = classify_error(result)
                        if error_type == "s3_version":
                            s3_version_error_count += 1
                            s3_version_error_etld1s.append(mfg.etld1)
                        else:
                            other_error_count += 1
                            other_error_etld1s.append(mfg.etld1)
                elif isinstance(result, tuple) and len(result) == 2:
                    # Normal case: (scraped_file, exception) tuple
                    scraped_file, exception = result
                    if exception or not scraped_file:
                        # S3 download failed - track as error but don't queue
                        s3_failure_count += 1
                        error_type = classify_error(exception)
                        error_details = (
                            f"{type(exception).__name__}: {str(exception)}"
                            if exception
                            else "No scraped file returned"
                        )
                        logger.warning(
                            f"S3 download failed for {mfg.etld1} (type: {error_type}): {error_details}"
                        )
                        # Process as failure immediately (not queued for processing)
                        async with stats_lock:
                            processed += 1
                            failure_count += 1
                            failed_etld1s.append(mfg.etld1)
                            error_type = classify_error(exception)
                            if error_type == "s3_version":
                                s3_version_error_count += 1
                                s3_version_error_etld1s.append(mfg.etld1)
                            else:
                                other_error_count += 1
                                other_error_etld1s.append(mfg.etld1)
                    else:
                        # S3 download succeeded - add to queue
                        s3_success_count += 1
                        existing = deferred_mfg_map.get(
                            (mfg.etld1, mfg.scraped_text_file_version_id)
                        )
                        manufacturers_queue.append((mfg, scraped_file, existing))
                else:
                    # Unexpected result format
                    s3_failure_count += 1
                    logger.error(
                        f"Unexpected S3 download result format for {mfg.etld1}: {type(result)}"
                    )
                    async with stats_lock:
                        processed += 1
                        failure_count += 1
                        failed_etld1s.append(mfg.etld1)
                        other_error_count += 1
                        other_error_etld1s.append(mfg.etld1)

            logger.info(
                f"S3 downloads: {s3_success_count} succeeded, {s3_failure_count} failed. "
                f"{s3_success_count} manufacturers queued for processing."
            )

            # Update counters
            skip += current_batch_size
            total_fetched += len(batch_manufacturers)

            logger.info(
                f"Queue size now: {len(manufacturers_queue)} manufacturers "
                f"({total_fetched}/{total_count} total fetched)"
            )
        finally:
            async with fetch_lock:
                is_fetching = False
            # Reset prefetch trigger flag when new batch arrives
            nonlocal prefetch_triggered
            prefetch_triggered = False

    # Fetch initial batch
    await fetch_next_batch()

    # Immediately start prefetching second batch
    if total_fetched < total_count:
        asyncio.create_task(fetch_next_batch())

    # Track if we've triggered prefetch for current batch (avoid spam)
    prefetch_triggered = False

    # Process manufacturers from the queue
    while total_fetched < total_count or manufacturers_queue:
        # Wait if queue is empty but more to fetch
        if not manufacturers_queue:
            if total_fetched < total_count:
                logger.info("Queue empty, waiting for fetch to complete...")
                await asyncio.sleep(0.1)
                continue
            else:
                # No more to fetch and queue is empty
                break

        # Pop from queue and create task (queue now contains: mfg, scraped_file, existing)
        mfg, scraped_file, existing = manufacturers_queue.pop(0)
        task = asyncio.create_task(process_with_semaphore(mfg, scraped_file, existing))
        tasks.append(task)

        # Check if we should trigger prefetch AFTER popping (only once per batch)
        if (
            not prefetch_triggered
            and len(manufacturers_queue) <= prefetch_threshold
            and total_fetched < total_count
            and not is_fetching
        ):
            logger.info(
                f"Queue low ({len(manufacturers_queue)} remaining), "
                f"triggering prefetch..."
            )
            asyncio.create_task(fetch_next_batch())
            prefetch_triggered = True

        # No delay - let tasks run as fast as semaphore allows

    # Wait for all remaining tasks to complete
    if tasks:
        logger.info(f"Waiting for {len(tasks)} remaining tasks to complete...")
        await asyncio.gather(*tasks, return_exceptions=True)

    # Write any remaining buffered errors
    if duplicate_key_error_buffer or other_error_buffer:
        log_file.write(f"\n{datetime.now()}\n")

        if duplicate_key_error_buffer:
            log_file.write(
                f"Final Duplicate Key Errors ({len(duplicate_key_error_buffer)}):\n"
            )
            for etld1 in duplicate_key_error_buffer:
                log_file.write(f"  - {etld1}\n")
            log_file.write("\n")

        if other_error_buffer:
            log_file.write(f"Final Other Errors ({len(other_error_buffer)}):\n")
            for etld1, error_msg in other_error_buffer:
                log_file.write(f"  - {etld1}:\n    {error_msg}\n")
            log_file.write("\n")

    # Calculate timing metrics
    total_time = time.perf_counter() - start_time
    avg_time_per_mfg = total_time / processed if processed > 0 else 0

    logger.info(
        f"✅ Dynamic concurrent processing complete: {processed} total "
        f"(✅ {success_count} success, 🔄 {updated_count} updated | ❌ {failure_count} failed) "
        f"in {total_time:.2f}s (avg: {avg_time_per_mfg:.2f}s/mfg)"
    )

    # Log error breakdown
    if failure_count > 0:
        logger.info(
            f"Error breakdown: S3 version errors: {s3_version_error_count}, "
            f"Duplicate key errors: {duplicate_key_error_count}, "
            f"Other errors: {other_error_count}"
        )

    # Log S3 version errors separately
    if s3_version_error_etld1s:
        logger.warning(
            f"❌ S3 Version Errors ({len(s3_version_error_etld1s)}): "
            f"{s3_version_error_etld1s[:10]}"
        )
        if len(s3_version_error_etld1s) > 10:
            logger.warning(f"... and {len(s3_version_error_etld1s) - 10} more")

    # Log duplicate key errors separately
    if duplicate_key_error_etld1s:
        logger.warning(
            f"❌ Duplicate Key Errors ({len(duplicate_key_error_etld1s)}): "
            f"{duplicate_key_error_etld1s[:10]}"
        )
        if len(duplicate_key_error_etld1s) > 10:
            logger.warning(f"... and {len(duplicate_key_error_etld1s) - 10} more")

    # Log other errors separately
    if other_error_etld1s:
        logger.warning(
            f"❌ Other Errors ({len(other_error_etld1s)}): {other_error_etld1s[:10]}"
        )
        if len(other_error_etld1s) > 10:
            logger.warning(f"... and {len(other_error_etld1s) - 10} more")

        # Log a sample of error types for debugging
        if other_error_buffer:
            logger.warning("Sample of other error types:")
            for etld1, error_msg in other_error_buffer[:5]:
                logger.warning(f"  - {etld1}: {error_msg}")

    # Write final summary to log file
    log_file.write(f"\n{'='*70}\n")
    log_file.write(f"FINAL SUMMARY\n")
    log_file.write(f"{'='*70}\n")
    log_file.write(f"Processing completed at: {datetime.now()}\n")
    log_file.write(f"Total processed: {processed}/{total_count}\n")
    log_file.write(
        f"Success: {success_count} ({100*success_count/processed:.1f}%)\n"
        if processed > 0
        else "Success: 0\n"
    )
    log_file.write(
        f"Updated: {updated_count} ({100*updated_count/success_count:.1f}% of successful)\n"
        if success_count > 0
        else "Updated: 0\n"
    )
    log_file.write(f"Failed: {failure_count}\n")
    log_file.write(f"  - S3 version errors: {s3_version_error_count}\n")
    log_file.write(f"  - Duplicate key errors: {duplicate_key_error_count}\n")
    log_file.write(f"  - Other errors: {other_error_count}\n")
    log_file.write(f"Total time: {total_time:.2f}s\n")
    log_file.write(f"Avg per manufacturer: {avg_time_per_mfg:.2f}s\n")
    log_file.write(f"{'='*70}\n")
    log_file.close()
    logger.info(f"Processing log saved to: {log_path}")

    return {
        "total": processed,
        "success": success_count,
        "updated": updated_count,
        "failed": failure_count,
        "s3_version_errors": s3_version_error_count,
        "duplicate_key_errors": duplicate_key_error_count,
        "other_errors": other_error_count,
        "failed_etld1s": failed_etld1s,
        "s3_version_error_etld1s": s3_version_error_etld1s,
        "duplicate_key_error_etld1s": duplicate_key_error_etld1s,
        "other_error_etld1s": other_error_etld1s,
        "total_time_seconds": total_time,
        "avg_time_per_manufacturer_seconds": avg_time_per_mfg,
    }


async def process_single_manufacturer_by_etld1(etld1: str):
    """Process a single manufacturer by etld1 (for testing/debugging)"""
    mfg = await find_manufacturer_by_etld1(mfg_etld1=etld1)
    if not mfg:
        logger.error(f"Manufacturer not found: {etld1}")
        return

    logger.info(f"Found manufacturer: {mfg.etld1}")
    deferred_at = get_current_time()

    # Download S3 file first
    scraped_file, exception = await ScrapedTextFile.download_from_s3_and_create(
        mfg.etld1, mfg.scraped_text_file_version_id
    )

    if exception or not scraped_file:
        logger.error(f"❌ Failed to download S3 file for {mfg.etld1}: {exception}")
        return

    etld1_result, success, updated, error = await process_single_manufacturer(
        deferred_at, mfg, scraped_file
    )

    if success:
        logger.info(f"✅ Successfully processed {etld1_result} (updated={updated})")
    else:
        logger.error(f"❌ Failed to process {etld1_result}: {error}")


async def async_main():
    """Main entry point with different processing modes"""
    parser = argparse.ArgumentParser(
        description="Create deferred manufacturers for GPT batch processing"
    )
    parser.add_argument(
        "--mode",
        choices=["single", "all"],
        default=None,  # Will be auto-determined
        help="Processing mode: single manufacturer or all manufacturers (auto-detected if --etld1 provided)",
    )
    parser.add_argument(
        "--etld1",
        type=str,
        help="Manufacturer etld1 (for single mode). If provided, automatically switches to single mode.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Maximum number of manufacturers to process (for testing)",
    )
    parser.add_argument(
        "--stats-interval",
        type=int,
        default=100,
        help="Log detailed statistics every N manufacturers (default: 100)",
    )
    parser.add_argument(
        "--yes",
        "-y",
        action="store_true",
        help="Skip confirmation prompt and proceed automatically",
    )

    args = parser.parse_args()

    # Auto-detect mode based on --etld1 flag
    if args.mode is None:
        if args.etld1:
            args.mode = "single"
            logger.info(f"Auto-detected mode: single (--etld1={args.etld1})")
        else:
            args.mode = "all"
            logger.info("Auto-detected mode: all (no --etld1 provided)")

    # ========================================================================
    # CONCURRENCY AND POOL SIZING
    # ========================================================================
    # MongoDB connection pool sizing for 8GB EC2 instance:
    # - Each connection: ~5 MB RAM average
    # - 120 connections: ~600 MB (safe for 8GB instance)
    # - Server can handle 500-1000 connections before performance degrades
    #
    # Key insight: Connections are borrowed ONLY during DB operations, not held
    # by Pydantic instances. Each task's timeline:
    #   1. Borrow connection → Query manufacturer → Return (~10-50ms)
    #   2. [If update needed]:
    #      a. Fetch S3 (1-3s) - NO MongoDB connection used
    #      b. Chunking (0.5-2s) - NO MongoDB connection used (runs in thread pool)
    #      c. LLM requests (0.5-1s) - NO MongoDB connection used (runs in thread pool)
    #      d. Borrow connection → Write results → Return (~100-500ms)
    #
    # Configuration (Ultra-Conservative):
    #   max_pool_size = 120 (safe for 8GB EC2, ~600 MB RAM)
    #   max_concurrent = 30 (leaves 75% buffer: 90 connections)
    #
    # This 4:1 ratio (120 pool : 30 concurrent) provides:
    #   - 30 connections for concurrent task queries/writes
    #   - 90 connections buffer for batch fetching, overlap, and background ops
    #   - Massive headroom even when all 30 tasks need updates simultaneously
    #   - Eliminates timeout risk with huge safety margin
    #
    # Note: Reduced from 50 to 30 due to timeouts observed at batch 2500+
    # when manufacturers started needing updates (S3 fetch + chunking hold time)
    # ========================================================================

    MAX_POOL_SIZE = 120  # Conservative for 8GB EC2 (~600 MB RAM)
    MAX_CONCURRENT = 20  # Ultra-conservative: leaves 75% buffer (90 connections)

    max_concurrent = MAX_CONCURRENT
    min_pool_size = max(20, MAX_POOL_SIZE // 4)  # 25% of max, minimum 20

    logger.info("=" * 70)
    logger.info("CONCURRENCY AND POOL CONFIGURATION (ULTRA-CONSERVATIVE)")
    logger.info("=" * 70)
    logger.info(f"MongoDB Pool Configuration (8GB EC2):")
    logger.info(f"  - Max pool size: {MAX_POOL_SIZE} (~600 MB RAM)")
    logger.info(f"  - Min pool size: {min_pool_size}")
    logger.info(f"Concurrency Configuration:")
    logger.info(f"  - Max concurrent manufacturers: {max_concurrent}")
    logger.info(f"  - Pool to concurrency ratio: 4:1 (75% buffer = 90 connections)")
    logger.info(f"Rationale:")
    logger.info(f"  - Pool size {MAX_POOL_SIZE} is conservative for 8GB EC2")
    logger.info(f"  - Each connection uses ~5 MB RAM")
    logger.info(f"  - Connections borrowed ONLY during DB operations (queries/writes)")
    logger.info(f"  - Pydantic instances don't hold connections")
    logger.info(f"  - 75% buffer (90 connections) eliminates timeout risk")
    logger.info(f"  - During updates: S3/chunking uses semaphore but NOT MongoDB")
    logger.info(f"  - Reduced from 50 to 30 due to timeouts when updates started")
    logger.info("=" * 70)

    logger.info(
        f"Initializing MongoDB with maxPoolSize={MAX_POOL_SIZE}, minPoolSize={min_pool_size}, "
        f"max_concurrent={max_concurrent}"
    )

    await init_db(
        max_pool_size=MAX_POOL_SIZE,
        min_pool_size=min_pool_size,
        max_idle_time_ms=60000,  # 60s - keep connections alive longer
        server_selection_timeout_ms=30000,  # 30s - more time to find server
        connect_timeout_ms=30000,  # 30s - more time to connect
        socket_timeout_ms=120000,  # 120s - more time for operations
    )

    # Initialize AWS clients
    await initialize_core_aws_clients()
    await initialize_data_etl_aws_clients()

    try:
        if args.mode == "single":
            # Process single manufacturer
            logger.info(f"Processing single manufacturer: {args.etld1}")
            await process_single_manufacturer_by_etld1(args.etld1)

        elif args.mode == "all":
            # Process manufacturers that actually need processing:
            # 1. Have scraped text file (scraped_text_file_version_id exists)
            # 2. Token count < 1,000,000
            # 3. Missing at least one of the extracted fields
            # NOTE: We check for fields that are NOT yet populated (None/null)
            # to avoid reprocessing manufacturers that already have all fields
            query_filter = {
                "scraped_text_file_version_id": {"$exists": True},
                "scraped_text_file_num_tokens": {"$lt": 1000000},
                # "$or": [
                #     # Missing classification fields
                #     {"is_manufacturer": None},
                #     {"is_contract_manufacturer": None},
                #     {"is_product_manufacturer": None},
                #     # Missing extraction fields
                #     {"addresses": None},
                #     {"business_desc": None},
                #     {"products": None},
                #     {"certificates": None},
                #     {"industries": None},
                #     {"process_caps": None},
                #     {"material_caps": None},
                # ],
            }

            # Get count of manufacturers matching the filter using pymongo collection directly
            collection = Manufacturer.get_pymongo_collection()
            matching_count = await collection.count_documents(query_filter)

            # Show filter details and count
            print("\n" + "=" * 70)
            print("MANUFACTURER PROCESSING CONFIGURATION")
            print("=" * 70)
            print(f"Filter:")
            print(f"  - scraped_text_file_num_tokens < 1,000,000")
            print(f"  - Missing at least one field (classification/extraction)")
            print(f"Max concurrent: {max_concurrent} (auto-calculated)")
            print(f"MongoDB pool size: {MAX_POOL_SIZE}")
            print(f"Total manufacturers needing processing: {matching_count}")
            if args.limit:
                print(f"Limit: {args.limit} (testing mode)")
                print(f"Will process: {min(args.limit, matching_count)} manufacturers")
            else:
                print(f"Will process: {matching_count} manufacturers (ALL matching)")
            print("=" * 70)

            # Ask for confirmation (unless --yes flag is set)
            if not args.yes:
                response = input("\nDo you want to proceed? (yes/no): ").strip().lower()

                if response not in ["yes", "y"]:
                    logger.info("Processing cancelled by user")
                    print("Processing cancelled.")
                    return
            else:
                logger.info("Auto-proceeding with --yes flag")

            print("\nStarting processing...\n")
            logger.info(f"Processing {matching_count} manufacturers")

            # Process with dynamic concurrency and sliding window batching
            results = await process_manufacturers_with_dynamic_concurrency(
                query_filter=query_filter,
                max_concurrent=max_concurrent,  # Use auto-calculated value
                limit=args.limit,
                stats_interval=args.stats_interval,
                batch_size=50,  # Fetch 50 manufacturers at a time (smaller buffer)
                prefetch_threshold=49,  # Start fetching next batch when 49 remain (2/3 consumed)
            )
            logger.info(f"Final results: {results}")

    finally:
        # Clean up chunking thread pool
        shutdown_chunk_thread_pool(wait=True)
        logger.info("Chunking thread pool shut down")

        # Clean up AWS clients
        await cleanup_data_etl_aws_clients()
        await cleanup_core_aws_clients()


def main():
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
