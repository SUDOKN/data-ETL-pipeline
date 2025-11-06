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
from core.models.db.deferred_manufacturer import DeferredManufacturer

logger = logging.getLogger(__name__)

from core.utils.time_util import get_current_time
from core.services.deferred_manufacturer_service import (
    get_deferred_manufacturer_by_etld1_scraped_file_version,
    upsert_deferred_manufacturer_phase2,
)
from core.services.manufacturer_service import find_manufacturer_by_etld1


def classify_error(error: Optional[Exception]) -> str:
    """
    Classify error type for tracking.

    Returns:
        "incomplete_phase1" for phase 1 incomplete errors
        "duplicate_key" for MongoDB duplicate key errors
        "other" for all other errors
    """
    if not error:
        return "other"

    error_str = str(error)

    if "phase 1 incomplete" in error_str or "phase 1 partially incomplete" in error_str:
        return "incomplete_phase1"
    elif "duplicate key error" in error_str or "E11000" in error_str:
        return "duplicate_key"
    else:
        return "other"


async def process_single_deferred_manufacturer(
    deferred_at: datetime,
    deferred_mfg: DeferredManufacturer,
) -> tuple[str, bool, bool, Optional[Exception]]:
    """
    Process a single deferred manufacturer for phase 2.

    Args:
        deferred_at: Timestamp for deferred processing
        deferred_mfg: DeferredManufacturer to process

    Returns:
        (etld1, success, updated, exception)
    """
    try:
        _deferred_manufacturer, updated = await upsert_deferred_manufacturer_phase2(
            timestamp=deferred_at,
            deferred_manufacturer=deferred_mfg,
        )
        logger.info(f"✅ Processed {deferred_mfg.mfg_etld1}: updated={updated}")
        return (deferred_mfg.mfg_etld1, True, updated, None)
    except Exception as e:
        logger.error(
            f"❌ Failed to process {deferred_mfg.mfg_etld1}: {e}", exc_info=True
        )
        return (deferred_mfg.mfg_etld1, False, False, e)


async def process_deferred_manufacturers_with_dynamic_concurrency(
    query_filter: dict,
    max_concurrent: int,
    batch_size: int,
    stats_interval: int,
    prefetch_threshold: int,
    limit: Optional[int],
):
    """
    Process deferred manufacturers (phase 2) with dynamic concurrency using sliding window batching.

    Args:
        query_filter: MongoDB query to filter deferred manufacturers
        max_concurrent: Maximum number of manufacturers to process concurrently
        batch_size: Number of manufacturers to fetch per batch
        stats_interval: Number of manufacturers between statistics reports
        prefetch_threshold: Trigger next batch fetch when remaining drops to this
        limit: Maximum number of manufacturers to process (optional, for testing)
    """

    start_time = time.perf_counter()
    deferred_at = get_current_time()

    # Get total count using pymongo collection directly
    collection = DeferredManufacturer.get_pymongo_collection()
    total_count = await collection.count_documents(query_filter)

    if limit:
        total_count = min(total_count, limit)

    logger.info(
        f"Starting phase 2 processing: {total_count} deferred manufacturers, "
        f"max_concurrent={max_concurrent}, batch_size={batch_size}, "
        f"prefetch_threshold={prefetch_threshold}"
    )

    # Semaphore to control concurrency
    semaphore = asyncio.Semaphore(max_concurrent)

    # Tracking variables
    processed = 0
    success_count = 0
    updated_count = 0
    failure_count = 0
    incomplete_phase1_error_count = 0
    duplicate_key_error_count = 0
    other_error_count = 0
    failed_etld1s = []
    incomplete_phase1_error_etld1s = []
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
    log_path = f"processing_phase2_log_{int(time.time())}.log"
    log_file = open(log_path, "w", encoding="utf-8")
    log_file.write(f"Phase 2 Processing started at {datetime.now()}\n")
    log_file.write(f"Max concurrent: {max_concurrent}\n")
    log_file.write(f"Stats interval: {stats_interval}\n")
    log_file.write("=" * 70 + "\n\n")
    log_file.flush()
    logger.info(f"Writing processing log to: {log_path}")

    async def process_with_semaphore(deferred_mfg: DeferredManufacturer):
        """Process a single deferred manufacturer with semaphore control"""
        nonlocal processed, success_count, updated_count, failure_count
        nonlocal incomplete_phase1_error_count, duplicate_key_error_count, other_error_count
        nonlocal last_stats_count, last_stats_time

        async with semaphore:
            mfg_start_time = asyncio.get_event_loop().time()
            etld1, success, updated, error = await process_single_deferred_manufacturer(
                deferred_at, deferred_mfg
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

                    if error_type == "incomplete_phase1":
                        incomplete_phase1_error_count += 1
                        incomplete_phase1_error_etld1s.append(etld1)
                    elif error_type == "duplicate_key":
                        duplicate_key_error_count += 1
                        duplicate_key_error_etld1s.append(etld1)
                        duplicate_key_error_buffer.append(etld1)
                    else:
                        other_error_count += 1
                        other_error_etld1s.append(etld1)
                        error_details = f"{type(error).__name__}: {str(error)}"
                        other_error_buffer.append((etld1, error_details))

                # Log progress every 10 manufacturers or on failure
                if processed % 10 == 0 or not success:
                    logger.info(
                        f"Progress: {processed}/{total_count} "
                        f"(✅ {success_count} | 🔄 {updated_count} updated | ❌ {failure_count}: "
                        f"Phase1: {incomplete_phase1_error_count}, DupKey: {duplicate_key_error_count}, Other: {other_error_count}) "
                        f"[last: {etld1} in {elapsed:.1f}s]"
                    )

                # Log detailed statistics at intervals
                if processed % stats_interval == 0:
                    current_time = time.perf_counter()
                    batch_count = processed - last_stats_count
                    batch_time = current_time - last_stats_time
                    overall_elapsed = current_time - start_time

                    # Calculate ETA
                    remaining = total_count - processed
                    current_rate = processed / overall_elapsed
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

                    # Write buffered errors
                    if duplicate_key_error_buffer:
                        log_file.write(
                            f"\nDuplicate Key Errors since last interval ({len(duplicate_key_error_buffer)}):\n"
                        )
                        for etld1 in duplicate_key_error_buffer:
                            log_file.write(f"  - {etld1}\n")
                        duplicate_key_error_buffer.clear()

                    if other_error_buffer:
                        log_file.write(
                            f"\nOther Errors since last interval ({len(other_error_buffer)}):\n"
                        )
                        for etld1, error_msg in other_error_buffer:
                            log_file.write(f"  - {etld1}:\n    {error_msg}\n")
                        other_error_buffer.clear()

                    log_file.write("\n")
                    log_file.flush()

                    # Update for next interval
                    last_stats_count = processed
                    last_stats_time = current_time

            return etld1, success, error

    # Sliding window batch processing
    skip = 0
    manufacturers_queue = []
    tasks = []
    fetch_lock = asyncio.Lock()
    is_fetching = False
    total_fetched = 0

    async def fetch_next_batch():
        """Fetch the next batch of deferred manufacturers from MongoDB"""
        nonlocal skip, is_fetching, total_fetched

        async with fetch_lock:
            if is_fetching:
                return
            is_fetching = True

        try:
            # Calculate how many to fetch
            remaining = total_count - total_fetched
            current_batch_size = min(batch_size, remaining)

            if current_batch_size <= 0:
                logger.info("No more deferred manufacturers to fetch")
                return

            logger.info(
                f"Fetching batch: skip={skip}, limit={current_batch_size} "
                f"(fetched: {total_fetched}/{total_count})"
            )

            # Fetch batch
            batch_docs = (
                await collection.find(query_filter)
                .skip(skip)
                .limit(current_batch_size)
                .to_list(length=current_batch_size)
            )

            if not batch_docs:
                logger.info("No documents returned from MongoDB")
                return

            logger.info(
                f"Fetched {len(batch_docs)} deferred manufacturers from MongoDB"
            )

            # Convert dicts to DeferredManufacturer objects
            batch_manufacturers = [DeferredManufacturer(**doc) for doc in batch_docs]

            # Add to queue
            manufacturers_queue.extend(batch_manufacturers)

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
            nonlocal prefetch_triggered
            prefetch_triggered = False

    # Fetch initial batch
    await fetch_next_batch()

    # Immediately start prefetching second batch
    if total_fetched < total_count:
        asyncio.create_task(fetch_next_batch())

    # Track if we've triggered prefetch for current batch
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
                break

        # Pop from queue and create task
        deferred_mfg = manufacturers_queue.pop(0)
        task = asyncio.create_task(process_with_semaphore(deferred_mfg))
        tasks.append(task)

        # Check if we should trigger prefetch
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
        f"✅ Phase 2 processing complete: {processed} total "
        f"(✅ {success_count} success, 🔄 {updated_count} updated | ❌ {failure_count} failed) "
        f"in {total_time:.2f}s (avg: {avg_time_per_mfg:.2f}s/mfg)"
    )

    # Log error breakdown
    if failure_count > 0:
        logger.info(
            f"Error breakdown: Incomplete phase1: {incomplete_phase1_error_count}, "
            f"Duplicate key errors: {duplicate_key_error_count}, "
            f"Other errors: {other_error_count}"
        )

    # Log incomplete phase1 errors
    if incomplete_phase1_error_etld1s:
        logger.warning(
            f"❌ Incomplete Phase1 Errors ({len(incomplete_phase1_error_etld1s)}): "
            f"{incomplete_phase1_error_etld1s[:10]}"
        )
        if len(incomplete_phase1_error_etld1s) > 10:
            logger.warning(f"... and {len(incomplete_phase1_error_etld1s) - 10} more")

    # Log duplicate key errors
    if duplicate_key_error_etld1s:
        logger.warning(
            f"❌ Duplicate Key Errors ({len(duplicate_key_error_etld1s)}): "
            f"{duplicate_key_error_etld1s[:10]}"
        )
        if len(duplicate_key_error_etld1s) > 10:
            logger.warning(f"... and {len(duplicate_key_error_etld1s) - 10} more")

    # Log other errors
    if other_error_etld1s:
        logger.warning(
            f"❌ Other Errors ({len(other_error_etld1s)}): {other_error_etld1s[:10]}"
        )
        if len(other_error_etld1s) > 10:
            logger.warning(f"... and {len(other_error_etld1s) - 10} more")

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
    log_file.write(f"  - Incomplete phase1 errors: {incomplete_phase1_error_count}\n")
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
        "incomplete_phase1_errors": incomplete_phase1_error_count,
        "duplicate_key_errors": duplicate_key_error_count,
        "other_errors": other_error_count,
        "failed_etld1s": failed_etld1s,
        "incomplete_phase1_error_etld1s": incomplete_phase1_error_etld1s,
        "duplicate_key_error_etld1s": duplicate_key_error_etld1s,
        "other_error_etld1s": other_error_etld1s,
        "total_time_seconds": total_time,
        "avg_time_per_manufacturer_seconds": avg_time_per_mfg,
    }


async def process_single_deferred_manufacturer_by_etld1(etld1: str):
    """Process a single deferred manufacturer by etld1 (for testing/debugging)"""
    # First find the manufacturer to get the scraped_text_file_version_id
    mfg = await find_manufacturer_by_etld1(mfg_etld1=etld1)
    if not mfg:
        logger.error(f"Manufacturer not found: {etld1}")
        return

    logger.info(f"Found manufacturer: {mfg.etld1}")

    # Get the deferred manufacturer
    deferred_mfg = await get_deferred_manufacturer_by_etld1_scraped_file_version(
        mfg_etld1=mfg.etld1,
        scraped_text_file_version_id=mfg.scraped_text_file_version_id,
    )

    if not deferred_mfg:
        logger.error(f"Deferred manufacturer not found: {etld1}")
        return

    logger.info(f"Found deferred manufacturer: {deferred_mfg.mfg_etld1}")
    deferred_at = get_current_time()

    etld1_result, success, updated, error = await process_single_deferred_manufacturer(
        deferred_at, deferred_mfg
    )

    if success:
        logger.info(f"✅ Successfully processed {etld1_result} (updated={updated})")
    else:
        logger.error(f"❌ Failed to process {etld1_result}: {error}")


async def async_main():
    """Main entry point with different processing modes"""
    parser = argparse.ArgumentParser(
        description="Process deferred manufacturers phase 2 for GPT batch processing"
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

    # Concurrency configuration
    MAX_POOL_SIZE = 120
    MAX_CONCURRENT = 20

    max_concurrent = MAX_CONCURRENT
    min_pool_size = max(20, MAX_POOL_SIZE // 4)

    logger.info("=" * 70)
    logger.info("PHASE 2 PROCESSING - CONCURRENCY CONFIGURATION")
    logger.info("=" * 70)
    logger.info(f"MongoDB Pool Configuration:")
    logger.info(f"  - Max pool size: {MAX_POOL_SIZE}")
    logger.info(f"  - Min pool size: {min_pool_size}")
    logger.info(f"Concurrency Configuration:")
    logger.info(f"  - Max concurrent manufacturers: {max_concurrent}")
    logger.info("=" * 70)

    await init_db(
        max_pool_size=MAX_POOL_SIZE,
        min_pool_size=min_pool_size,
        max_idle_time_ms=60000,
        server_selection_timeout_ms=30000,
        connect_timeout_ms=30000,
        socket_timeout_ms=120000,
    )

    # Initialize AWS clients
    await initialize_core_aws_clients()
    await initialize_data_etl_aws_clients()

    try:
        if args.mode == "single":
            # Process single deferred manufacturer
            logger.info(f"Processing single deferred manufacturer: {args.etld1}")
            await process_single_deferred_manufacturer_by_etld1(args.etld1)

        elif args.mode == "all":
            # Query filter for deferred manufacturers with complete phase 1
            # Phase 1 is complete when at least one concept field has deferred_stats with chunk_request_map
            query_filter = {
                "$or": [
                    {
                        "certificates.deferred_stats.chunk_request_map": {
                            "$exists": True,
                            "$ne": None,
                        }
                    },
                    {
                        "industries.deferred_stats.chunk_request_map": {
                            "$exists": True,
                            "$ne": None,
                        }
                    },
                    {
                        "process_caps.deferred_stats.chunk_request_map": {
                            "$exists": True,
                            "$ne": None,
                        }
                    },
                    {
                        "material_caps.deferred_stats.chunk_request_map": {
                            "$exists": True,
                            "$ne": None,
                        }
                    },
                ]
            }

            # Get count of deferred manufacturers matching the filter
            collection = DeferredManufacturer.get_pymongo_collection()
            matching_count = await collection.count_documents(query_filter)

            # Show filter details and count
            print("\n" + "=" * 70)
            print("DEFERRED MANUFACTURER PHASE 2 PROCESSING CONFIGURATION")
            print("=" * 70)
            print(f"Filter:")
            print(
                f"  - Phase 1 complete (at least one concept field has deferred_stats)"
            )
            print(f"Max concurrent: {max_concurrent}")
            print(f"MongoDB pool size: {MAX_POOL_SIZE}")
            print(f"Total deferred manufacturers needing phase 2: {matching_count}")
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

            print("\nStarting phase 2 processing...\n")
            logger.info(f"Processing {matching_count} deferred manufacturers")

            # Process with dynamic concurrency
            results = await process_deferred_manufacturers_with_dynamic_concurrency(
                query_filter=query_filter,
                max_concurrent=max_concurrent,
                limit=args.limit,
                stats_interval=args.stats_interval,
                batch_size=50,
                prefetch_threshold=49,
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
