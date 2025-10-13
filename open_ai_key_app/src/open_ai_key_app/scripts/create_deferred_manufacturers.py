import argparse
import asyncio
import logging
import multiprocessing
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
from data_etl_app.utils.process_pool_manager import ProcessPoolManager

from core.utils.mongo_client import init_db
from core.models.db.manufacturer import Manufacturer
from open_ai_key_app.models.db.deferred_manufacturer import DeferredManufacturer

logger = logging.getLogger(__name__)

from core.utils.time_util import get_current_time
from core.services.manufacturer_service import find_manufacturer_by_etld1
from open_ai_key_app.services.deferred_manufacturer_service import (
    upsert_deferred_manufacturer,
)


async def process_single_manufacturer(
    deferred_at: datetime,
    mfg: Manufacturer,
    existing_deferred_mfg: Optional["DeferredManufacturer"] = None,
) -> tuple[str, bool, Optional[Exception]]:
    """
    Process a single manufacturer and return result.

    Args:
        deferred_at: Timestamp for deferred processing
        mfg: Manufacturer to process
        existing_deferred_mfg: Pre-fetched deferred manufacturer (for batch optimization)

    Returns:
        (etld1, success, exception)
    """
    try:
        _deferred_manufacturer, updated = await upsert_deferred_manufacturer(
            timestamp=deferred_at,
            manufacturer=mfg,
            existing_deferred_manufacturer=existing_deferred_mfg,
        )
        logger.info(f"✅ Processed {mfg.etld1}: updated={updated}")
        return (mfg.etld1, True, None)
    except Exception as e:
        logger.error(f"❌ Failed to process {mfg.etld1}: {e}", exc_info=True)
        return (mfg.etld1, False, e)


async def process_batch(
    batch: list[Manufacturer],
    deferred_at: datetime,
) -> list[tuple[str, bool, Optional[Exception]] | BaseException]:
    """
    Process a batch of manufacturers with optimized database queries.

    Args:
        batch: List of manufacturers to process
        deferred_at: Timestamp for deferred processing

    Returns:
        List of (etld1, success, exception) tuples or BaseException for each manufacturer
    """
    # Batch fetch existing deferred manufacturers for this batch
    batch_keys = [(m.etld1, m.scraped_text_file_version_id) for m in batch]
    existing_deferred_mfgs = await DeferredManufacturer.find(
        {
            "$or": [
                {
                    "mfg_etld1": etld1,
                    "scraped_text_file_version_id": version_id,
                }
                for etld1, version_id in batch_keys
            ]
        }
    ).to_list()

    # Create lookup map for quick access
    deferred_mfg_map = {
        (dm.mfg_etld1, dm.scraped_text_file_version_id): dm
        for dm in existing_deferred_mfgs
    }

    # Process batch concurrently, passing pre-fetched deferred manufacturers
    results = await asyncio.gather(
        *[
            process_single_manufacturer(
                deferred_at,
                m,
                deferred_mfg_map.get((m.etld1, m.scraped_text_file_version_id)),
            )
            for m in batch
        ],
        return_exceptions=True,
    )

    return results


async def process_manufacturers_in_batches(
    query_filter: dict,
    batch_size: int = 5,
    limit: Optional[int] = None,
):
    """
    Process manufacturers using cursor with controlled parallelism.

    This hybrid approach:
    - Uses cursor for memory efficiency
    - Processes small batches concurrently for speed
    - Continues processing even if individual manufacturers fail

    Args:
        batch_size: Number of manufacturers to process concurrently
        query_filter: MongoDB query to filter manufacturers
        limit: Maximum number of manufacturers to process (optional, for testing)
    """

    deferred_at = get_current_time()

    # Get cursor for all manufacturers to process
    cursor = Manufacturer.find(query_filter)
    total_count = await Manufacturer.find(query_filter).count()

    logger.info(
        f"Starting batch processing: {total_count} manufacturers, "
        f"batch_size={batch_size}"
    )

    processed = 0
    success_count = 0
    failure_count = 0
    s3_version_error_count = 0
    other_error_count = 0
    current_batch = []
    failed_etld1s = []
    s3_version_error_etld1s = []
    other_error_etld1s = []

    async for mfg in cursor:
        current_batch.append(mfg)

        # Check if we've reached the limit
        if limit and (processed + len(current_batch)) >= limit:
            # Process what we have and stop
            logger.info(
                f"Reached limit of {limit} manufacturers, processing final batch"
            )
            break

        # When batch is full, process it
        if len(current_batch) >= batch_size:
            # Process batch with optimized database queries
            results = await process_batch(current_batch, deferred_at)

            # Update counters
            for result in results:
                processed += 1
                if isinstance(result, BaseException):
                    # Unexpected exception from asyncio.gather
                    failure_count += 1
                    other_error_count += 1
                    logger.error(
                        f"Unexpected error in batch: {result}", exc_info=result
                    )
                else:
                    # Normal result tuple
                    etld1, success, error = result
                    if success:
                        success_count += 1
                    else:
                        failure_count += 1
                        failed_etld1s.append(etld1)

                        # Categorize error type
                        if error and "NoSuchVersion" in str(error):
                            s3_version_error_count += 1
                            s3_version_error_etld1s.append(etld1)
                        else:
                            other_error_count += 1
                            other_error_etld1s.append(etld1)

            logger.info(
                f"Progress: {processed}/{total_count} "
                f"(✅ {success_count} | ❌ {failure_count}: "
                f"S3 version errors: {s3_version_error_count}, Other: {other_error_count})"
            )

            # Clear batch for next iteration
            current_batch = []

    # Process remaining manufacturers in last partial batch
    if current_batch:
        # Process final batch with optimized database queries
        results = await process_batch(current_batch, deferred_at)

        for result in results:
            processed += 1
            if isinstance(result, BaseException):
                # Unexpected exception from asyncio.gather
                failure_count += 1
                other_error_count += 1
                logger.error(
                    f"Unexpected error in final batch: {result}", exc_info=result
                )
            else:
                # Normal result tuple
                etld1, success, error = result
                if success:
                    success_count += 1
                else:
                    failure_count += 1
                    failed_etld1s.append(etld1)

                    # Categorize error type
                    if error and "NoSuchVersion" in str(error):
                        s3_version_error_count += 1
                        s3_version_error_etld1s.append(etld1)
                    else:
                        other_error_count += 1
                        other_error_etld1s.append(etld1)

    logger.info(
        f"✅ Batch processing complete: {processed} total "
        f"(✅ {success_count} success | ❌ {failure_count} failed)"
    )

    # Log error breakdown
    if failure_count > 0:
        logger.info(
            f"Error breakdown: S3 version errors: {s3_version_error_count}, "
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

    # Log other errors separately
    if other_error_etld1s:
        logger.warning(
            f"❌ Other Errors ({len(other_error_etld1s)}): {other_error_etld1s[:10]}"
        )
        if len(other_error_etld1s) > 10:
            logger.warning(f"... and {len(other_error_etld1s) - 10} more")

    return {
        "total": processed,
        "success": success_count,
        "failed": failure_count,
        "s3_version_errors": s3_version_error_count,
        "other_errors": other_error_count,
        "failed_etld1s": failed_etld1s,
        "s3_version_error_etld1s": s3_version_error_etld1s,
        "other_error_etld1s": other_error_etld1s,
    }


async def process_single_manufacturer_by_etld1(etld1: str):
    """Process a single manufacturer by etld1 (for testing/debugging)"""
    mfg = await find_manufacturer_by_etld1(mfg_etld1=etld1)
    if not mfg:
        logger.error(f"Manufacturer not found: {etld1}")
        return

    logger.info(f"Found manufacturer: {mfg.etld1}")
    deferred_at = get_current_time()

    etld1_result, success, error = await process_single_manufacturer(deferred_at, mfg)

    if success:
        logger.info(f"✅ Successfully processed {etld1_result}")
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
        default="single",
        help="Processing mode: single manufacturer or all manufacturers",
    )
    parser.add_argument(
        "--etld1",
        type=str,
        default="limitedproductions.net",
        help="Manufacturer etld1 (for single mode)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=5,
        help="Number of manufacturers to process concurrently (for all mode)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Maximum number of manufacturers to process (for testing)",
    )

    args = parser.parse_args()

    await init_db()

    # Initialize AWS clients
    await initialize_core_aws_clients()
    await initialize_data_etl_aws_clients()

    # Initialize process pool for parallel chunking
    # Use fewer workers for batch processing to avoid overwhelming system
    cpu_count = multiprocessing.cpu_count()
    max_workers = min(cpu_count - 2, 8)  # Leave 2 cores for system, cap at 6

    ProcessPoolManager.initialize(max_workers=max_workers)
    logger.info(
        f"Process pool initialized with {max_workers} workers "
        f"(system has {cpu_count} CPU cores)"
    )

    try:
        if args.mode == "single":
            # Process single manufacturer
            logger.info(f"Processing single manufacturer: {args.etld1}")
            await process_single_manufacturer_by_etld1(args.etld1)

        elif args.mode == "all":
            # Process manufacturers with scraped_text_file_num_tokens < 1000000
            query_filter = {
                "scraped_text_file_version_id": {"$exists": True},
                "scraped_text_file_num_tokens": {"$lt": 1000000},
            }

            # Get count of manufacturers matching the filter
            matching_count = await Manufacturer.find(query_filter).count()

            # Show filter details and count
            print("\n" + "=" * 70)
            print("MANUFACTURER PROCESSING CONFIGURATION")
            print("=" * 70)
            print(f"Filter: scraped_text_file_num_tokens < 1,000,000")
            print(f"Batch size: {args.batch_size}")
            print(f"Total manufacturers: {matching_count}")
            if args.limit:
                print(f"Limit: {args.limit} (testing mode)")
                print(f"Will process: {min(args.limit, matching_count)} manufacturers")
            else:
                print(f"Will process: {matching_count} manufacturers (ALL matching)")
            print("=" * 70)

            # Ask for confirmation
            response = input("\nDo you want to proceed? (yes/no): ").strip().lower()

            if response not in ["yes", "y"]:
                logger.info("Processing cancelled by user")
                print("Processing cancelled.")
                return

            print("\nStarting processing...\n")
            logger.info(f"User confirmed processing of {matching_count} manufacturers")

            results = await process_manufacturers_in_batches(
                query_filter=query_filter,
                batch_size=args.batch_size,
                limit=args.limit,
            )
            logger.info(f"Final results: {results}")

    finally:
        # Clean up process pool
        ProcessPoolManager.shutdown(wait=True)
        logger.info("Process pool shut down")

        # Clean up AWS clients
        await cleanup_data_etl_aws_clients()
        await cleanup_core_aws_clients()


def main():
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
