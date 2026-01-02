#!/usr/bin/env python3
from datetime import datetime
import logging
from typing import Optional
import argparse
import asyncio
import csv
from pathlib import Path
from asyncio import Task

# Note: Environment variables should be loaded by the entry point script
# (e.g., batch_file_station.py) before importing this module

from core.dependencies.load_core_env import load_core_env
from scraper_app.dependencies.load_scraper_env import load_scraper_env
from open_ai_key_app.dependencies.load_open_ai_app_env import load_open_ai_app_env
from data_etl_app.dependencies.load_data_etl_env import load_data_etl_env

# Load environment variables (entry point)
load_core_env()
load_scraper_env()
load_data_etl_env()
load_open_ai_app_env()


from core.utils.mongo_client import init_db
from core.utils.time_util import get_current_time
from core.models.db.manufacturer import Manufacturer
from core.models.db.deferred_manufacturer import DeferredManufacturer
from data_etl_app.services.manufacturer_extraction_orchestrator import (
    ManufacturerExtractionOrchestrator,
)
from scraper_app.models.scraped_text_file import ScrapedTextFile

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logging.getLogger("botocore").setLevel(logging.WARNING)
logging.getLogger("boto3").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)


async def process_single_manufacturer(
    orchestrator: ManufacturerExtractionOrchestrator,
    timestamp: datetime,
    mfg: Manufacturer,
) -> tuple[bool, Optional[str]]:
    """
    Process a single manufacturer: download scraped text and run extraction pipeline.

    Returns:
        tuple[bool, Optional[str]]: (success, error_message)
    """
    # # Download scraped text file from S3
    # scraped_text_file = await ScrapedTextFile.download_from_s3_and_create(
    #     mfg.etld1, mfg.scraped_text_file_version_id
    # )

    # Process manufacturer through the orchestrator
    try:
        await orchestrator.process_manufacturer(timestamp, mfg)
        logger.debug(f"process_single_manufacturer:[{mfg.etld1}] ✓ Processing complete")
        return (True, None)
    except Exception as e:
        error_msg = str(e)
        logger.error(
            f"process_single_manufacturer:[{mfg.etld1}] ❌ Error during processing: {e}",
            exc_info=True,
        )
        return (False, error_msg)


async def process_manufacturers_concurrently(
    parallel: int,
    limit: Optional[int] = None,
    dry_run: bool = False,
    mfg_etld1s_from_csv: Optional[set[str]] = None,
):
    """
    Process multiple manufacturers concurrently with a parallelism limit.

    Args:
        limit: Maximum number of manufacturers to process (None = all)
        parallel: Maximum number of concurrent manufacturer processing tasks
        dry_run: If True, don't actually create batch requests
        mfg_etld1s_from_csv: Optional set of mfg_etld1s to process from CSV file
    """
    orchestrator = ManufacturerExtractionOrchestrator()
    timestamp = get_current_time()

    # Query for manufacturers with scraped text files
    # query_filter = {
    #     "scraped_text_file_version_id": {"$exists": True},
    #     "scraped_text_file_num_tokens": {"$lt": 200_000},
    # }

    query_filter = {
        "scraped_text_file_version_id": {"$exists": True},
        # "scraped_text_file_num_tokens": {"$gt": 100},
        "$or": [
            {"addresses": {"$eq": None}},
            {"business_desc": {"$eq": None}},
            # {"is_contract_manufacturer": {"$eq": None}},
            {"is_manufacturer": {"$eq": None}},
            # {"is_product_manufacturer": {"$eq": None}},
            {"products": {"$eq": None}},
            {"certificates": {"$eq": None}},
            {"industries": {"$eq": None}},
            {"material_caps": {"$eq": None}},
            {"process_caps": {"$eq": None}},
        ],
    }

    # If CSV file was provided, filter by etld1s from the file
    if mfg_etld1s_from_csv:
        query_filter["etld1"] = {"$in": list(mfg_etld1s_from_csv)}
        logger.info(f"Filtering by {len(mfg_etld1s_from_csv)} etld1s from CSV file")

    """
    query_filter = {
        "$and": [
            {"addresses": {"$ne": None}},
            {"business_desc": {"$ne": None}},
            {"is_contract_manufacturer": {"$ne": None}},
            {"is_manufacturer": {"$ne": None}},
            {"is_product_manufacturer": {"$ne": None}},
            {"products": {"$ne": None}},
            {"certificates": {"$ne": None}},
            {"industries": {"$ne": None}},
            {"material_caps": {"$ne": None}},
            {"process_caps": {"$ne": None}},
        ]
    }
    """

    df_mfg_collection = DeferredManufacturer.get_pymongo_collection()
    df_mfgs = await df_mfg_collection.find({}, {"mfg_etld1": 1, "_id": 0}).to_list(
        length=None
    )
    df_mfgdf_etld1s = [df_mfg["mfg_etld1"] for df_mfg in df_mfgs]
    logger.info(f"some df_mfgdf_etld1s:{df_mfgdf_etld1s[0:5]}")

    query_filter["etld1"] = {"$nin": df_mfgdf_etld1s}
    # logger.info(f"query_filter:{query_filter}")
    mfg_collection = Manufacturer.get_pymongo_collection()
    total_count = await mfg_collection.count_documents(query_filter)

    if limit:
        total_count = min(total_count, limit)

    logger.info(f"\n{'='*70}")
    logger.info(f"Found {total_count:,} manufacturers matching the query")
    logger.info(f"Parallelism: {parallel}")
    if dry_run:
        logger.info("Mode: 🔍 DRY RUN (No batch requests will be created)")
    logger.info(f"{'='*70}\n")

    # Wait for user confirmation
    user_input = (
        input(
            f"Do you want to proceed with processing {total_count:,} manufacturers? (yes/no): "
        )
        .strip()
        .lower()
    )
    if user_input not in ["yes", "y"]:
        logger.info("Processing cancelled by user.")
        return

    logger.info(f"\nStarting to process {total_count:,} manufacturers...\n")

    # Create cursor
    cursor = (
        mfg_collection.find(query_filter)
        .sort("created_at", -1)
        .limit(limit if limit else 0)
    )

    # Statistics
    processed = 0
    succeeded = 0
    failed = 0
    no_such_version: list[str] = []
    active_tasks: set[Task] = set()

    async for mfg_doc in cursor:
        mfg = Manufacturer(**mfg_doc)

        # Wait if we've reached the parallelism limit
        if len(active_tasks) >= parallel:
            # Wait for at least one task to complete
            done, active_tasks = await asyncio.wait(
                active_tasks, return_when=asyncio.FIRST_COMPLETED
            )

            # Update statistics from completed tasks
            for task in done:
                success, error = await task
                if success:
                    succeeded += 1
                else:
                    failed += 1
                if error and "NoSuchVersion" in error:
                    no_such_version.append(mfg.etld1)

        # Start processing this manufacturer
        task = asyncio.create_task(
            process_single_manufacturer(orchestrator, timestamp, mfg)
        )
        active_tasks.add(task)
        processed += 1

        if processed % 10 == 0:
            logger.info(
                f"Progress: {processed}/{total_count} started | "
                f"Active: {len(active_tasks)} | "
                f"Completed: {succeeded} succeeded, {failed} failed\n"
                f"NoSuchVersion errors for: {len(no_such_version)} manufacturers"
            )

    # Wait for all remaining tasks to complete
    if active_tasks:
        logger.info(f"Waiting for {len(active_tasks)} remaining tasks to complete...")
        done, _ = await asyncio.wait(active_tasks)

        for task in done:
            success, error = await task
            if success:
                succeeded += 1
            else:
                failed += 1
            if error and "NoSuchVersion" in error:
                no_such_version.append(mfg.etld1)

    # Log final summary
    logger.info("\n" + "=" * 70)
    logger.info("PROCESSING COMPLETE")
    logger.info("=" * 70)
    logger.info(f"Total processed: {processed:,}")
    logger.info(f"Succeeded: {succeeded:,}")
    logger.info(f"Failed: {failed:,}")
    logger.info(f"NoSuchVersion errors for {len(no_such_version)} manufacturers:")
    logger.info("=" * 70)

    # save no such version etld1s to a file
    if no_such_version:
        output_path = Path(__file__).parent / "no_such_version_etld1s.txt"
        with open(output_path, "w") as f:
            for etld1 in no_such_version:
                f.write(f"{etld1}\n")
        logger.info(f"NoSuchVersion etld1s saved to {output_path}")


async def async_main():
    """Main async execution function."""
    parser = argparse.ArgumentParser(
        description="Process manufacturer(s) and create batch requests for missing data"
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--etld1",
        type=str,
        help="Process a single manufacturer by etld1 (e.g., example.com)",
    )
    group.add_argument(
        "--limit",
        type=int,
        help="Process up to N manufacturers from the database",
    )
    group.add_argument(
        "--csv",
        action="store_true",
        help="Process manufacturers from orchestrate_mfg_etld1s.csv in the same directory",
    )
    parser.add_argument(
        "--parallel",
        type=int,
        default=100,
        help="Maximum number of manufacturers to process concurrently (default: 100)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Don't actually create batch requests, just show what would be done",
    )

    args = parser.parse_args()

    # Initialize database
    await init_db(
        max_pool_size=500,
        min_pool_size=50,
        max_idle_time_ms=60000,
        server_selection_timeout_ms=30000,
        connect_timeout_ms=30000,
        socket_timeout_ms=120000,
    )
    logger.info("✓ Database initialized successfully\n")

    from data_etl_app.dependencies.aws_clients import initialize_data_etl_aws_clients
    from core.dependencies.aws_clients import initialize_core_aws_clients

    await initialize_core_aws_clients()
    await initialize_data_etl_aws_clients()

    try:
        if args.etld1:
            # Single manufacturer mode
            mfg = await Manufacturer.find_one(Manufacturer.etld1 == args.etld1)

            if not mfg:
                logger.error(f"Manufacturer with etld1 '{args.etld1}' not found")
                return

            logger.info(f"Found manufacturer: {args.etld1}")
            logger.info(f"  URL: {mfg.url_accessible_at}")
            logger.info(f"  Name: {mfg.name}")
            logger.info(
                f"  Scraped text file version ID: {mfg.scraped_text_file_version_id}"
            )
            logger.info(f"  Num tokens: {mfg.scraped_text_file_num_tokens}\n")

            orchestrator = ManufacturerExtractionOrchestrator()
            timestamp = get_current_time()

            await process_single_manufacturer(orchestrator, timestamp, mfg)

        elif args.csv:
            # CSV mode - read mfg_etld1s from CSV file
            csv_path = Path(__file__).parent / "orchestrate_mfg_etld1s.csv"

            if not csv_path.exists():
                logger.error(f"CSV file not found: {csv_path}")
                logger.error(
                    "Please create orchestrate_mfg_etld1s.csv in the same directory"
                )
                return

            mfg_etld1s = set()
            with open(csv_path, "r") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    mfg_etld1 = row.get("mfg_etld1", "").strip()
                    if mfg_etld1:
                        mfg_etld1s.add(mfg_etld1)

            logger.info(f"Loaded {len(mfg_etld1s)} unique mfg_etld1s from {csv_path}")

            await process_manufacturers_concurrently(
                limit=None,
                parallel=args.parallel,
                dry_run=args.dry_run,
                mfg_etld1s_from_csv=mfg_etld1s,
            )

        else:
            # Bulk processing mode
            await process_manufacturers_concurrently(
                limit=args.limit,
                parallel=args.parallel,
                dry_run=args.dry_run,
            )

    except Exception as e:
        logger.error(f"Error during processing: {e}", exc_info=True)
        raise


def main():
    """Main execution function."""
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
