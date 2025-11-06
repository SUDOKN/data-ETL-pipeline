#!/usr/bin/env python3
from datetime import datetime
import logging
from typing import Optional
import argparse
import asyncio
from asyncio import Task

# Note: Environment variables should be loaded by the entry point script
# (e.g., batch_file_station.py) before importing this module

from core.utils.mongo_client import init_db
from core.utils.time_util import get_current_time
from core.models.db.manufacturer import Manufacturer
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
) -> None:
    """
    Process a single manufacturer: download scraped text and run extraction pipeline.
    """
    # Download scraped text file from S3
    scraped_text_file = await ScrapedTextFile.download_from_s3_and_create(
        mfg.etld1, mfg.scraped_text_file_version_id
    )

    # Process manufacturer through the orchestrator
    await orchestrator.process_manufacturer(timestamp, mfg, scraped_text_file)
    logger.info(f"[{mfg.etld1}] ✓ Processing complete")


async def process_manufacturers_concurrently(
    limit: Optional[int] = None,
    parallel: int = 10,
    dry_run: bool = False,
):
    """
    Process multiple manufacturers concurrently with a parallelism limit.

    Args:
        limit: Maximum number of manufacturers to process (None = all)
        parallel: Maximum number of concurrent manufacturer processing tasks
        dry_run: If True, don't actually create batch requests
    """
    orchestrator = ManufacturerExtractionOrchestrator()
    timestamp = get_current_time()

    # Query for manufacturers with scraped text files
    # query_filter = {
    #     "scraped_text_file_version_id": {"$exists": True},
    #     "scraped_text_file_num_tokens": {"$lt": 200_000},
    # }

    query_filter = {
        # "scraped_text_file_version_id": {"$exists": True},
        "scraped_text_file_num_tokens": {"$lt": 200_000},
        "$or": [
            {"addresses": {"$eq": None}},
            {"business_desc": {"$eq": None}},
            {"is_contract_manufacturer": {"$eq": None}},
            {"is_manufacturer": {"$eq": None}},
            {"is_product_manufacturer": {"$eq": None}},
            {"products": {"$eq": None}},
            {"certificates": {"$eq": None}},
            {"industries": {"$eq": None}},
            {"material_caps": {"$eq": None}},
            {"process_caps": {"$eq": None}},
        ],
    }

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

    collection = Manufacturer.get_pymongo_collection()
    total_count = await collection.count_documents(query_filter)

    if limit:
        total_count = min(total_count, limit)

    logger.info(f"Processing {total_count:,} manufacturers with parallelism={parallel}")
    if dry_run:
        logger.info("🔍 DRY RUN MODE - No batch requests will be created\n")

    # Create cursor
    cursor = (
        collection.find(query_filter)
        .sort("scraped_text_file_num_tokens", 1)
        .limit(limit if limit else 0)
    )

    # Statistics
    processed = 0
    succeeded = 0
    failed = 0
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
                f"Completed: {succeeded} succeeded, {failed} failed"
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

    # Log final summary
    logger.info("\n" + "=" * 70)
    logger.info("PROCESSING COMPLETE")
    logger.info("=" * 70)
    logger.info(f"Total processed: {processed:,}")
    logger.info(f"Succeeded: {succeeded:,}")
    logger.info(f"Failed: {failed:,}")
    logger.info("=" * 70)


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
    parser.add_argument(
        "--parallel",
        type=int,
        default=10,
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
