import asyncio
import argparse
import logging
import csv
from datetime import datetime
from pathlib import Path

from core.dependencies.load_core_env import load_core_env
from core.models.db.deferred_manufacturer import DeferredManufacturer
from scraper_app.dependencies.load_scraper_env import load_scraper_env
from open_ai_key_app.dependencies.load_open_ai_app_env import load_open_ai_app_env
from data_etl_app.dependencies.load_data_etl_env import load_data_etl_env

# Load environment variables
load_core_env()
load_scraper_env()
load_data_etl_env()
load_open_ai_app_env()

from core.utils.mongo_client import init_db
from core.models.db.manufacturer import Manufacturer
from data_etl_app.services.batch_file_generator import (
    BatchFileGenerationResult,
    iterate_df_manufacturers_and_write_batch_files,
)

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

# Constraints
OUTPUT_DIR_DEFAULT = "../../../../batch_data"
BATCH_FILE_ETLD1S_FILE_DEFAULT = "./batch_file_etld1s.csv"
MAX_MANUFACTURER_TOKENS = 200_000
MAX_TOKENS_PER_FILE = 20_000_000
MAX_REQUESTS_PER_FILE = 40_000
MAX_FILE_SIZE_MB = 120  # 120MB in MB


def get_etld1s_from_file() -> list[str]:
    """Read etld1s from the specified CSV file."""
    etld1s = []
    with open(
        BATCH_FILE_ETLD1S_FILE_DEFAULT, "r", newline="", encoding="utf-8"
    ) as csvfile:
        reader = csv.reader(csvfile)
        next(reader, None)  # Skip header
        for row in reader:
            if row:
                etld1s.append(row[0].strip())
    return etld1s


async def async_main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Collect incomplete GPT batch requests from deferred manufacturers"
    )

    def add_arguments():
        parser.add_argument(
            "--output-dir",
            type=str,
            default=f"{OUTPUT_DIR_DEFAULT}/pending_batch_requests",
            help="Output directory for batch request JSONL files (default: ./pending_batch_requests)",
        )
        parser.add_argument(
            "--limit",
            type=int,
            default=None,
            help="Maximum number of manufacturers to process (for testing)",
        )
        parser.add_argument(
            "--only-etld1s",
            type=bool,
            help=f"If set, only process manufacturers with the etld1s in {BATCH_FILE_ETLD1S_FILE_DEFAULT}",
        )
        parser.add_argument(
            "--max-files",
            type=int,
            help="Maximum number of batch files to create before stopping",
        )
        parser.add_argument(
            "--yes",
            "-y",
            action="store_true",
            help="Skip confirmation prompt and proceed automatically",
        )

    add_arguments()

    args = parser.parse_args()

    def validate_arguments():
        if args.only_etld1s and args.limit:
            raise ValueError("--only-etld1s and --limit cannot be used together.")

    validate_arguments()

    # Convert output dir to Path
    output_dir = Path(args.output_dir)

    # Initialize MongoDB
    logger.info("Initializing MongoDB connection...")
    await init_db(
        max_pool_size=10,  # Reduced from 50 - this is primarily a read-only operation
        min_pool_size=2,  # Reduced from 10 - we don't need many idle connections
        max_idle_time_ms=60000,
        server_selection_timeout_ms=30000,
        connect_timeout_ms=30000,
        socket_timeout_ms=120000,
    )

    # Query filter: manufacturers with scraped text file
    query_filter = {
        "scraped_text_file_version_id": {"$exists": True},
        "scraped_text_file_num_tokens": {"$lt": MAX_MANUFACTURER_TOKENS},
    }

    # Add etld1 filter if specified
    if args.only_etld1s:
        etld1s_to_match = get_etld1s_from_file()
        query_filter["mfg_etld1"] = {"$in": etld1s_to_match}

    collection = DeferredManufacturer.get_pymongo_collection()

    async def show_config() -> None:
        # Get count
        matching_count = await collection.count_documents(query_filter)
        """Format number with commas as thousand separators."""
        # Show configuration
        print("\n" + "=" * 70)
        print("INCOMPLETE BATCH REQUESTS COLLECTION")
        print("=" * 70)
        if args.only_etld1s:
            print(
                f"Filter: manufacturer with etld1 matching any one of {etld1s_to_match:,} in {BATCH_FILE_ETLD1S_FILE_DEFAULT}"
            )
        else:
            print(f"Filter: manufacturers with scraped_text_file_version_id")
        print(f"Total matching manufacturers: {matching_count:,}")

        if args.limit:
            print(f"Limit: {args.limit:,} manufacturers")
            print(f"Will process: {min(args.limit, matching_count):,} manufacturers")
        else:
            print(f"Will process: {matching_count:,} manufacturers (ALL)")
        if args.max_files:
            print(
                f"Max files: {args.max_files} (will stop after creating this many files)"
            )
        print(f"Output directory: {output_dir}")
        print(f"Constraints:")
        print(f"  - Max {MAX_TOKENS_PER_FILE:,} tokens per file")
        print(f"  - Max {MAX_REQUESTS_PER_FILE:,} requests per file")
        print(f"  - Max {MAX_FILE_SIZE_MB} MB per file")
        print("=" * 70)

        # Ask for confirmation (unless --yes flag is set)
        if not args.yes:
            response = input("\nDo you want to proceed? (yes/no): ").strip().lower()
            if response not in ["yes", "y"]:
                logger.info("Collection cancelled by user")
                print("Collection cancelled.")
                return
        else:
            logger.info("Auto-proceeding with --yes flag")

    await show_config()
    run_timestamp = datetime.now()

    results: BatchFileGenerationResult = (
        await iterate_df_manufacturers_and_write_batch_files(
            timestamp=run_timestamp,
            query_filter=query_filter,
            output_dir=output_dir,
            max_requests_per_file=MAX_REQUESTS_PER_FILE,
            max_tokens_per_file=MAX_TOKENS_PER_FILE,
            max_file_size_in_bytes=MAX_FILE_SIZE_MB * 1024 * 1024,
            max_manufacturers=args.limit,
            max_files=args.max_files,
        )
    )

    logger.info(f"Collection results: {results.final_summary}")


def main():
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
