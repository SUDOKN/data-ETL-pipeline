#!/usr/bin/env python3
"""
Migration script to pair batch requests from a finished batch output file.

This migration:
1. Reads custom IDs from a finished batch JSONL file
2. Finds the GPTBatch with the matching external_batch_id
3. Pairs all custom IDs with that batch using pair_batch_request_custom_ids_with_batch
"""

import asyncio
import json
import logging
from pathlib import Path

from core.dependencies.load_core_env import load_core_env
from scraper_app.dependencies.load_scraper_env import load_scraper_env
from open_ai_key_app.dependencies.load_open_ai_app_env import load_open_ai_app_env
from data_etl_app.dependencies.load_data_etl_env import load_data_etl_env

# Load environment variables
load_core_env()
load_scraper_env()
load_data_etl_env()
load_open_ai_app_env()

from core.models.db.gpt_batch import GPTBatch
from core.services.gpt_batch_request_service import (
    pair_batch_request_custom_ids_with_batch,
)
from core.utils.mongo_client import init_db

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

# Configuration
BATCH_OUTPUT_FILE = Path(
    "/Users/amaryadav/batch_data/finished_batches/batch_691056ea68ac8190875d1b37ed109b0a_output.jsonl"
)
EXTERNAL_BATCH_ID = "batch_691056ea68ac8190875d1b37ed109b0a"


def read_custom_ids_from_jsonl(file_path: Path) -> set[str]:
    """
    Read custom IDs from a batch output JSONL file.

    Args:
        file_path: Path to the JSONL file

    Returns:
        Set of custom IDs extracted from the file
    """
    custom_ids = set()

    logger.info(f"Reading custom IDs from {file_path}")

    with open(file_path, "r") as f:
        for line_num, line in enumerate(f, 1):
            try:
                data = json.loads(line.strip())
                custom_id = data.get("custom_id")

                if custom_id:
                    custom_ids.add(custom_id)
                else:
                    logger.warning(f"Line {line_num}: No custom_id found in data")

            except json.JSONDecodeError as e:
                logger.error(f"Line {line_num}: Failed to parse JSON: {e}")
                continue

    logger.info(f"Extracted {len(custom_ids)} custom IDs from {file_path.name}")
    return custom_ids


async def find_batch_by_external_id(external_batch_id: str) -> GPTBatch | None:
    """
    Find a GPTBatch by its external_batch_id.

    Args:
        external_batch_id: The external batch ID to search for

    Returns:
        GPTBatch if found, None otherwise
    """
    batch = await GPTBatch.find_one(GPTBatch.external_batch_id == external_batch_id)
    return batch


async def main():
    await init_db()
    logger.info("Database initialized.\n")

    logger.info("=" * 80)
    logger.info("Batch Request Pairing Migration")
    logger.info("=" * 80)

    # Check if file exists
    if not BATCH_OUTPUT_FILE.exists():
        logger.error(f"❌ Batch output file not found: {BATCH_OUTPUT_FILE}")
        return

    # Read custom IDs from file
    logger.info(f"\nReading custom IDs from: {BATCH_OUTPUT_FILE.name}")
    custom_ids = read_custom_ids_from_jsonl(BATCH_OUTPUT_FILE)

    if not custom_ids:
        logger.error("❌ No custom IDs found in the batch output file")
        return

    logger.info(f"✓ Found {len(custom_ids)} custom IDs")

    # Find the batch
    logger.info(f"\nSearching for batch with external_batch_id: {EXTERNAL_BATCH_ID}")
    batch = await find_batch_by_external_id(EXTERNAL_BATCH_ID)

    if not batch:
        logger.error(f"❌ Batch with external_batch_id '{EXTERNAL_BATCH_ID}' not found")
        return

    logger.info(f"✓ Found batch: {batch.external_batch_id}")
    logger.info(f"  Status: {batch.status}")
    logger.info(f"  Created at: {batch.created_at}")

    # Display summary
    logger.info("\n" + "=" * 80)
    logger.info("MIGRATION SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Batch file: {BATCH_OUTPUT_FILE.name}")
    logger.info(f"External batch ID: {EXTERNAL_BATCH_ID}")
    logger.info(f"Custom IDs to pair: {len(custom_ids):,}")
    logger.info("=" * 80)

    # Ask for confirmation
    logger.info(
        "\n⚠️  WARNING: This operation will update batch_id for matching batch requests!"
    )
    response = input("\nDo you want to proceed? (yes/no): ").strip().lower()

    if response not in ["yes", "y"]:
        logger.info("\n❌ Operation cancelled by user.")
        return

    logger.info("\n✅ Proceeding with pairing...\n")
    logger.info("=" * 80)

    # Pair the custom IDs with the batch
    modified_count = await pair_batch_request_custom_ids_with_batch(
        custom_ids=custom_ids,
        gpt_batch=batch,
    )

    # Display results
    logger.info("\n" + "=" * 80)
    logger.info("PAIRING RESULTS")
    logger.info("=" * 80)
    logger.info(f"Total custom IDs processed: {len(custom_ids):,}")
    logger.info(f"Total batch requests modified: {modified_count:,}")
    logger.info("=" * 80)

    if modified_count < len(custom_ids):
        logger.warning(
            f"\n⚠️  WARNING: Only {modified_count}/{len(custom_ids)} custom IDs were paired. "
            f"This could mean some batch requests don't exist in the database."
        )
    else:
        logger.info("\n✅ All custom IDs successfully paired with the batch!")

    logger.info("\n" + "=" * 80)
    logger.info("✅ OPERATION COMPLETED SUCCESSFULLY!")
    logger.info("=" * 80)


if __name__ == "__main__":
    asyncio.run(main())
