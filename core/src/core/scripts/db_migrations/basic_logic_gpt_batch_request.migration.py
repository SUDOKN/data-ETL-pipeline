#!/usr/bin/env python3
"""
Migration script to update GPT batch request documents with model="basic_logic".

This migration:
1. Finds all GPT batch request documents where request.body.model = "basic_logic"
2. Updates their batch_id to "empty_unmapped_unknowns"
3. Updates their response_blob with the dummy response structure
"""

import asyncio
import logging
from datetime import datetime
from pymongo import UpdateOne

from core.dependencies.load_core_env import load_core_env
from scraper_app.dependencies.load_scraper_env import load_scraper_env
from open_ai_key_app.dependencies.load_open_ai_app_env import load_open_ai_app_env
from data_etl_app.dependencies.load_data_etl_env import load_data_etl_env

# Load environment variables
load_core_env()
load_scraper_env()
load_data_etl_env()
load_open_ai_app_env()

from core.models.db.gpt_batch_request import GPTBatchRequest
from core.utils.mongo_client import init_db

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

FETCH_BATCH_SIZE = 5000  # Fetch this many documents at a time
UPDATE_BATCH_SIZE = 1000  # Update this many documents at a time


def get_dummy_response_blob(created_at: datetime) -> dict:
    """
    Get the dummy response blob structure matching _get_dummy_completed_batch_request.
    """
    return {
        "batch_id": "empty_unmapped_unknowns",
        "request_custom_id": "no-request",
        "response": {
            "status_code": 200,
            "body": {
                "created": created_at,
                "choices": [
                    {
                        "index": 0,
                        "message": {"role": "assistant", "content": "```json\n{}\n```"},
                    }
                ],
                "usage": {
                    "prompt_tokens": 1,
                    "completion_tokens": 1,
                    "total_tokens": 2,
                },
            },
        },
    }


async def count_basic_logic_requests(collection):
    """Count documents with model='basic_logic' using cursor iteration."""
    logger.info("Counting GPT batch requests with model='basic_logic'...")

    total_count = 0
    cursor = (
        collection.find(
            {"request.body.model": "basic_logic", "response_blob": None},
            {"_id": 1},
        )
        .hint([("request.body.model", 1)])
        .batch_size(FETCH_BATCH_SIZE)
    )

    async for _ in cursor:
        total_count += 1
        if total_count % 1000 == 0:
            logger.info(f"Progress: Counted {total_count} documents so far...")

    logger.info(f"Final count: Found {total_count} documents with model='basic_logic'")
    return total_count


async def update_basic_logic_requests(collection):
    """
    Update GPT batch request documents with model="basic_logic" in batches.
    """
    logger.info("=" * 80)
    logger.info("UPDATING DOCUMENTS...")
    logger.info("=" * 80)

    total_processed = 0
    total_updated = 0

    cursor = (
        collection.find(
            {"request.body.model": "basic_logic", "response_blob": None},
            {"_id": 1, "created_at": 1},
        )
        .hint([("request.body.model", 1)])
        .batch_size(FETCH_BATCH_SIZE)
    )

    update_operations = []

    async for doc in cursor:
        total_processed += 1

        doc_id = doc["_id"]
        created_at = doc.get("created_at", datetime.now())

        # Create update operation
        update_operations.append(
            UpdateOne(
                {"_id": doc_id},
                {
                    "$set": {
                        "batch_id": "empty_unmapped_unknowns",
                        "response_blob": get_dummy_response_blob(created_at),
                    }
                },
            )
        )

        # Execute batch updates when we reach the batch size
        if len(update_operations) >= UPDATE_BATCH_SIZE:
            result = await collection.bulk_write(update_operations, ordered=False)
            total_updated += result.modified_count
            logger.info(
                f"Progress: Processed {total_processed} documents, "
                f"updated {total_updated} so far"
            )
            update_operations = []

    # Execute remaining operations
    if update_operations:
        result = await collection.bulk_write(update_operations, ordered=False)
        total_updated += result.modified_count

    logger.info(
        f"\nFinal: Processed {total_processed} documents, updated {total_updated} total"
    )

    return total_processed, total_updated


async def main():
    await init_db()
    logger.info("Database initialized.\n")

    collection = GPTBatchRequest.get_pymongo_collection()

    # Count documents with model="basic_logic"
    logger.info("=" * 80)
    logger.info("ANALYZING DATABASE...")
    logger.info("=" * 80)

    total_count = await count_basic_logic_requests(collection)

    if total_count == 0:
        logger.info("\n✅ No documents found with model='basic_logic'.")
        return

    # Display summary
    logger.info("\n" + "=" * 80)
    logger.info("IMPACT SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Total documents to update: {total_count:,}")
    logger.info("  - Will set batch_id to 'empty_unmapped_unknowns'")
    logger.info("  - Will add dummy response_blob structure")
    logger.info(f"  - Fetch batch size: {FETCH_BATCH_SIZE:,}")
    logger.info(f"  - Update batch size: {UPDATE_BATCH_SIZE:,}")
    logger.info("=" * 80)

    # Ask for confirmation
    logger.info("\n⚠️  WARNING: This operation will update the database!")

    response = input("\nDo you want to proceed? (yes/no): ").strip().lower()

    if response not in ["yes", "y"]:
        logger.info("\n❌ Operation cancelled by user.")
        return

    logger.info("\n✅ Proceeding with updates...\n")

    # Perform chunked bulk updates
    processed, updated = await update_basic_logic_requests(collection)

    # Print final summary
    logger.info("\n" + "=" * 80)
    logger.info("UPDATE SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Total documents processed: {processed:,}")
    logger.info(f"Total documents updated: {updated:,}")
    logger.info("=" * 80)

    logger.info("\n" + "=" * 80)
    logger.info("✅ OPERATION COMPLETED SUCCESSFULLY!")
    logger.info("=" * 80)


if __name__ == "__main__":
    asyncio.run(main())
