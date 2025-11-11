#!/usr/bin/env python3
"""
Migration script to update existing GPT batch documents.

This migration:
1. Sets processing_completed_at to null for all GPT batches
"""

import asyncio
import logging
from pymongo.errors import BulkWriteError

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
from core.utils.mongo_client import init_db

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


async def migrate_gpt_batches():
    """
    Set processing_completed_at to null for all GPT batch documents.
    """
    print("=" * 80)
    print("GPT Batch Migration Script")
    print("=" * 80)
    print("\nAnalyzing documents that need migration...\n")

    collection = GPTBatch.get_pymongo_collection()

    total_docs = await collection.count_documents({})
    print(f"Total GPT batch documents: {total_docs}")

    print("\n" + "-" * 80)
    print("Migration Summary:")
    print("-" * 80)
    print(
        f"This migration will set processing_completed_at to null for all {total_docs} documents"
    )
    print("-" * 80)

    if total_docs == 0:
        print("\n✓ No documents found.")
        return

    # Ask for confirmation
    print("\n" + "=" * 80)
    response = (
        input("Do you want to continue with the migration? (yes/no): ").strip().lower()
    )

    if response not in ["yes", "y"]:
        print("\n✗ Migration cancelled by user.")
        return

    print("\n" + "=" * 80)
    print("Starting migration...")
    print("=" * 80 + "\n")

    try:
        result = await collection.update_many(
            {}, {"$set": {"processing_completed_at": None}}
        )

        print("\n" + "=" * 80)
        print("Migration Complete!")
        print("=" * 80)
        print(f"Total documents matched: {result.matched_count}")
        print(f"Total documents updated: {result.modified_count}")
        print("=" * 80 + "\n")
    except Exception as e:
        logger.error(f"Error during migration: {e}")
        print(f"\n✗ Migration failed: {e}\n")


async def main():
    await init_db()
    print("Database initialized.")
    await migrate_gpt_batches()


if __name__ == "__main__":
    asyncio.run(main())
