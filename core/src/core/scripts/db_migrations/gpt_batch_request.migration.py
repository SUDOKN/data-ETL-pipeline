#!/usr/bin/env python3
"""
Migration script to remove deprecated fields from GPT batch request documents.

This migration:
1. Finds all GPT batch request documents with deprecated fields
2. Removes 'request_sent_at' and 'response_received_at' fields
"""

import asyncio
import logging
from pymongo import UpdateOne
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

from core.models.db.gpt_batch_request import GPTBatchRequest
from core.utils.mongo_client import init_db

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


async def migrate_gpt_batch_requests():
    """
    Remove deprecated fields 'request_sent_at' and 'response_received_at'
    from GPT batch request documents.
    """
    print("Starting migration of GPT batch request documents...")
    collection = GPTBatchRequest.get_pymongo_collection()

    # total_docs = await collection.count_documents({})
    # print(f"Total GPT batch request documents: {total_docs}")

    # Remove deprecated fields from all documents in one shot
    print("Removing 'request_sent_at' and 'response_received_at' fields...")
    try:
        result = await collection.update_many(
            {},  # Match all documents
            {
                "$unset": {
                    "request_sent_at": "",
                    "response_received_at": "",
                    "response_blob.response.result": "",  # Remove computed field
                }
            },
        )
        print(
            f"\nMigration complete!\n"
            f"Documents matched: {result.matched_count}\n"
            f"Documents modified: {result.modified_count}"
        )
    except Exception as e:
        logger.error(f"Error during migration: {e}", exc_info=True)
        raise


async def main():
    await init_db()
    print("Database initialized.")
    await migrate_gpt_batch_requests()


if __name__ == "__main__":
    asyncio.run(main())
