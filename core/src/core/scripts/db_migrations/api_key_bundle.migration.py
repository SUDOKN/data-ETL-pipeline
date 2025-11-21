#!/usr/bin/env python3
"""
Migration script to update APIKeyBundle documents.

This migration:
- Resets available_at to the current timestamp for all existing documents
"""

import asyncio
import logging
from datetime import datetime, UTC, timedelta

from core.dependencies.load_core_env import load_core_env
from scraper_app.dependencies.load_scraper_env import load_scraper_env
from open_ai_key_app.dependencies.load_open_ai_app_env import load_open_ai_app_env
from data_etl_app.dependencies.load_data_etl_env import load_data_etl_env

# Load environment variables
load_core_env()
load_scraper_env()
load_data_etl_env()
load_open_ai_app_env()

from core.models.db.api_key_bundle import APIKeyBundle
from core.utils.mongo_client import init_db

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


async def migrate_api_key_bundles():
    """
    Migrate existing APIKeyBundle documents to reset available_at to current timestamp.
    """
    print("Starting migration of APIKeyBundle documents...")
    collection = APIKeyBundle.get_pymongo_collection()

    total_docs = await collection.count_documents({})
    print(f"Total APIKeyBundle documents: {total_docs}")

    if total_docs == 0:
        print("No documents need migration.")
        return

    current_timestamp = datetime.now(UTC) + timedelta(minutes=5)
    print(f"Setting available_at to: {current_timestamp}")

    # Update all documents
    result = await collection.update_many({}, {"$set": {"exhausted": True}})

    print(
        f"\nMigration complete!\n"
        f"Documents matched: {result.matched_count}\n"
        f"Documents modified: {result.modified_count}"
    )


async def main():
    await init_db()
    print("Database initialized.")
    await migrate_api_key_bundles()


if __name__ == "__main__":
    asyncio.run(main())
