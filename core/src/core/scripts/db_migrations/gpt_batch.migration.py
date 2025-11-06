#!/usr/bin/env python3
"""
Migration script to set processing_completed_at for existing GPT batch documents.

This migration:
1. Finds all GPT batch documents where processing_completed_at is None
2. Sets processing_completed_at to the current timestamp for batches that have completed/failed/expired
"""

import asyncio
import logging
from datetime import datetime
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

from core.models.db.gpt_batch import GPTBatch, GPTBatchStatus
from core.utils.mongo_client import init_db

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


async def migrate_gpt_batches():
    """
    Migrate existing GPT batch documents to set processing_completed_at.
    """
    print("Starting migration of GPT batch documents...")
    collection = GPTBatch.get_pymongo_collection()

    total_docs = await collection.count_documents({})
    print(f"Total GPT batch documents: {total_docs}")

    # Find batches that should have processing_completed_at set
    # (i.e., batches in a terminal state but missing the field)
    query = {
        "$and": [
            {
                "$or": [
                    {"processing_completed_at": {"$exists": False}},
                    {"processing_completed_at": None},
                ]
            },
            {
                "status": {
                    "$in": [
                        GPTBatchStatus.COMPLETED.value,
                        GPTBatchStatus.FAILED.value,
                        GPTBatchStatus.EXPIRED.value,
                    ]
                }
            },
        ]
    }

    docs_to_update = await collection.count_documents(query)
    print(f"Documents to update: {docs_to_update}")

    if docs_to_update == 0:
        print("No documents need migration.")
        return

    cursor = collection.find(query)
    bulk_operations = []
    batch_size = 1000
    total_updated = 0
    total_failed = 0
    current_time = datetime.utcnow()

    async for doc in cursor:
        print(
            f"Processing batch {doc.get('external_batch_id')} "
            f"(status: {doc.get('status')}, api_key: {doc.get('api_key_label')})"
        )

        # Determine the appropriate timestamp to use
        # Prefer using the timestamp from the status field if available
        timestamp = current_time
        if doc.get("status") == GPTBatchStatus.COMPLETED.value and doc.get(
            "completed_at"
        ):
            timestamp = doc["completed_at"]
        elif doc.get("status") == GPTBatchStatus.FAILED.value and doc.get("failed_at"):
            timestamp = doc["failed_at"]
        elif doc.get("status") == GPTBatchStatus.EXPIRED.value and doc.get(
            "expired_at"
        ):
            timestamp = doc["expired_at"]

        bulk_operations.append(
            UpdateOne(
                {"_id": doc["_id"]},
                {"$set": {"processing_completed_at": timestamp}},
            )
        )

        # Execute bulk operation when batch size is reached
        if len(bulk_operations) >= batch_size:
            print(f"Executing batch of {len(bulk_operations)} operations...")
            try:
                result = await collection.bulk_write(bulk_operations)
                total_updated += result.modified_count
                total_failed += len(bulk_operations) - result.modified_count
                print(
                    f"Batch complete: {result.modified_count} updated, "
                    f"Total so far: {total_updated} updated, {total_failed} failed"
                )
                bulk_operations = []
            except BulkWriteError as bwe:
                logger.error(f"Bulk write error: {bwe.details}")
                for err in bwe.details.get("writeErrors", [])[:5]:
                    print(
                        f"Error index: {err['index']}, "
                        f"errmsg: {err['errmsg']}, "
                        f"errInfo: {err.get('errInfo')}"
                    )
                total_failed += len(bulk_operations)
                bulk_operations = []
            except Exception as e:
                logger.error(f"Unexpected error during bulk write: {e}")
                total_failed += len(bulk_operations)
                bulk_operations = []

    # Execute remaining operations
    if bulk_operations:
        print(f"Executing final batch of {len(bulk_operations)} operations...")
        try:
            result = await collection.bulk_write(bulk_operations)
            total_updated += result.modified_count
            total_failed += len(bulk_operations) - result.modified_count
            print(f"Final batch complete: {result.modified_count} updated")
        except BulkWriteError as bwe:
            logger.error(f"Final bulk write error: {bwe.details}")
            for err in bwe.details.get("writeErrors", [])[:5]:
                print(
                    f"Error index: {err['index']}, "
                    f"errmsg: {err['errmsg']}, "
                    f"errInfo: {err.get('errInfo')}"
                )
            total_failed += len(bulk_operations)
        except Exception as e:
            logger.error(f"Final unexpected error: {e}")
            total_failed += len(bulk_operations)

    print(
        f"\nMigration complete!\n"
        f"Total updated: {total_updated}\n"
        f"Total failed: {total_failed}\n"
        f"Total processed: {total_updated + total_failed}"
    )


async def main():
    await init_db()
    print("Database initialized.")
    await migrate_gpt_batches()


if __name__ == "__main__":
    asyncio.run(main())
