import asyncio
import logging
import argparse
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

from core.models.db.gpt_batch_request import GPTBatchRequest
from core.utils.mongo_client import init_db

logger = logging.getLogger(__name__)


async def iterate(limit=None, skip_confirmation=False):
    print("Starting migration of GPTBatchRequest documents...")
    collection = GPTBatchRequest.get_pymongo_collection()

    # Query all documents
    query_filter = {}

    total = await collection.count_documents(query_filter)
    print(f"Total documents to update: {total}")

    # Ask for confirmation unless --yes flag is provided
    if not skip_confirmation:
        if total == 0:
            print("No documents to update. Exiting.")
            return

        response = (
            input(
                f"\nDo you want to proceed with updating {total} documents? (yes/no): "
            )
            .strip()
            .lower()
        )
        if response not in ["yes", "y"]:
            print("Migration cancelled by user.")
            return
        print("Proceeding with migration...")

    # Apply limit if specified
    if limit:
        print(f"Limiting to {limit} documents")
        cursor = collection.find(query_filter).limit(limit)
    else:
        cursor = collection.find(query_filter)

    bulk_operations = []
    batch_size = 20_000
    total_count = 0
    processed = 0

    async for doc in cursor:
        processed += 1
        if processed % 100 == 0:
            print(
                f"Processing document {processed}/{min(limit, total) if limit else total}"
            )

        update_fields = {}

        # Set updated_at to created_at for all documents
        if "created_at" in doc:
            update_fields["updated_at"] = doc["created_at"]

        # Set num_batches_paired_with based on batch_id
        if doc.get("batch_id") is not None:
            update_fields["num_batches_paired_with"] = 1
        else:
            update_fields["num_batches_paired_with"] = 0

        if update_fields:
            bulk_operations.append(
                UpdateOne({"_id": doc["_id"]}, {"$set": update_fields}, upsert=False)
            )

        # Execute bulk operation when batch size is reached
        if len(bulk_operations) >= batch_size:
            print(f"Executing batch of {len(bulk_operations)} update operations...")
            try:
                result = await collection.bulk_write(bulk_operations, ordered=False)
                total_count += result.modified_count
                print(
                    f"Batch complete: {result.modified_count} documents updated (Total: {total_count})"
                )
                bulk_operations = []
            except BulkWriteError as bwe:
                logger.error(f"Bulk write error: {bwe.details}")
                total_count += bwe.details.get("nModified", 0)
                bulk_operations = []
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                bulk_operations = []

    # Execute remaining operations
    if bulk_operations:
        print(f"Executing final batch of {len(bulk_operations)} update operations...")
        try:
            result = await collection.bulk_write(bulk_operations, ordered=False)
            total_count += result.modified_count
            print(f"Final batch complete: {result.modified_count} documents updated")
        except BulkWriteError as bwe:
            logger.error(f"Final bulk write error: {bwe.details}")
            total_count += bwe.details.get("nModified", 0)
        except Exception as e:
            logger.error(f"Unexpected final error: {e}")

    print(f"\nMigration complete: {total_count} documents updated successfully.")


async def main():
    parser = argparse.ArgumentParser(
        description="Update GPTBatchRequest documents: set updated_at=created_at for all, and num_batches_paired_with=1 for docs with batch_id"
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Limit the number of documents to process",
        default=None,
    )
    parser.add_argument(
        "--yes",
        "-y",
        action="store_true",
        help="Skip confirmation prompt and proceed with migration",
        default=False,
    )
    args = parser.parse_args()

    await init_db()
    print("Database initialized.")
    await iterate(limit=args.limit, skip_confirmation=args.yes)


if __name__ == "__main__":
    asyncio.run(main())
