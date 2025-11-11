import asyncio
import logging
import argparse
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

from core.models.db.manufacturer import Manufacturer
from core.models.db.deferred_manufacturer import DeferredManufacturer
from core.utils.mongo_client import init_db

logger = logging.getLogger(__name__)


async def update_deferred_manufacturer_tokens(limit=None, mfg_etld1=None):
    """
    Read manufacturers, find linked deferred manufacturers, and update their scraped_text_file_num_tokens.
    Uses bulk operations and skips Pydantic models for efficiency.
    """
    print("Starting update of DeferredManufacturer scraped_text_file_num_tokens...")

    mfg_collection = Manufacturer.get_pymongo_collection()
    df_mfg_collection = DeferredManufacturer.get_pymongo_collection()

    # Build query filter for Manufacturers
    query_filter = {"scraped_text_file_num_tokens": {"$exists": True}}
    if mfg_etld1:
        query_filter["etld1"] = mfg_etld1
        print(f"Filtering by etld1: {mfg_etld1}")

    total = await mfg_collection.count_documents(query_filter)
    print(f"Total manufacturers matching filter: {total}")

    # Apply limit if specified
    if limit:
        print(f"Limiting to {limit} manufacturers")
        cursor = mfg_collection.find(
            query_filter,
            projection={
                "etld1": 1,
                "scraped_text_file_version_id": 1,
                "scraped_text_file_num_tokens": 1,
            },
        ).limit(limit)
    else:
        cursor = mfg_collection.find(
            query_filter,
            projection={
                "etld1": 1,
                "scraped_text_file_version_id": 1,
                "scraped_text_file_num_tokens": 1,
            },
        )

    bulk_operations = []
    batch_size = 1000
    total_updated = 0
    total_failed = 0
    total_not_found = 0
    processed = 0

    async for mfg_doc in cursor:
        processed += 1
        etld1 = mfg_doc.get("etld1")
        scraped_text_file_version_id = mfg_doc.get("scraped_text_file_version_id")
        scraped_text_file_num_tokens = mfg_doc.get("scraped_text_file_num_tokens")

        if processed % 100 == 0:
            print(f"Processing manufacturer {processed}/{total}: {etld1}")

        if not scraped_text_file_version_id or scraped_text_file_num_tokens is None:
            continue

        # Create bulk update operation
        bulk_operations.append(
            UpdateOne(
                {
                    "mfg_etld1": etld1,
                    "scraped_text_file_version_id": scraped_text_file_version_id,
                },
                {
                    "$set": {
                        "scraped_text_file_num_tokens": scraped_text_file_num_tokens
                    }
                },
            )
        )

        # Execute bulk operation when batch size is reached
        if len(bulk_operations) >= batch_size:
            logger.info(f"Executing batch of {len(bulk_operations)} operations...")
            try:
                result = await df_mfg_collection.bulk_write(bulk_operations)
                total_updated += result.modified_count
                # Count operations that didn't match any document
                total_not_found += len(bulk_operations) - result.matched_count
                logger.info(
                    f"Batch complete: {result.matched_count} matched, "
                    f"{result.modified_count} modified, "
                    f"{len(bulk_operations) - result.matched_count} not found"
                )
                bulk_operations = []
            except BulkWriteError as bwe:
                logger.error(f"Bulk write error: {bwe.details}")
                for err in bwe.details.get("writeErrors", [])[:5]:
                    logger.error(
                        f"Error index: {err['index']}, errmsg: {err['errmsg']}, "
                        f"errInfo: {err.get('errInfo')}"
                    )
                total_failed += len(bulk_operations)
                bulk_operations = []
            except Exception as e:
                logger.error(f"Bulk write error: {e}")
                total_failed += len(bulk_operations)
                bulk_operations = []

    # Execute remaining operations
    if bulk_operations:
        logger.info(f"Executing final batch of {len(bulk_operations)} operations...")
        try:
            result = await df_mfg_collection.bulk_write(bulk_operations)
            total_updated += result.modified_count
            total_not_found += len(bulk_operations) - result.matched_count
            logger.info(
                f"Final batch complete: {result.matched_count} matched, "
                f"{result.modified_count} modified, "
                f"{len(bulk_operations) - result.matched_count} not found"
            )
        except BulkWriteError as bwe:
            logger.error(f"Final bulk write error: {bwe.details}")
            for err in bwe.details.get("writeErrors", [])[:5]:
                logger.error(
                    f"Error index: {err['index']}, errmsg: {err['errmsg']}, "
                    f"errInfo: {err.get('errInfo')}"
                )
            total_failed += len(bulk_operations)
        except Exception as e:
            logger.error(f"Final bulk write error: {e}")
            total_failed += len(bulk_operations)

    print("\n" + "=" * 70)
    print("UPDATE COMPLETE")
    print("=" * 70)
    print(f"Manufacturers processed: {processed}")
    print(f"DeferredManufacturers updated: {total_updated}")
    print(f"DeferredManufacturers not found: {total_not_found}")
    print(f"Failed operations: {total_failed}")
    print("=" * 70)


async def main():
    parser = argparse.ArgumentParser(
        description="Update DeferredManufacturer scraped_text_file_num_tokens from linked Manufacturers"
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--limit",
        type=int,
        help="Limit the number of manufacturers to process",
        default=None,
    )
    group.add_argument(
        "--mfg-etld1",
        type=str,
        help="Process only the manufacturer with this specific etld1",
        default=None,
    )
    args = parser.parse_args()

    await init_db()
    print("Database initialized.")
    await update_deferred_manufacturer_tokens(
        limit=args.limit, mfg_etld1=args.mfg_etld1
    )


if __name__ == "__main__":
    asyncio.run(main())
