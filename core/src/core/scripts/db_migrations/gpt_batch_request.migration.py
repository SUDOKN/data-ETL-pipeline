import asyncio
import logging
import argparse
import csv
from datetime import datetime
from pathlib import Path
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


def get_priority_score(doc):
    """
    Calculate priority score for a document.
    Higher score = higher priority to keep.

    Priority:
    1. Has both batch_id and response_blob (score: 3)
    2. Has batch_id but no response_blob (score: 2)
    3. Has neither (score: 1, tiebreak by created_at - newer is better)
    """
    has_batch_id = doc.get("batch_id") is not None
    has_response_blob = doc.get("response_blob") is not None

    if has_batch_id and has_response_blob:
        return (3, doc.get("created_at", datetime.min))
    elif has_batch_id:
        return (2, doc.get("created_at", datetime.min))
    else:
        return (1, doc.get("created_at", datetime.min))


def fix_custom_id(custom_id):
    """
    Fix custom_id by replacing:
    - '>materials>' with '>material_caps>'
    - '>processes>' with '>process_caps>'

    Returns: (new_custom_id, was_modified)
    """
    if not custom_id:
        return custom_id, False

    original = custom_id
    modified = False

    if ">materials>" in custom_id:
        custom_id = custom_id.replace(">materials>", ">material_caps>")
        modified = True

    if ">processes>" in custom_id:
        custom_id = custom_id.replace(">processes>", ">process_caps>")
        modified = True

    if modified:
        print(f"    '{original}' -> '{custom_id}'")

    return custom_id, modified


def extract_mfg_etld1(custom_id):
    """Extract mfg_etld1 from custom_id (substring before first '>')"""
    if not custom_id:
        return None
    parts = custom_id.split(">")
    return parts[0] if parts else None


def extract_mfg_etld1(custom_id):
    """Extract mfg_etld1 from custom_id (substring before first '>')"""
    if not custom_id:
        return None
    parts = custom_id.split(">")
    return parts[0] if parts else None


async def iterate(limit=None, mfg_etld1=None, skip_confirmation=False):
    print("Starting migration of GPTBatchRequest custom_ids...")
    collection = GPTBatchRequest.get_pymongo_collection()

    # Prepare CSV files for error logging
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    duplicate_errors_csv = f"duplicate_key_errors_{timestamp}.csv"
    other_errors_csv = f"other_errors_{timestamp}.csv"

    duplicate_etld1s = set()
    other_errors = []

    # Build query filter - only process documents that have the old patterns
    query_filter = {
        "$or": [
            {"request.custom_id": {"$regex": ">materials>"}},
            {"request.custom_id": {"$regex": ">processes>"}},
            {"response_blob.request_custom_id": {"$regex": ">materials>"}},
            {"response_blob.request_custom_id": {"$regex": ">processes>"}},
        ]
    }

    # Add mfg_etld1 filter if specified
    if mfg_etld1:
        # Match custom_id that starts with "mfg_etld1>"
        mfg_filter = {
            "$or": [
                {"request.custom_id": {"$regex": f"^{mfg_etld1}>"}},
                {"response_blob.request_custom_id": {"$regex": f"^{mfg_etld1}>"}},
            ]
        }
        # Combine with existing filter using $and
        query_filter = {"$and": [query_filter, mfg_filter]}
        print(f"Filtering by mfg_etld1: {mfg_etld1}")

    total = await collection.count_documents(query_filter)
    print(f"Total documents matching filter: {total}")

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
    batch_size = 1000
    total_count = 0
    processed = 0

    async for doc in cursor:
        processed += 1
        print(
            f"Processing document {processed}/{min(limit, total) if limit else total}"
        )

        update_fields = {}
        doc_updated = False

        # Fix request.custom_id
        if "request" in doc and "custom_id" in doc["request"]:
            old_custom_id = doc["request"]["custom_id"]
            new_custom_id, modified = fix_custom_id(old_custom_id)
            if modified:
                update_fields["request.custom_id"] = new_custom_id
                doc_updated = True
                print(f"  Updated request.custom_id")

        # Fix response_blob.request_custom_id (if response_blob exists)
        if "response_blob" in doc and doc["response_blob"] is not None:
            if "request_custom_id" in doc["response_blob"]:
                old_custom_id = doc["response_blob"]["request_custom_id"]
                new_custom_id, modified = fix_custom_id(old_custom_id)
                if modified:
                    update_fields["response_blob.request_custom_id"] = new_custom_id
                    doc_updated = True
                    print(f"  Updated response_blob.request_custom_id")

        if doc_updated:
            bulk_operations.append(
                UpdateOne({"_id": doc["_id"]}, {"$set": update_fields})
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
                # Process write errors
                for err in bwe.details.get("writeErrors", []):
                    if err.get("code") == 11000:  # Duplicate key error
                        # Extract custom_id from error
                        key_value = err.get("keyValue", {})
                        custom_id = key_value.get("request.custom_id")
                        etld1 = extract_mfg_etld1(custom_id)
                        if etld1:
                            duplicate_etld1s.add(etld1)
                        print(
                            f"  Duplicate key error for: {custom_id} (etld1: {etld1})"
                        )
                    else:
                        # Other error
                        op = err.get("op", {})
                        update_data = op.get("u", {}).get("$set", {})
                        custom_id = update_data.get(
                            "request.custom_id"
                        ) or update_data.get("response_blob.request_custom_id")
                        etld1 = extract_mfg_etld1(custom_id)
                        error_msg = err.get("errmsg", "Unknown error")
                        other_errors.append(
                            {
                                "etld1": etld1,
                                "error": error_msg,
                                "code": err.get("code"),
                            }
                        )
                        print(f"  Error for {custom_id} (etld1: {etld1}): {error_msg}")

                # Count successful operations in this batch
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
            # Process write errors
            for err in bwe.details.get("writeErrors", []):
                if err.get("code") == 11000:  # Duplicate key error
                    key_value = err.get("keyValue", {})
                    custom_id = key_value.get("request.custom_id")
                    etld1 = extract_mfg_etld1(custom_id)
                    if etld1:
                        duplicate_etld1s.add(etld1)
                    print(f"  Duplicate key error for: {custom_id} (etld1: {etld1})")
                else:
                    op = err.get("op", {})
                    update_data = op.get("u", {}).get("$set", {})
                    custom_id = update_data.get("request.custom_id") or update_data.get(
                        "response_blob.request_custom_id"
                    )
                    etld1 = extract_mfg_etld1(custom_id)
                    error_msg = err.get("errmsg", "Unknown error")
                    other_errors.append(
                        {"etld1": etld1, "error": error_msg, "code": err.get("code")}
                    )
                    print(f"  Error for {custom_id} (etld1: {etld1}): {error_msg}")

            # Count successful operations
            total_count += bwe.details.get("nModified", 0)
        except Exception as e:
            logger.error(f"Unexpected final error: {e}")

    print(f"\nMigration complete: {total_count} documents updated successfully.")

    # Write duplicate key errors to CSV
    if duplicate_etld1s:
        with open(duplicate_errors_csv, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["mfg_etld1"])
            for etld1 in sorted(duplicate_etld1s):
                writer.writerow([etld1])
        print(f"\n✓ Duplicate key errors logged to: {duplicate_errors_csv}")
        print(f"  Total unique etld1s with duplicate errors: {len(duplicate_etld1s)}")

    # Write other errors to CSV
    if other_errors:
        with open(other_errors_csv, "w", newline="") as f:
            writer = csv.DictWriter(
                f, fieldnames=["mfg_etld1", "error_code", "error_message"]
            )
            writer.writeheader()
            for err in other_errors:
                writer.writerow(
                    {
                        "mfg_etld1": err["etld1"],
                        "error_code": err["code"],
                        "error_message": err["error"],
                    }
                )
        print(f"\n✓ Other errors logged to: {other_errors_csv}")
        print(f"  Total other errors: {len(other_errors)}")


async def main():
    parser = argparse.ArgumentParser(
        description="Fix custom_id fields in GPTBatchRequest documents by replacing >materials> and >processes>"
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--limit",
        type=int,
        help="Limit the number of documents to process",
        default=None,
    )
    group.add_argument(
        "--mfg-etld1",
        type=str,
        help="Process only documents with custom_id starting with this mfg_etld1",
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
    await iterate(
        limit=args.limit, mfg_etld1=args.mfg_etld1, skip_confirmation=args.yes
    )


if __name__ == "__main__":
    asyncio.run(main())
