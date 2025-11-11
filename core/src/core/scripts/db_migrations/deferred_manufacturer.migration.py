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

from core.models.db.deferred_manufacturer import DeferredManufacturer
from core.utils.mongo_client import init_db

logger = logging.getLogger(__name__)


def fix_chunk_keys_in_material_caps(field_data):
    """
    Fix llm_search_request_id values in material_caps field.
    Replace '>materials>' with '>material_caps>' in llm_search_request_id.

    Example: 1a.tools>materials>llm_search>chunk>0:18706
          -> 1a.tools>material_caps>llm_search>chunk>0:18706
    """
    if not field_data or "chunk_request_bundle_map" not in field_data:
        return field_data, False

    chunk_map = field_data["chunk_request_bundle_map"]
    modified = False

    for chunk_key, bundle in chunk_map.items():
        if "llm_search_request_id" in bundle:
            llm_search_id = bundle["llm_search_request_id"]
            if ">materials>" in llm_search_id:
                new_id = llm_search_id.replace(">materials>", ">material_caps>")
                bundle["llm_search_request_id"] = new_id
                modified = True
                print(f"    Chunk {chunk_key}: '{llm_search_id}' -> '{new_id}'")

    return field_data, modified


def fix_chunk_keys_in_process_caps(field_data):
    """
    Fix llm_search_request_id values in process_caps field.
    Replace '>processes>' with '>process_caps>' in llm_search_request_id.

    Example: 1a.tools>processes>llm_search>chunk>0:9573
          -> 1a.tools>process_caps>llm_search>chunk>0:9573
    """
    if not field_data or "chunk_request_bundle_map" not in field_data:
        return field_data, False

    chunk_map = field_data["chunk_request_bundle_map"]
    modified = False

    for chunk_key, bundle in chunk_map.items():
        if "llm_search_request_id" in bundle:
            llm_search_id = bundle["llm_search_request_id"]
            if ">processes>" in llm_search_id:
                new_id = llm_search_id.replace(">processes>", ">process_caps>")
                bundle["llm_search_request_id"] = new_id
                modified = True
                print(f"    Chunk {chunk_key}: '{llm_search_id}' -> '{new_id}'")

    return field_data, modified


async def iterate(limit=None, mfg_etld1=None):
    print("Starting migration of DeferredManufacturer chunk keys...")
    collection = DeferredManufacturer.get_pymongo_collection()

    # Build query filter
    query_filter = {}
    if mfg_etld1:
        query_filter["mfg_etld1"] = mfg_etld1
        print(f"Filtering by mfg_etld1: {mfg_etld1}")

    total = await collection.count_documents(query_filter)
    print(f"Total documents matching filter: {total}")

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
        mfg_etld1_value = doc.get("mfg_etld1")
        print(
            f"Processing document {processed}/{min(limit, total) if limit else total} with mfg_etld1: {mfg_etld1_value}"
        )

        update_fields = {}
        doc_updated = False

        # Fix material_caps chunk keys
        if "material_caps" in doc and doc["material_caps"] is not None:
            material_caps_data = doc["material_caps"]
            updated_material_caps, modified = fix_chunk_keys_in_material_caps(
                material_caps_data
            )
            if modified:
                update_fields["material_caps"] = updated_material_caps
                doc_updated = True
                print(f"  Updated material_caps chunk keys")

        # Fix process_caps chunk keys
        if "process_caps" in doc and doc["process_caps"] is not None:
            process_caps_data = doc["process_caps"]
            updated_process_caps, modified = fix_chunk_keys_in_process_caps(
                process_caps_data
            )
            if modified:
                update_fields["process_caps"] = updated_process_caps
                doc_updated = True
                print(f"  Updated process_caps chunk keys")

        if doc_updated:
            bulk_operations.append(
                UpdateOne({"_id": doc["_id"]}, {"$set": update_fields})
            )

        # Execute bulk operation when batch size is reached
        if len(bulk_operations) >= batch_size:
            print(f"Executing batch of {len(bulk_operations)} update operations...")
            try:
                result = await collection.bulk_write(bulk_operations, ordered=True)
                total_count += result.modified_count
                print(
                    f"Batch complete: {result.modified_count} documents updated (Total: {total_count})"
                )
                bulk_operations = []
            except BulkWriteError as bwe:
                logger.error(f"Bulk write error: {bwe.details}")
                for err in bwe.details.get("writeErrors", [])[:5]:
                    print(
                        f"Error index: {err['index']}, errmsg: {err['errmsg']}, errInfo: {err.get('errInfo')}"
                    )
                raise  # Stop on first error
            except Exception as e:
                logger.error(f"Bulk write error: {e}")
                raise  # Stop on first error

    # Execute remaining operations
    if bulk_operations:
        print(f"Executing final batch of {len(bulk_operations)} update operations...")
        try:
            result = await collection.bulk_write(bulk_operations, ordered=True)
            total_count += result.modified_count
            print(f"Final batch complete: {result.modified_count} documents updated")
        except BulkWriteError as bwe:
            logger.error(f"Final bulk write error: {bwe.details}")
            for err in bwe.details.get("writeErrors", [])[:5]:
                print(
                    f"Error index: {err['index']}, errmsg: {err['errmsg']}, errInfo: {err.get('errInfo')}"
                )
            raise  # Stop on first error
        except Exception as e:
            logger.error(f"Final bulk write error: {e}")
            raise  # Stop on first error

    print(f"\nMigration complete: {total_count} documents updated successfully.")


async def main():
    parser = argparse.ArgumentParser(
        description="Fix chunk_request_bundle_map keys in material_caps and process_caps fields"
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
        help="Process only the document with this specific mfg_etld1",
        default=None,
    )
    args = parser.parse_args()

    await init_db()
    print("Database initialized.")
    await iterate(limit=args.limit, mfg_etld1=args.mfg_etld1)


if __name__ == "__main__":
    asyncio.run(main())
