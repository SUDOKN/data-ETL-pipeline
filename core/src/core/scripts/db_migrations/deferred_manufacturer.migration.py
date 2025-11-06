import asyncio
import logging
import argparse
from pymongo import ReplaceOne
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


def migrate_binary_classification(field_data):
    """
    Migrate binary classification fields (is_manufacturer, is_contract_manufacturer, is_product_manufacturer)
    Remove deferred_stats wrapper and rename chunk_batch_request_id_map to chunk_request_id_map
    """
    if not field_data or "deferred_stats" not in field_data:
        return field_data

    stats = field_data["deferred_stats"]
    migrated = {
        "prompt_version_id": stats["prompt_version_id"],
        "final_chunk_key": stats["final_chunk_key"],
        "chunk_request_id_map": stats["chunk_batch_request_id_map"],
    }
    return migrated


def migrate_basic_extraction_array(field_data):
    """
    Migrate addresses field from array of strings to DeferredBasicExtraction object
    """
    if not field_data or not isinstance(field_data, list) or len(field_data) == 0:
        return field_data

    return {
        "prompt_version_id": "PF7oLk38eu5SnTc20JiZfWOPMO1czNJC",
        "gpt_request_id": field_data[0],
    }


def migrate_basic_extraction_string(field_data):
    """
    Migrate business_desc field from string to DeferredBasicExtraction object
    """
    if not field_data or not isinstance(field_data, str):
        return field_data

    return {
        "prompt_version_id": "oGmkYFTGmRhl3mWzpkmB9pWjrnhY6YyP",
        "gpt_request_id": field_data,
    }


def migrate_keyword_extraction(field_data):
    """
    Migrate products field
    Remove deferred_stats wrapper and rename chunk_batch_request_id_map to chunk_request_id_map
    """
    if not field_data or "deferred_stats" not in field_data:
        return field_data

    stats = field_data["deferred_stats"]
    migrated = {
        "extract_prompt_version_id": stats["extract_prompt_version_id"],
        "chunk_request_id_map": stats["chunk_batch_request_id_map"],
    }
    return migrated


def migrate_concept_extraction(field_data):
    """
    Migrate concept extraction fields (certificates, industries, process_caps, material_caps)
    Remove deferred_stats wrapper, rename chunked_stats_batch_request_map to chunk_request_bundle_map,
    rename llm_batch_request_id to llm_search_request_id in bundles,
    remove mapping_batch_request_id from bundles, add llm_mapping_request_id: None at top level
    """
    if not field_data or "deferred_stats" not in field_data:
        return field_data

    stats = field_data["deferred_stats"]

    # Migrate chunk bundles
    chunk_request_bundle_map = {}
    for chunk_key, bundle in stats.get("chunked_stats_batch_request_map", {}).items():
        migrated_bundle = {
            "brute": bundle["brute"],
            "llm_search_request_id": bundle["llm_batch_request_id"],
            # Note: mapping_batch_request_id is intentionally deleted
        }
        chunk_request_bundle_map[chunk_key] = migrated_bundle

    migrated = {
        "extract_prompt_version_id": stats["extract_prompt_version_id"],
        "map_prompt_version_id": stats["map_prompt_version_id"],
        "ontology_version_id": stats["ontology_version_id"],
        "chunk_request_bundle_map": chunk_request_bundle_map,
        "llm_mapping_request_id": None,
    }
    return migrated


async def iterate(limit=None, mfg_etld1=None):
    print("Starting migration of DeferredManufacturer documents...")
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
    failed = 0
    processed = 0

    async for doc in cursor:
        updated = False
        processed += 1
        print(
            f"Processing document {processed}/{total} with mfg_etld1: {doc.get('mfg_etld1')}"
        )

        # Migrate binary classification fields
        for field in [
            "is_manufacturer",
            "is_contract_manufacturer",
            "is_product_manufacturer",
        ]:
            if field in doc and doc[field] is not None:
                migrated = migrate_binary_classification(doc[field])
                if migrated != doc[field]:
                    doc[field] = migrated
                    updated = True
                    print(f"  Migrated {field}")

        # Migrate addresses
        if "addresses" in doc and doc["addresses"] is not None:
            migrated = migrate_basic_extraction_array(doc["addresses"])
            if migrated != doc["addresses"]:
                doc["addresses"] = migrated
                updated = True
                print(f"  Migrated addresses")

        # Migrate business_desc
        if "business_desc" in doc and doc["business_desc"] is not None:
            migrated = migrate_basic_extraction_string(doc["business_desc"])
            if migrated != doc["business_desc"]:
                doc["business_desc"] = migrated
                updated = True
                print(f"  Migrated business_desc")

        # Migrate products
        if "products" in doc and doc["products"] is not None:
            migrated = migrate_keyword_extraction(doc["products"])
            if migrated != doc["products"]:
                doc["products"] = migrated
                updated = True
                print(f"  Migrated products")

        # Migrate concept extraction fields
        for field in ["certificates", "industries", "process_caps", "material_caps"]:
            if field in doc and doc[field] is not None:
                migrated = migrate_concept_extraction(doc[field])
                if migrated != doc[field]:
                    doc[field] = migrated
                    updated = True
                    print(f"  Migrated {field}")

        if updated:
            bulk_operations.append(ReplaceOne({"_id": doc["_id"]}, doc))

        # Execute bulk operation when batch size is reached
        if len(bulk_operations) >= batch_size:
            print(f"Processing batch of {len(bulk_operations)} operations...")
            try:
                result = await collection.bulk_write(bulk_operations)
                total_count += result.modified_count
                failed += len(bulk_operations) - result.modified_count
                print(f"Processed batch: {total_count} updated, {failed} failed")
                bulk_operations = []
            except BulkWriteError as bwe:
                logger.error(f"Bulk write error: {bwe.details}")
                for err in bwe.details.get("writeErrors", [])[:5]:
                    print(
                        f"Error index: {err['index']}, errmsg: {err['errmsg']}, errInfo: {err.get('errInfo')}"
                    )
                failed += len(bulk_operations)
                bulk_operations = []
            except Exception as e:
                logger.error(f"Bulk write error: {e}")
                failed += len(bulk_operations)
                bulk_operations = []

    # Execute remaining operations
    if bulk_operations:
        try:
            print(f"Processing last batch of {len(bulk_operations)} operations...")
            result = await collection.bulk_write(bulk_operations)
            total_count += result.modified_count
            failed += len(bulk_operations) - result.modified_count
        except BulkWriteError as bwe:
            logger.error(f"Final bulk write error: {bwe.details}")
            for err in bwe.details.get("writeErrors", [])[:5]:
                print(
                    f"Error index: {err['index']}, errmsg: {err['errmsg']}, errInfo: {err.get('errInfo')}"
                )
            failed += len(bulk_operations)
        except Exception as e:
            logger.error(f"Final bulk write error: {e}")
            failed += len(bulk_operations)

    print(f"Migration complete: {total_count} documents updated, {failed} failed.")


async def main():
    parser = argparse.ArgumentParser(
        description="Migrate DeferredManufacturer documents to new schema"
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
