import asyncio
import logging
import os
from urllib.parse import urlparse

from pymongo import UpdateOne

from pymongo.errors import BulkWriteError

from pure_utils.env_util import load_env

from data_etl_app.dependencies.env import MIGRATION_ENV

load_env(MIGRATION_ENV)

from data_etl_app.db_models.manufacturer import Manufacturer
from data_etl_app.dependencies.db import init_app_db
from pure_utils.time_util import get_current_time
from pure_utils.url_util import get_etld1_from_host

logger = logging.getLogger(__name__)


LLM_RESET_FIELDS = [
    "is_manufacturer",
    "is_contract_manufacturer",
    "is_product_manufacturer",
    "addresses",
    "business_desc",
    "products",
    "certificates",
    "industries",
    "process_caps",
    "material_caps",
]


def normalize_legacy_accessible_at(value: str) -> str | None:
    cleaned_value = value.strip()
    if not cleaned_value:
        return None

    parsed = urlparse(cleaned_value if "://" in cleaned_value else f"//{cleaned_value}")
    host = parsed.hostname or cleaned_value
    normalized = get_etld1_from_host(host)
    return normalized or None


def build_update_operation(doc: dict) -> UpdateOne | None:
    update_fields: dict[str, object] = {}
    unset_fields: dict[str, str] = {}

    legacy_accessible_at = doc.get("url_accessible_at")
    if legacy_accessible_at is not None:
        normalized_accessible_at = normalize_legacy_accessible_at(
            str(legacy_accessible_at)
        )
        if normalized_accessible_at is not None:
            update_fields["etld1_accessible_at"] = normalized_accessible_at
        unset_fields["url_accessible_at"] = ""

    if "email_addresses" not in doc:
        update_fields["email_addresses"] = None

    for field_name in LLM_RESET_FIELDS:
        if field_name not in doc or doc.get(field_name) is not None:
            update_fields[field_name] = None

    if not update_fields and not unset_fields:
        return None

    update_fields["updated_at"] = get_current_time()

    update_document: dict[str, object] = {"$set": update_fields}
    if unset_fields:
        update_document["$unset"] = unset_fields

    return UpdateOne({"_id": doc["_id"]}, update_document)


def get_mongo_connection_label() -> str:
    mongo_uri = os.getenv("MONGO_DB_URI", "")
    parsed = urlparse(mongo_uri)
    hosts = parsed.netloc.rsplit("@", 1)[-1] if parsed.netloc else "<unknown>"
    return hosts or "<unknown>"


def get_mongo_database_name() -> str:
    mongo_uri = os.getenv("MONGO_DB_URI", "")
    parsed = urlparse(mongo_uri)
    database_name = parsed.path.lstrip("/")
    return database_name or "<default>"


async def iterate():
    print("Starting iteration over Manufacturer documents...")
    collection = Manufacturer.get_pymongo_collection()
    print(f"Mongo connection: {get_mongo_connection_label()}")
    print(f"Mongo database: {get_mongo_database_name()}")
    print("Count:", await collection.count_documents({}))
    input("Press Enter to continue...")
    cursor = collection.find({})

    bulk_operations = []
    batch_size = 1000  # Process in batches of 1000
    total_count = 0
    failed = 0

    async for doc in cursor:
        print(
            f"Processing document {total_count + failed + 1} with etld1: {doc.get('etld1')}"
        )

        update_operation = build_update_operation(doc)
        if update_operation is not None:
            bulk_operations.append(update_operation)

        # Execute bulk operation when batch size is reached
        if len(bulk_operations) >= batch_size:
            print(f"Processing batch of {len(bulk_operations)} operations...")
            print(f"Sample operation: {bulk_operations[0]}")
            try:
                result = await collection.bulk_write(bulk_operations)
                total_count += result.modified_count
                failed += len(bulk_operations) - result.modified_count
                print(f"Processed batch: {total_count} updated, {failed} failed")
                bulk_operations = []
            except BulkWriteError as bwe:
                logger.error(f"Bulk write error: {bwe.details}")
                # Print reasons for the first 5 errors
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
            print(f"Sample operation: {bulk_operations[0]}")
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
    await init_app_db()
    print("Database initialized.")
    await iterate()


if __name__ == "__main__":
    asyncio.run(main())
