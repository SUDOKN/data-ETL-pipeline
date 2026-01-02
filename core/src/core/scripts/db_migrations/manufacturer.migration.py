from aiobotocore.session import get_session
import asyncio
import logging
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

from core.models.db.manufacturer import Manufacturer

# from core.utils.aws.s3.s3_client_util import make_s3_client
from core.utils.aws.s3.scraped_text_util import (
    download_scraped_text_from_s3_by_mfg_etld1,
)
from core.utils.mongo_client import (
    init_db,
)

from open_ai_key_app.models.gpt_model import (
    GPT_4o_mini,
    GPTModel,
)

from core.models.binary_classification_result import (
    BinaryClassificationStats,
    ChunkBinaryClassificationResult,
)

# from data_etl_app.services.knowledge.prompt_service import prompt_service
from data_etl_app.utils.chunk_util import (
    get_chunks_respecting_line_boundaries,
)

logger = logging.getLogger(__name__)


async def iterate():
    print("Starting iteration over Manufacturer documents...")
    collection = Manufacturer.get_pymongo_collection()
    print("Count:", await collection.count_documents({}))
    cursor = collection.find({})

    bulk_operations = []
    batch_size = 1000  # Process in batches of 1000
    total_count = 0
    failed = 0

    async for doc in cursor:
        updated = False
        print(
            f"Processing document {total_count + failed + 1} with etld1: {doc.get('etld1')}"
        )

        # set "email_addresses" to None if it doesn't exist
        if "email_addresses" not in doc:
            doc["email_addresses"] = None
            updated = True

        if updated:
            bulk_operations.append(ReplaceOne({"_id": doc["_id"]}, doc))

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
    await init_db()
    print("Database initialized.")
    session = get_session()
    # async with make_s3_client(session) as s3_client:
    await iterate()


if __name__ == "__main__":
    asyncio.run(main())
