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

from data_etl_app.models.binary_classification_result import (
    BinaryClassificationStats,
    ChunkBinaryClassificationResult,
)

# from data_etl_app.services.knowledge.prompt_service import prompt_service
from data_etl_app.utils.chunk_util import (
    get_chunks_respecting_line_boundaries,
)

logger = logging.getLogger(__name__)


async def iterate():
    gpt_model: GPTModel = GPT_4o_mini
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

        # for field in [
        #     "is_manufacturer",
        #     "is_contract_manufacturer",
        #     "is_product_manufacturer",
        # ]:
        #     if (
        #         field in doc
        #         and doc[field] is not None
        #         and ("stats" not in doc[field] or doc[field]["stats"] is None)
        #     ):
        #         prompt = getattr(prompt_service, f"{field}_prompt")
        #         mfg_text, _scraped_text_file_version_id = (
        #             await download_scraped_text_from_s3_by_mfg_etld1(
        #                 s3_client, doc["etld1"], doc.get("scraped_text_file_version_id")
        #             )
        #         )
        #         chunks_map = get_chunks_respecting_line_boundaries(
        #             mfg_text,
        #             gpt_model.max_context_tokens - prompt.num_tokens - 5000,
        #         )
        #         first_chunk_key = min(
        #             chunks_map.keys(), key=lambda k: int(k.split(":")[0])
        #         )

        #         doc[field]["stats"] = BinaryClassificationStats(
        #             prompt_version_id=prompt.s3_version_id,
        #             final_chunk_key=first_chunk_key,
        #             chunk_result_map={
        #                 first_chunk_key: ChunkBinaryClassificationResult(
        #                     answer=doc[field]["answer"],
        #                     confidence=doc[field]["confidence"],
        #                     reason=doc[field]["reason"],
        #                 )
        #             },
        #         ).model_dump()
        #         updated = True

        # # rename stats.search to stats.chunked_stats for specified fields
        # for field in [
        #     "products",
        #     "certificates",
        #     "industries",
        #     "process_caps",
        #     "material_caps",
        # ]:
        #     if field in doc and doc[field] is not None:

        #         # if "extract_prompt_version_id" not in doc[field]["stats"]:
        #         doc[field]["stats"]["extract_prompt_version_id"] = getattr(
        #             prompt_service,
        #             f"extract_any_{field[0:-1] if field != 'industries' else 'industry'}_prompt",
        #         ).s3_version_id
        #         updated = True

        #         # if "map_prompt_version_id" not in doc[field]["stats"]:
        #         if field != "products":
        #             doc[field]["stats"]["map_prompt_version_id"] = getattr(
        #                 prompt_service,
        #                 f"unknown_to_known_{field[0:-1] if field != 'industries' else 'industry'}_prompt",
        #             ).s3_version_id
        #             updated = True

        #         if "search" in doc[field]["stats"]:
        #             if field == "products":
        #                 old_search_map = doc[field]["stats"].pop("search")
        #                 doc[field]["stats"]["chunked_stats"] = {
        #                     k: {"results": list(set(v))}
        #                     for k, v in old_search_map.items()
        #                 }
        #                 updated = True
        #             else:
        #                 doc[field]["stats"]["chunked_stats"] = doc[field]["stats"].pop(
        #                     "search"
        #                 )
        #                 updated = True

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
