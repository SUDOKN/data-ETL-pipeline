import asyncio
import logging
from pymongo import ReplaceOne
from pymongo.errors import BulkWriteError
from shared.utils.mongo_client import init_db
from shared.models.db.manufacturer import Manufacturer
from data_etl_app.models.binary_classification import (
    BinaryClassificationResult,
)
from data_etl_app.models.db.binary_ground_truth import BinaryGroundTruth
from data_etl_app.models.types_and_enums import GroundTruthSource

logger = logging.getLogger(__name__)


async def iterate():
    print("Starting iteration over BinaryGroundTruth documents...")
    collection = BinaryGroundTruth.get_pymongo_collection()
    total = await collection.count_documents({})
    print(f"Total documents: {total}")
    cursor = collection.find({})

    bulk_operations = []
    batch_size = 1000
    total_count = 0
    failed = 0
    processed = 0

    async for doc in cursor:
        updated = False
        processed += 1
        print(f"Processing document {processed} with _id: {doc.get('_id')}")
        logs = doc.get("human_decision_logs", [])
        existing_manufacturer = await Manufacturer.find_one(
            {"etld1": doc.get("mfg_etld1")}
        )
        if not existing_manufacturer:
            print(
                f"Manufacturer with etld1 {doc.get('mfg_etld1')} not found. Skipping..."
            )
            continue

        print(doc.get("llm_decision"))
        original_llm_decision = getattr(
            existing_manufacturer, doc.get("classification_type"), None
        )
        print(original_llm_decision)
        if not isinstance(original_llm_decision, BinaryClassificationResult):
            print(
                f"Manufacturer with etld1 {doc.get('mfg_etld1')} does not have valid original LLM decision for {doc.get('classification_type')}. Skipping..."
            )
            continue
        elif original_llm_decision.evaluated_at != doc["llm_decision"]["evaluated_at"]:
            print(
                f"Manufacturer with etld1 {doc.get('mfg_etld1')} has different evaluated_at for {doc.get('classification_type')}. Skipping..."
            )
            continue
        elif "stats" not in doc.get("llm_decision", {}):
            doc["llm_decision"]["stats"] = original_llm_decision.stats.model_dump()
            updated = True

        for log in logs:
            hd = log.get("human_decision", {})
            if "source" not in hd:
                hd["source"] = GroundTruthSource.API_SURVEY.value
                updated = True
        if updated:
            bulk_operations.append(ReplaceOne({"_id": doc["_id"]}, doc))

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
                failed += len(bulk_operations)
                bulk_operations = []
            except Exception as e:
                logger.error(f"Bulk write error: {e}")
                failed += len(bulk_operations)
                bulk_operations = []

    if bulk_operations:
        try:
            print(f"Processing last batch of {len(bulk_operations)} operations...")
            result = await collection.bulk_write(bulk_operations)
            total_count += result.modified_count
            failed += len(bulk_operations) - result.modified_count
        except BulkWriteError as bwe:
            logger.error(f"Final bulk write error: {bwe.details}")
            failed += len(bulk_operations)
        except Exception as e:
            logger.error(f"Final bulk write error: {e}")
            failed += len(bulk_operations)

    print(f"Migration complete: {total_count} documents updated, {failed} failed.")


async def main():
    await init_db()
    print("Database initialized.")
    await iterate()


if __name__ == "__main__":
    asyncio.run(main())
