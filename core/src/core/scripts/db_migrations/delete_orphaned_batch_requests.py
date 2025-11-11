import asyncio
import logging
from datetime import datetime
from pathlib import Path
from typing import Set

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
from core.models.db.gpt_batch_request import GPTBatchRequest
from core.services.deferred_manufacturer_service import get_embedded_gpt_request_ids
from core.utils.mongo_client import init_db
from open_ai_key_app.models.field_types import GPTBatchRequestCustomID

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Batch IDs to exclude from reset operation
# EXCLUDED_BATCH_IDS = ["batch_68f0d06be40881909b23e7d94f307324"]


async def count_incomplete_requests():
    """
    Count GPTBatchRequest documents for deferred manufacturers with < 200,000 tokens
    where batch_id is not None but response_blob is None.

    Returns:
        Tuple of (total_df_mfgs_affected, total_requests_to_reset)
    """
    logger.info("Counting incomplete GPTBatchRequest documents...")
    logger.info("Querying deferred manufacturers with < 200,000 tokens...")

    total_df_mfgs_processed = 0
    total_df_mfgs_affected = 0
    total_requests_to_reset = 0

    cursor = DeferredManufacturer.find(
        {"scraped_text_file_num_tokens": {"$lt": 200_000}}
    )

    async for df_mfg in cursor:
        total_df_mfgs_processed += 1

        embedded_request_ids = get_embedded_gpt_request_ids(df_mfg)

        if not embedded_request_ids:
            continue

        # Count requests with batch_id set but no response_blob (excluding specific batch_ids)
        count = await GPTBatchRequest.find(
            {
                "request.custom_id": {"$in": list(embedded_request_ids)},
                "batch_id": {
                    "$ne": None,
                    # "$nin": EXCLUDED_BATCH_IDS
                },
                "response_blob": None,
            }
        ).count()

        if count > 0:
            total_df_mfgs_affected += 1
            total_requests_to_reset += count

        # Print progress every 1000 manufacturers
        if total_df_mfgs_processed % 1000 == 0:
            logger.info(
                f"Progress: Processed {total_df_mfgs_processed} deferred manufacturers, "
                f"found {total_requests_to_reset} requests to reset so far"
            )

    logger.info(
        f"Final count: Processed {total_df_mfgs_processed} deferred manufacturers"
    )

    return total_df_mfgs_affected, total_requests_to_reset


async def reset_batch_ids_for_incomplete_requests():
    """
    For deferred manufacturers with < 200,000 tokens, reset batch_id to None
    for GPTBatchRequest documents where batch_id is not None but response_blob is None.
    """
    logger.info("\n" + "=" * 80)
    logger.info("Starting reset of batch_id for incomplete requests...")
    logger.info("Querying deferred manufacturers with < 200,000 tokens...")

    # Track statistics
    total_df_mfgs_processed = 0
    total_requests_reset = 0
    df_mfgs_with_resets: Set[str] = set()

    # Track per-mfg resets for detailed logging
    mfg_reset_details = []

    # Query deferred manufacturers with < 200,000 tokens
    cursor = DeferredManufacturer.find(
        {"scraped_text_file_num_tokens": {"$lt": 200_000}}
    )

    async for df_mfg in cursor:
        total_df_mfgs_processed += 1

        # Get all embedded request IDs for this deferred manufacturer
        embedded_request_ids = get_embedded_gpt_request_ids(df_mfg)

        if not embedded_request_ids:
            continue

        # Find requests with batch_id set but no response_blob (excluding specific batch_ids)
        incomplete_request_ids: list[GPTBatchRequestCustomID] = []

        async for gpt_req in GPTBatchRequest.find(
            {
                "request.custom_id": {"$in": list(embedded_request_ids)},
                "batch_id": {
                    "$ne": None,
                    # "$nin": EXCLUDED_BATCH_IDS
                },
                "response_blob": None,
            }
        ):
            incomplete_request_ids.append(gpt_req.request.custom_id)

        # Reset batch_id for incomplete requests if any found
        if incomplete_request_ids:
            reset_result = await GPTBatchRequest.get_pymongo_collection().update_many(
                {"request.custom_id": {"$in": incomplete_request_ids}},
                {"$set": {"batch_id": None}},
            )

            num_reset = reset_result.modified_count
            total_requests_reset += num_reset
            df_mfgs_with_resets.add(df_mfg.mfg_etld1)

            mfg_reset_details.append(
                {
                    "mfg_etld1": df_mfg.mfg_etld1,
                    "num_tokens": df_mfg.scraped_text_file_num_tokens,
                    "num_reset": num_reset,
                }
            )

            logger.info(
                f"Reset batch_id for {num_reset} incomplete requests for {df_mfg.mfg_etld1} "
                f"({df_mfg.scraped_text_file_num_tokens:,} tokens)"
            )

        # Print progress every 100 manufacturers
        if total_df_mfgs_processed % 100 == 0:
            logger.info(
                f"Progress: Processed {total_df_mfgs_processed} deferred manufacturers, "
                f"reset {total_requests_reset} requests so far"
            )

    # Print final summary
    logger.info("\n" + "=" * 80)
    logger.info("BATCH_ID RESET SUMMARY")
    logger.info("=" * 80)
    logger.info(
        f"Total deferred manufacturers processed (<200k tokens): {total_df_mfgs_processed}"
    )
    logger.info(f"Total GPTBatchRequest batch_ids reset: {total_requests_reset}")
    logger.info(
        f"Number of deferred manufacturers affected: {len(df_mfgs_with_resets)}"
    )
    logger.info("=" * 80)

    # Save detailed results to CSV
    if mfg_reset_details:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_dir = Path("batch_id_reset_" + timestamp)
        output_dir.mkdir(exist_ok=True)

        csv_path = output_dir / "reset_details.csv"
        with open(csv_path, "w") as f:
            f.write("mfg_etld1,num_tokens,num_reset\n")
            for detail in sorted(
                mfg_reset_details, key=lambda x: x["num_reset"], reverse=True
            ):
                f.write(
                    f"{detail['mfg_etld1']},{detail['num_tokens']},{detail['num_reset']}\n"
                )

        logger.info(f"\nDetailed reset report saved to: {csv_path}")
        logger.info(f"Output directory: {output_dir}")

    logger.info("\n✅ Batch_id reset complete!")


async def main():
    await init_db()
    logger.info("Database initialized.\n")

    # Count what will be affected
    logger.info("=" * 80)
    logger.info("ANALYZING DATABASE...")
    logger.info("=" * 80)

    df_mfgs_to_reset, requests_to_reset = await count_incomplete_requests()

    # Display summary
    logger.info("\n" + "=" * 80)
    logger.info("IMPACT SUMMARY")
    logger.info("=" * 80)
    logger.info("BATCH_ID RESET (Deferred Manufacturers with < 200,000 tokens)")
    logger.info(f"  - Deferred manufacturers affected: {df_mfgs_to_reset}")
    logger.info(f"  - GPTBatchRequest batch_ids to RESET: {requests_to_reset}")
    # logger.info(f"  - Excluded batch_ids: {EXCLUDED_BATCH_IDS}")
    logger.info("=" * 80)

    # Ask for confirmation
    if requests_to_reset == 0:
        logger.info("\n✅ No changes needed. Database is already in the desired state.")
        return

    logger.info("\n⚠️  WARNING: This operation will RESET batch_ids to None!")

    response = input("\nDo you want to proceed? (yes/no): ").strip().lower()

    if response not in ["yes", "y"]:
        logger.info("\n❌ Operation cancelled by user.")
        return

    logger.info("\n✅ Proceeding with batch_id reset...\n")

    # Proceed with reset
    await reset_batch_ids_for_incomplete_requests()

    logger.info("\n" + "=" * 80)
    logger.info("✅ OPERATION COMPLETED SUCCESSFULLY!")
    logger.info("=" * 80)


if __name__ == "__main__":
    asyncio.run(main())
