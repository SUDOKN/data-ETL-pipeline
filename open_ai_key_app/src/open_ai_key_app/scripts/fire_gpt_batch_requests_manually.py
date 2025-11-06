import asyncio
import logging

from core.dependencies.load_core_env import load_core_env
from scraper_app.dependencies.load_scraper_env import load_scraper_env
from open_ai_key_app.dependencies.load_open_ai_app_env import load_open_ai_app_env
from core.dependencies.load_core_env import load_core_env
from scraper_app.dependencies.load_scraper_env import load_scraper_env
from open_ai_key_app.dependencies.load_open_ai_app_env import load_open_ai_app_env
from data_etl_app.dependencies.load_data_etl_env import load_data_etl_env

# Load environment variables
load_core_env()
load_scraper_env()
load_data_etl_env()
load_open_ai_app_env()

import argparse
import asyncio
import logging
import re
from datetime import datetime
from time import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from core.utils.mongo_client import init_db
from core.utils.time_util import get_current_time
from core.models.db.manufacturer import Manufacturer
from core.models.db.gpt_batch_request import GPTBatchRequest
from core.models.gpt_batch_response_blob import (
    GPTBatchResponseBlob,
    GPTBatchResponseBody,
    GPTResponseBlobBody,
    GPTBatchResponseBlobChoice,
    GPTBatchResponseBlobChoiceMessage,
    GPTBatchResponseBlobUsage,
)
from open_ai_key_app.utils.ask_gpt_util import send_gpt_batch_request_sync


async def process_manufacturer_requests(
    manufacturer: Manufacturer, mfg_index: int, total_mfgs: int
):
    """
    Process all pending GPT batch requests for a single manufacturer.

    Args:
        manufacturer: The Manufacturer document
        mfg_index: Current index in the iteration (1-based)
        total_mfgs: Total number of manufacturers being processed

    Returns:
        tuple: (successful_count, failed_count, has_pending_requests)
    """
    etld1 = manufacturer.etld1
    logger.info(
        f"[{mfg_index}/{total_mfgs}] Processing manufacturer: {etld1} (tokens: {manufacturer.scraped_text_file_num_tokens})"
    )

    # Find all matching GPTBatchRequest documents
    regex_pattern = f"^{re.escape(etld1)}>.*"

    batch_requests = await GPTBatchRequest.find(
        {
            "request.custom_id": {"$regex": regex_pattern},
            "batch_id": None,
            "response_blob": None,
        }
    ).to_list()

    if not batch_requests:
        logger.info(f"  No pending batch requests found for {etld1}")
        return 0, 0, False

    logger.info(f"  Found {len(batch_requests)} pending batch request(s) for {etld1}")

    successful_count = 0
    failed_count = 0

    async def process_single_request(batch_req: GPTBatchRequest, idx: int):
        """Process a single batch request"""
        nonlocal successful_count, failed_count

        custom_id = batch_req.request.custom_id
        logger.info(f"    [{idx}/{len(batch_requests)}] Sending request: {custom_id}")

        try:
            # Send the request
            response = await send_gpt_batch_request_sync(batch_req.request)

            # Parse the response and create GPTBatchResponseBlob
            response_blob = GPTBatchResponseBlob(
                batch_id="fired_manually",
                request_custom_id=custom_id,
                response=GPTBatchResponseBody(
                    status_code=200,
                    body=GPTResponseBlobBody(
                        created=datetime.fromtimestamp(response["created"]),
                        choices=[
                            GPTBatchResponseBlobChoice(
                                index=choice["index"],
                                message=GPTBatchResponseBlobChoiceMessage(
                                    role=choice["message"]["role"],
                                    content=choice["message"]["content"],
                                ),
                            )
                            for choice in response["choices"]
                        ],
                        usage=GPTBatchResponseBlobUsage(
                            prompt_tokens=response["usage"]["prompt_tokens"],
                            completion_tokens=response["usage"]["completion_tokens"],
                            total_tokens=response["usage"]["total_tokens"],
                        ),
                    ),
                ),
            )

            # Update the GPTBatchRequest document
            batch_req.batch_id = "fired_manually"
            batch_req.response_blob = response_blob
            await batch_req.save()

            successful_count += 1
            logger.info(f"    ✓ Successfully processed: {custom_id}")

        except Exception as e:
            failed_count += 1
            logger.error(f"    ✗ Failed to process {custom_id}: {str(e)}")
            # Continue processing other requests (exception is caught, not raised)

    # Process batch requests in parallel, 10 at a time
    BATCH_SIZE = 20
    for i in range(0, len(batch_requests), BATCH_SIZE):
        batch = batch_requests[i : i + BATCH_SIZE]
        batch_num = (i // BATCH_SIZE) + 1
        total_batches = (len(batch_requests) + BATCH_SIZE - 1) // BATCH_SIZE

        logger.info(
            f"  Processing batch {batch_num}/{total_batches} ({len(batch)} requests)..."
        )

        # Process this batch in parallel
        tasks = [
            process_single_request(batch_req, i + idx + 1)
            for idx, batch_req in enumerate(batch)
        ]
        await asyncio.gather(*tasks, return_exceptions=True)

    return successful_count, failed_count, True


async def main():
    """Main function to process all manufacturers"""
    # Parse command-line arguments
    parser = argparse.ArgumentParser(
        description="Fire GPT batch requests manually for manufacturers"
    )
    parser.add_argument(
        "--limit-manufacturers",
        type=int,
        help="Maximum number of manufacturers with pending requests to process (for testing)",
    )
    parser.add_argument(
        "--limit-requests",
        type=int,
        help="Maximum number of total batch requests to process (for testing)",
    )
    args = parser.parse_args()

    logger.info("Starting GPT batch request processing...")

    if args.limit_manufacturers:
        logger.info(
            f"⚠️  LIMIT MODE: Processing only {args.limit_manufacturers} manufacturers with pending requests"
        )
    if args.limit_requests:
        logger.info(
            f"⚠️  LIMIT MODE: Processing only {args.limit_requests} total batch requests"
        )

    MAX_POOL_SIZE = 40
    min_pool_size = 10  # 25% of max

    logger.info("=" * 70)
    logger.info("MONGODB POOL CONFIGURATION")
    logger.info("=" * 70)
    logger.info(f"  - Max pool size: {MAX_POOL_SIZE} (~600 MB RAM)")
    logger.info(f"  - Min pool size: {min_pool_size}")
    logger.info("=" * 70)

    # Initialize database with connection pool settings
    await init_db(
        max_pool_size=MAX_POOL_SIZE,
        min_pool_size=min_pool_size,
        max_idle_time_ms=60000,
        server_selection_timeout_ms=30000,
        connect_timeout_ms=30000,
        socket_timeout_ms=120000,
    )

    # Track overall statistics
    start_time = time()
    total_processed = 0
    total_with_pending = 0  # Track manufacturers that have pending requests
    total_skipped = 0  # Track manufacturers without pending requests
    total_successful = 0
    total_failed = 0
    total_requests_processed = 0  # Track total number of requests processed
    total_tokens_processed = 0
    total_tokens_with_pending = (
        0  # Track tokens only for manufacturers with pending requests
    )
    last_checkpoint_time = start_time
    last_checkpoint_count = 0
    last_checkpoint_with_pending = 0

    # Query manufacturers with scraped_text_file_num_tokens < 30000 in ascending order
    mfg_index = 0
    async for manufacturer in Manufacturer.find(
        {"scraped_text_file_num_tokens": {"$lt": 30000}}
    ).sort("scraped_text_file_num_tokens"):
        mfg_index += 1
        total_processed += 1
        total_tokens_processed += manufacturer.scraped_text_file_num_tokens or 0

        # Process all requests for this manufacturer
        successful, failed, has_pending = await process_manufacturer_requests(
            manufacturer, mfg_index, total_processed
        )

        total_successful += successful
        total_failed += failed
        total_requests_processed += successful + failed

        # Only count manufacturers with pending requests towards the limit
        if has_pending:
            total_with_pending += 1
            total_tokens_with_pending += manufacturer.scraped_text_file_num_tokens or 0
        else:
            total_skipped += 1

        # Check if we've reached the manufacturer limit
        if args.limit_manufacturers and total_with_pending >= args.limit_manufacturers:
            logger.info(
                f"\n⚠️  Reached limit of {args.limit_manufacturers} manufacturers with pending requests. Stopping."
            )
            logger.info(
                f"   (Checked {total_processed} total manufacturers, {total_with_pending} had pending requests, {total_skipped} skipped)"
            )
            logger.info(f"   (Processed {total_requests_processed} total requests)")
            break

        # Check if we've reached the request limit
        if args.limit_requests and total_requests_processed >= args.limit_requests:
            logger.info(
                f"\n⚠️  Reached limit of {args.limit_requests} batch requests. Stopping."
            )
            logger.info(
                f"   (Checked {total_processed} total manufacturers, {total_with_pending} had pending requests, {total_skipped} skipped)"
            )
            logger.info(f"   (Processed {total_requests_processed} total requests)")
            break

        # Log progress every 100 manufacturers
        if total_processed % 100 == 0:
            current_time = time()
            elapsed_total = current_time - start_time
            elapsed_checkpoint = current_time - last_checkpoint_time
            mfgs_in_checkpoint = total_processed - last_checkpoint_count
            mfgs_with_pending_in_checkpoint = (
                total_with_pending - last_checkpoint_with_pending
            )

            avg_time_per_mfg_total = elapsed_total / total_processed
            avg_time_per_mfg_checkpoint = (
                elapsed_checkpoint / mfgs_in_checkpoint if mfgs_in_checkpoint > 0 else 0
            )
            avg_time_per_mfg_with_pending = (
                elapsed_total / total_with_pending if total_with_pending > 0 else 0
            )
            avg_time_per_token = (
                elapsed_total / total_tokens_processed
                if total_tokens_processed > 0
                else 0
            )
            avg_time_per_token_with_pending = (
                elapsed_total / total_tokens_with_pending
                if total_tokens_with_pending > 0
                else 0
            )

            logger.info(f"\n{'='*80}")
            logger.info(
                f"PROGRESS CHECKPOINT - {total_processed} manufacturers checked"
            )
            logger.info(f"{'='*80}")
            logger.info(f"  Manufacturers with pending requests: {total_with_pending}")
            logger.info(f"  Manufacturers skipped (no pending): {total_skipped}")
            logger.info(
                f"  Last 100: {mfgs_with_pending_in_checkpoint} with pending, {mfgs_in_checkpoint - mfgs_with_pending_in_checkpoint} skipped"
            )
            logger.info(f"  Total time elapsed: {elapsed_total:.2f}s")
            logger.info(f"  Time for last 100 mfgs: {elapsed_checkpoint:.2f}s")
            logger.info(f"  Avg time per mfg (overall): {avg_time_per_mfg_total:.3f}s")
            logger.info(
                f"  Avg time per mfg (last 100): {avg_time_per_mfg_checkpoint:.3f}s"
            )
            logger.info(
                f"  Avg time per mfg with pending: {avg_time_per_mfg_with_pending:.3f}s"
            )
            logger.info(f"  Total tokens checked: {total_tokens_processed:,}")
            logger.info(
                f"  Tokens for mfgs with pending: {total_tokens_with_pending:,}"
            )
            logger.info(f"  Avg time per token (all): {avg_time_per_token*1000:.6f}ms")
            logger.info(
                f"  Avg time per token (pending): {avg_time_per_token_with_pending*1000:.6f}ms"
            )
            logger.info(f"  Total requests processed: {total_requests_processed}")
            logger.info(f"  Requests successful: {total_successful}")
            logger.info(f"  Requests failed: {total_failed}")
            logger.info(f"{'='*80}\n")

            last_checkpoint_time = current_time
            last_checkpoint_count = total_processed
            last_checkpoint_with_pending = total_with_pending

    # Final summary
    end_time = time()
    total_time = end_time - start_time
    avg_time_per_mfg = total_time / total_processed if total_processed > 0 else 0
    avg_time_per_mfg_with_pending = (
        total_time / total_with_pending if total_with_pending > 0 else 0
    )
    avg_time_per_token_all = (
        total_time / total_tokens_processed if total_tokens_processed > 0 else 0
    )
    avg_time_per_token_with_pending = (
        total_time / total_tokens_with_pending if total_tokens_with_pending > 0 else 0
    )

    logger.info(f"\n{'='*80}")
    logger.info(f"FINAL SUMMARY")
    logger.info(f"{'='*80}")
    logger.info(f"  Total manufacturers checked: {total_processed}")
    logger.info(f"  Manufacturers with pending requests: {total_with_pending}")
    logger.info(f"  Manufacturers skipped (no pending): {total_skipped}")
    logger.info(
        f"  Success rate: {total_with_pending}/{total_processed} ({100*total_with_pending/total_processed:.1f}%)"
        if total_processed > 0
        else "  Success rate: N/A"
    )
    logger.info(f"  Total time: {total_time:.2f}s ({total_time/60:.2f} minutes)")
    logger.info(f"  Avg time per manufacturer (all): {avg_time_per_mfg:.3f}s")
    logger.info(
        f"  Avg time per manufacturer (with pending): {avg_time_per_mfg_with_pending:.3f}s"
    )
    logger.info(f"  Total tokens checked: {total_tokens_processed:,}")
    logger.info(f"  Tokens for mfgs with pending: {total_tokens_with_pending:,}")
    logger.info(f"  Avg time per token (all): {avg_time_per_token_all*1000:.6f}ms")
    logger.info(
        f"  Avg time per token (pending): {avg_time_per_token_with_pending*1000:.6f}ms"
    )
    logger.info(f"  Total requests processed: {total_requests_processed}")
    logger.info(f"  Total requests successful: {total_successful}")
    logger.info(f"  Total requests failed: {total_failed}")
    logger.info(
        f"  Request success rate: {total_successful}/{total_successful + total_failed} ({100*total_successful/(total_successful + total_failed):.1f}%)"
        if (total_successful + total_failed) > 0
        else "  Request success rate: N/A"
    )
    logger.info(f"{'='*80}\n")


if __name__ == "__main__":
    asyncio.run(main())
