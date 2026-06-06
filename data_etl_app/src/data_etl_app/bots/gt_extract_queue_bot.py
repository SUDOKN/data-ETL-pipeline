import argparse
import asyncio
import logging
from datetime import datetime
from typing import Callable, Awaitable

from core.dependencies.load_core_env import load_core_env
from litellm_proxy_app.dependencies.load_litellm_env import load_litellm_env
from scraper_app.dependencies.load_scraper_env import load_scraper_env
from open_ai_key_app.dependencies.load_open_ai_app_env import load_open_ai_app_env
from data_etl_app.dependencies.load_data_etl_env import load_data_etl_env

# Load environment variables
load_core_env()
load_litellm_env()
load_scraper_env()
load_data_etl_env()
load_open_ai_app_env()

from core.dependencies.aws_clients import (
    initialize_core_aws_clients,
    cleanup_core_aws_clients,
)
from data_etl_app.dependencies.aws_clients import (
    initialize_data_etl_aws_clients,
    cleanup_data_etl_aws_clients,
)

from core.models.db.manufacturer import Manufacturer
from core.models.db.extraction_error import ExtractionError
from core.models.to_extract_item import ToExtractItem
from data_etl_app.models.types_and_enums import BinaryClassificationTypeEnum
from open_ai_key_app.models.gpt_model_params import GPTModelParams
from litellm_proxy_app.models.llm_model import GPT_4_1, LLM_Model
from scraper_app.models.scraped_text_file import ScrapedTextFile

from core.services.manufacturer_service import (
    find_manufacturer_by_etld1,
)
from data_etl_app.services.ground_truth.binary_ground_truth_service import (
    get_binary_ground_truth,
)
from data_etl_app.services.knowledge.ontology_service import get_ontology_service
from data_etl_app.services.knowledge.prompt_service import get_prompt_service
from data_etl_app.services.manufacturer_extraction_orchestrator import (
    ManufacturerExtractionOrchestrator,
)

from core.utils.mongo_client import init_db
from core.utils.time_util import get_current_time

logger = logging.getLogger(__name__)

"""
Extract Queue Bot - Data ETL Pipeline

This bot processes items from the extraction queue to extract manufacturing data
from scraped text using various AI services.

"""

INVALID_ITEM_TAG = "INVALID_ITEM"
TOO_SHORT_THRESHOLD = 50  # Minimum number of tokens for scraped text
TOO_LONG_THRESHOLD = 125000  # Maximum number of tokens for scraped text

# Configuration constants
DEFAULT_SLEEP_AFTER_RETRIES = 12 * 60 * 60  # 12 hours in seconds
RETRY_SLEEP_INTERVAL = 5  # seconds
CONCURRENCY_CHECK_INTERVAL = 0.1  # second


class ExtractionStats:
    """Track extraction timing statistics."""

    def __init__(self):
        self.total_time = 0.0
        self.completed_count = 0
        self.average_time = 0.0

    def add_timing(self, duration: float):
        """Add a new timing measurement and update average."""
        self.total_time += duration
        self.completed_count += 1
        self.average_time = self.total_time / self.completed_count

    def print_stats(self):
        """Print the current statistics."""
        logger.info(f"✅ Total completed: {self.completed_count} manufacturers")
        logger.info(
            f"⏱️  Total time: {self.total_time:.2f}s ({self.total_time/60:.1f} minutes)"
        )
        logger.info(f"📈 Average time per manufacturer: {self.average_time:.2f}s")


def parse_args():
    parser = argparse.ArgumentParser(description="SQS Extractor Bot")
    parser.add_argument(
        "--priority",
        type=int,
        default=0,
        help="Is this for priority queue? 0 for no, 1 for yes",
    )
    parser.add_argument(
        "--debug",
        type=str,
        default="INFO",
        help="Logging level: DEBUG, INFO, WARNING, ERROR, CRITICAL",
    )
    parser.add_argument(
        "--max_concurrent_manufacturers",
        type=int,
        default=25,
        help="Queue would not be polled if there are more than this many manufacturers are being processed concurrently.",
    )
    return parser.parse_args()


async def process_queue(
    poll_item_from_queue: Callable[
        [],
        Awaitable[tuple[ToExtractItem, str] | tuple[None, None]],
    ],
    delete_item_from_queue: Callable[[str], Awaitable[None]],
    llm_model: LLM_Model,
    model_params: GPTModelParams,
    max_concurrent_manufacturers: int = 25,
):

    concurrent_manufacturers = set()
    extraction_stats = ExtractionStats()  # Initialize timing stats
    try:
        prompt_service = await get_prompt_service(llm_model)
        ontology_service = await get_ontology_service()
        mfg_orchestrator = ManufacturerExtractionOrchestrator(
            prompt_service=prompt_service,
            ontology_service=ontology_service,
            llm_model=llm_model,
            model_params=model_params,
        )
        while True:
            # Check if we are at the concurrency threshold
            if len(concurrent_manufacturers) >= max_concurrent_manufacturers:
                await asyncio.sleep(CONCURRENCY_CHECK_INTERVAL)  # lets other tasks run
                continue

            item, receipt_handle = (
                await poll_item_from_queue()
            )  # 10 second long poll, doesn't block, yields control back to event loop

            if item is None:
                continue
            elif not receipt_handle:
                logger.warning("No receipt handle found, skipping this message.")
                continue

            polled_at = get_current_time()

            # Validate manufacturer before processing
            manufacturer, scraped_text_file, should_continue = (
                await validate_manufacturer_for_extraction(
                    timestamp=polled_at, item=item, llm_model=llm_model
                )
            )

            if not should_continue:
                # Delete invalid item from queue and continue
                await delete_item_from_queue(receipt_handle)
                continue

            # Help Pylance know we are now certain manufacturer is not None
            assert manufacturer is not None
            assert scraped_text_file is not None

            task = asyncio.create_task(
                extract_and_cleanup(
                    mfg_orchestrator,
                    item,
                    polled_at,
                    scraped_text_file,
                    manufacturer,
                    receipt_handle,
                    delete_item_from_queue,
                    concurrent_manufacturers,
                    extraction_stats,
                )
            )
            concurrent_manufacturers.add(task)

    except Exception as e:
        logger.error(f"Error processing Extract Queue message: {e}")
        await ExtractionError.insert_one(
            ExtractionError(
                created_at=get_current_time(),
                error=str(e),
                field="general_queue_processing",
                mfg_etld1="N/A",
            )
        )
    finally:
        # Print final statistics
        if extraction_stats.completed_count > 0:
            logger.info(f"\n🏁 Final Session Statistics:")
            extraction_stats.print_stats()


async def validate_manufacturer_for_extraction(
    timestamp: datetime,
    item: ToExtractItem,
    llm_model: LLM_Model,
) -> tuple[Manufacturer, ScrapedTextFile, bool] | tuple[None, None, bool]:
    """
    Validate manufacturer for extraction.
    Returns (manufacturer, mfg_txt, version_id, should_continue) where should_continue
    indicates if processing should continue.
    Note: This function does NOT delete from queue - cleanup is handled by caller.
    """

    manufacturer = await find_manufacturer_by_etld1(item.mfg_etld1)

    if not manufacturer:
        logger.warning(
            f"{INVALID_ITEM_TAG}: Manufacturer {item.mfg_etld1} does not exist. Skipping extraction."
        )
        await ExtractionError.insert_one(
            ExtractionError(
                created_at=timestamp,
                error=f"Manufacturer {item.mfg_etld1} does not exist.",
                field="manufacturer",
                mfg_etld1=item.mfg_etld1,
            )
        )
        return None, None, False

    if not manufacturer.scraped_text_file_version_id:
        logger.warning(
            f"{INVALID_ITEM_TAG}: Manufacturer {item.mfg_etld1} has no scraped_text_file_version_id, probably needs scraping. Skipping extraction."
        )
        await ExtractionError.insert_one(
            ExtractionError(
                created_at=timestamp,
                error=f"Manufacturer {item.mfg_etld1} has no scraped_text_file_version_id.",
                field="scraped_text_file_version_id",
                mfg_etld1=item.mfg_etld1,
            )
        )
        return None, None, False

    try:
        existing_scraped_file = await ScrapedTextFile.download_from_s3_and_create(
            item.mfg_etld1,
            manufacturer.scraped_text_file_version_id,
            llm_model,
        )
    except Exception as e:
        logger.error(
            f"Error downloading scraped text file for {item.mfg_etld1} with version ID {manufacturer.scraped_text_file_version_id}: {e}"
        )
        await ExtractionError.insert_one(
            ExtractionError(
                created_at=timestamp,
                error=str(e),
                field="scraped_text_file_download",
                mfg_etld1=item.mfg_etld1,
            )
        )
        return None, None, False

    if not existing_scraped_file:
        logger.warning(
            f"{INVALID_ITEM_TAG}: Scraped text file for {item.mfg_etld1} with version ID {manufacturer.scraped_text_file_version_id} does not exist in S3. Skipping extraction."
        )
        await ExtractionError.insert_one(
            ExtractionError(
                created_at=timestamp,
                error=f"Scraped text file for {item.mfg_etld1} with version ID {manufacturer.scraped_text_file_version_id} does not exist in S3.",
                field="scraped_text_file",
                mfg_etld1=item.mfg_etld1,
            )
        )
        return None, None, False

    return manufacturer, existing_scraped_file, True


async def extract_and_cleanup(
    mfg_orchestrator: ManufacturerExtractionOrchestrator,
    item: ToExtractItem,
    polled_at: datetime,
    scraped_text_file: ScrapedTextFile,
    manufacturer: Manufacturer,
    receipt_handle: str,
    delete_item_from_queue,
    concurrent_manufacturers: set,
    extraction_stats: ExtractionStats,
):
    """Extract manufacturer data and handle cleanup tasks."""
    start_time = get_current_time()
    try:
        await mfg_orchestrator.process_manufacturer(
            timestamp=polled_at,
            mfg=manufacturer,
            scraped_text_file=scraped_text_file,
            eager=True,  # we want to process as quickly as possible since this is a queue worker
        )
        logger.info(f"Manufacturer processed at {polled_at}:\n {manufacturer}\n\n")

        logger.info(
            f"Checking if something was missed in processing {manufacturer.etld1}."
        )

        if item.email_errand:
            logger.info(f"Running email errand for {manufacturer.etld1}.")
            assert manufacturer.is_manufacturer is not None
            is_manufacturer_gt = await get_binary_ground_truth(
                manufacturer,
                manufacturer.is_manufacturer.metadata.prompt_version_id,
                BinaryClassificationTypeEnum.is_manufacturer,
            )
            # final_decision: BaseClassificationDecision = (
            #     is_manufacturer_gt.final_decision
            #     if is_manufacturer_gt and is_manufacturer_gt.final_decision
            #     else manufacturer.is_manufacturer.result
            # )
            subject = f"GT: Finished processing manufacturer URL:{manufacturer.etld1}."
            # if not final_decision.answer:
            #     # send them the dispute form link
            #     html_content = str(
            #         f"Unfortunately, the company {manufacturer.etld1} isn't the kind of manufacturer we are targeting, you can dispute this classification using this link: https://sudokn.com/dispute-manufacturer-classification?etld1={manufacturer.etld1}",
            #     )
            # else:
            #     html_content = (
            #         str(
            #             f"As an MEP user, you can now add or edit the manufacturer details using this link: https://sudokn.com/app/manufacturer/{manufacturer.etld1}/edit",
            #         )
            #         if await is_user_MEP(item.email_errand.user_email)
            #         else str(
            #             f"You can now add or edit the manufacturer details using this link (you will need to verify your manufacturer email first): https://sudokn.com/app/otp-verify?etld1={manufacturer.etld1}"
            #         )
            #     )
            html_content = f"The manufacturer is ready for review."
            await item.email_errand.run_errand(
                subject=subject,
                html_content=html_content,
            )

        # Calculate and log timing
        end_time = get_current_time()
        duration = end_time - start_time
        extraction_stats.add_timing(duration.total_seconds())

        logger.info(f"✅ Extraction completed for {manufacturer.etld1}")
        logger.info(f"   ⏱️  Individual time: {duration.total_seconds():.2f}s")
        extraction_stats.print_stats()

    except Exception as e:
        end_time = get_current_time()
        duration = end_time - start_time
        logger.error(
            f"❌ Error processing manufacturer {manufacturer.etld1} after {duration.total_seconds():.2f}s: {e}"
        )
        await ExtractionError.insert_one(
            ExtractionError(
                created_at=polled_at,
                error=str(e),
                field="general_processing",
                mfg_etld1=manufacturer.etld1,
            )
        )
    finally:
        # Always clean up
        concurrent_manufacturers.discard(asyncio.current_task())
        await delete_item_from_queue(receipt_handle)


async def async_main():
    from core.utils.aws.queue.gt_extract_queue_util import (
        poll_item_from_gt_extract_queue,
        delete_item_from_gt_extract_queue,
    )

    await init_db()

    # Initialize AWS clients
    await initialize_core_aws_clients()
    await initialize_data_etl_aws_clients()

    args = parse_args()

    log_level = args.debug.upper()

    # Validate log level
    valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
    if log_level not in valid_levels:
        log_level = "INFO"

    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    logger = logging.getLogger(__name__)
    logger.info(f"Starting extract bot with log level: {log_level}")

    llm_model = GPT_4_1
    model_params = GPTModelParams(
        temperature=0,  # Greedy decoding — always picks highest probability token
        top_p=1,  # No nucleus sampling restriction needed when temp=0
        presence_penalty=0,  # No penalty adjustments that could shift token selection
        frequency_penalty=0,  # Same — keep it neutral
        seed=12345,  # NEW: explicitly request deterministic sampling,
        max_completion_tokens=7500,  # Limit the response length to avoid excessive token usage
    )

    try:
        await process_queue(
            poll_item_from_queue=poll_item_from_gt_extract_queue,
            delete_item_from_queue=delete_item_from_gt_extract_queue,
            llm_model=llm_model,
            model_params=model_params,
            max_concurrent_manufacturers=args.max_concurrent_manufacturers,
        )
    finally:
        # Clean up AWS clients
        await cleanup_data_etl_aws_clients()
        await cleanup_core_aws_clients()


def main():
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
