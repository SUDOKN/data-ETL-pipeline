import argparse
import asyncio
import logging
import os
import csv
from datetime import datetime
from urllib.parse import urlparse

from aiobotocore.session import get_session
from core.utils.aws.queue.sqs_scraper_client_util import make_sqs_scraper_client
from core.utils.aws.queue.scrape_queue_util import push_item_to_scrape_queue
from core.models.to_scrape_item import ToScrapeItem
from core.models.db.manufacturer import Batch

logger = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(description="Push URLs to scrape queue")
    parser.add_argument(
        "--file",
        type=str,
        default="urls.csv",
        help="Path to CSV file containing URLs and batch titles",
    )
    return parser.parse_args()


async def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    args = parse_args()

    # Read URLs from CSV file
    if not os.path.exists(args.file):
        logger.error(f"File {args.file} does not exist")
        return

    url_batch_pairs = []
    with open(args.file, "r", newline="") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            url = row.get("url", "").strip()
            batch_title = row.get("batch_title", "").strip()
            if url and batch_title:
                url_batch_pairs.append((url, batch_title))

    if not url_batch_pairs:
        logger.warning("No valid URL/batch_title pairs found in CSV file")
        return

    logger.info(
        f"Found {len(url_batch_pairs)} URL/batch_title pairs to push to scrape queue"
    )

    # Initialize AWS session and SQS client
    session = get_session()

    async with make_sqs_scraper_client(session) as sqs_client:
        for url, batch_title in url_batch_pairs:
            try:
                # Create batch info with the specific batch title from CSV
                batch = Batch(title=batch_title, timestamp=datetime.now())

                # Create ToScrapeItem
                item = ToScrapeItem(accessible_normalized_url=url, batch=batch)

                # Push to queue
                await push_item_to_scrape_queue(sqs_client, item)
                logger.info(
                    f"Pushed {item.accessible_normalized_url} to scrape queue with batch '{batch_title}'"
                )

            except Exception as e:
                logger.error(
                    f"Error processing URL {url} with batch '{batch_title}': {e}"
                )

    logger.info("Finished pushing all URLs to scrape queue")


if __name__ == "__main__":
    asyncio.run(main())
