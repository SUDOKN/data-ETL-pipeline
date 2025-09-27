import os
import json
import logging

from core.models.to_scrape_item import ToScrapeItem
from core.constants import LONG_POLL_INTERVAL
from core.dependencies.aws_clients import get_scrape_queue_client

SCRAPE_QUEUE_URL = os.getenv("SCRAPE_QUEUE_URL")
logger = logging.getLogger(__name__)


if not SCRAPE_QUEUE_URL:
    raise ValueError(
        "AWS SCRAPE_QUEUE_URL is not set. Please set it in your .env file."
    )


async def push_item_to_scrape_queue(item: ToScrapeItem):
    """
    Sends an item to the SQS queue for scraping.

    :param item: The item to send to the SQS queue.
    """
    assert SCRAPE_QUEUE_URL, "SCRAPE_QUEUE_URL is not set"
    sqs_client = get_scrape_queue_client()
    await sqs_client.send_message(
        QueueUrl=SCRAPE_QUEUE_URL,
        MessageBody=item.model_dump_json(),
    )
    logger.info(
        f"Sent ToScrapeItem for {item.accessible_normalized_url} to scrape queue: {SCRAPE_QUEUE_URL}"
    )


async def poll_item_from_scrape_queue() -> tuple[ToScrapeItem, str] | tuple[None, None]:
    """
    Receives a single item from the SQS queue.
    """
    assert SCRAPE_QUEUE_URL, "SCRAPE_QUEUE_URL is not set"
    sqs_client = get_scrape_queue_client()
    logger.info(f"Polling Scrape queue: {SCRAPE_QUEUE_URL}")
    response = await sqs_client.receive_message(
        QueueUrl=SCRAPE_QUEUE_URL,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=LONG_POLL_INTERVAL,
    )
    messages = response.get("Messages", [])
    if not messages:
        return None, None
    body = messages[0].get("Body")
    if not body:
        logger.error("Message missing Body field")
        return None, None
    logger.info(f"Received message body: {body.strip()}")
    receipt_handle = messages[0].get("ReceiptHandle")
    if not receipt_handle:
        logger.error("Message missing ReceiptHandle field")
        return None, None

    try:
        item_dict = json.loads(body.strip())
        item = ToScrapeItem(**item_dict)
    except Exception as e:
        logger.error(
            f"Error decoding ToScrapeItem JSON from message body: {e}, deleting message from Scrape queue."
        )
        # Optionally delete the message if it's malformed
        await sqs_client.delete_message(
            QueueUrl=SCRAPE_QUEUE_URL,
            ReceiptHandle=receipt_handle,
        )
        return None, None

    return item, receipt_handle


async def delete_item_from_scrape_queue(receipt_handle: str):
    """
    Deletes an item from the SQS queue.

    :param receipt_handle: The receipt handle of the message to delete.
    """
    assert SCRAPE_QUEUE_URL, "SCRAPE_QUEUE_URL is not set"
    sqs_client = get_scrape_queue_client()
    await sqs_client.delete_message(
        QueueUrl=SCRAPE_QUEUE_URL,
        ReceiptHandle=receipt_handle,
    )
    logger.info(f"Deleted item from Scrape queue with receipt handle: {receipt_handle}")
