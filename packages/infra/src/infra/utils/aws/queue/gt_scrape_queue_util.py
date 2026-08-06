import json
import logging

from pure_utils.env_util import require_env

from infra.models.queue_items.to_scrape_item import ToScrapeItem
from infra.constants import LONG_POLL_INTERVAL
from infra.utils.aws.clients import (
    get_scrape_queue_client,
)

logger = logging.getLogger(__name__)


def _gt_scrape_queue_url() -> str:
    return require_env("GT_SCRAPE_QUEUE_URL")


async def push_item_to_gt_scrape_queue(item: ToScrapeItem):
    """
    Sends an item to the SQS GT scrape queue for scraping.

    :param item: The item to send to the SQS GT scrape queue.
    """
    queue_url = _gt_scrape_queue_url()
    sqs_client = get_scrape_queue_client()
    await sqs_client.send_message(
        QueueUrl=queue_url,
        MessageBody=item.model_dump_json(),
    )
    logger.info(
        f"Sent ToScrapeItem for {item.start_url} to GT scrape queue: {queue_url}"
    )


async def poll_item_from_gt_scrape_queue() -> (
    tuple[ToScrapeItem, str] | tuple[None, None]
):
    """
    Receives a single item from the SQS GT scrape queue.
    """
    queue_url = _gt_scrape_queue_url()
    sqs_client = get_scrape_queue_client()
    logger.info(f"Polling GT Scrape queue: {queue_url}")
    response = await sqs_client.receive_message(
        QueueUrl=queue_url,
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
            QueueUrl=queue_url,
            ReceiptHandle=receipt_handle,
        )
        return None, None

    return item, receipt_handle


async def delete_item_from_gt_scrape_queue(receipt_handle: str):
    """
    Deletes an item from the SQS GT scrape queue.

    :param receipt_handle: The receipt handle of the message to delete.
    """
    queue_url = _gt_scrape_queue_url()
    sqs_client = get_scrape_queue_client()
    await sqs_client.delete_message(
        QueueUrl=queue_url,
        ReceiptHandle=receipt_handle,
    )
    logger.info(
        f"Deleted item from GT Scrape queue with receipt handle: {receipt_handle}"
    )
