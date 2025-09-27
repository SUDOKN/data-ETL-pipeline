import os
import json
import logging

from core.models.to_extract_item import ToExtractItem
from core.constants import LONG_POLL_INTERVAL
from core.dependencies.aws_clients import get_extract_queue_client

logger = logging.getLogger(__name__)

EXTRACT_QUEUE_URL = os.getenv("EXTRACT_QUEUE_URL")

if not EXTRACT_QUEUE_URL:
    raise ValueError(
        "AWS EXTRACT_QUEUE_URL is not set. Please set it in your .env file."
    )


async def push_item_to_extract_queue(item: ToExtractItem):
    """
    Sends an item to the Extract queue for extraction.

    :param item: The item to send to the SQS queue.
    """
    assert EXTRACT_QUEUE_URL, "EXTRACT_QUEUE_URL is not set"
    sqs_client = get_extract_queue_client()
    await sqs_client.send_message(
        QueueUrl=EXTRACT_QUEUE_URL,
        MessageBody=item.model_dump_json(),
    )
    logger.info(
        f"Sent ToExtractItem for {item.mfg_etld1} to extract queue: {EXTRACT_QUEUE_URL}."
    )


async def poll_item_from_extract_queue():
    """
    Receives a single item from the Extract queue.
    """
    assert EXTRACT_QUEUE_URL, "EXTRACT_QUEUE_URL is not set"
    sqs_client = get_extract_queue_client()
    logger.info(f"Polling SQS Extract queue: {EXTRACT_QUEUE_URL}")
    response = await sqs_client.receive_message(
        QueueUrl=EXTRACT_QUEUE_URL,
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
        item = ToExtractItem(**item_dict)
    except Exception as e:
        logger.error(f"Error decoding ToExtractItem JSON from message body: {e}")
        # Optionally delete the message if it's malformed
        await sqs_client.delete_message(
            QueueUrl=EXTRACT_QUEUE_URL,
            ReceiptHandle=receipt_handle,
        )
        return None, None

    return item, receipt_handle


async def delete_item_from_extract_queue(receipt_handle: str):
    """
    Deletes an item from the Extract queue.

    :param receipt_handle: The receipt handle of the message to delete.
    """
    assert EXTRACT_QUEUE_URL, "EXTRACT_QUEUE_URL is not set"
    sqs_client = get_extract_queue_client()
    await sqs_client.delete_message(
        QueueUrl=EXTRACT_QUEUE_URL,
        ReceiptHandle=receipt_handle,
    )
    logger.info(
        f"Deleted item from Extract queue with receipt handle: {receipt_handle}"
    )
