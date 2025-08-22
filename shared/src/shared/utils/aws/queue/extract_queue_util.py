import os
import json
import logging

from shared.models.to_extract_item import ToExtractItem
from shared.constants import LONG_POLL_INTERVAL

logger = logging.getLogger(__name__)

EXTRACT_QUEUE_URL = os.getenv("EXTRACT_QUEUE_URL")

if not EXTRACT_QUEUE_URL:
    raise ValueError(
        "AWS EXTRACT_QUEUE_URL is not set. Please set it in your .env file."
    )


async def push_item_to_extract_queue(sqs_client, item: ToExtractItem):
    """
    Sends an item to the Extract queue for extraction.

    :param sqs_client: The SQS client to use for sending messages.
    :param item: The item to send to the SQS queue.
    """
    await sqs_client.send_message(
        QueueUrl=EXTRACT_QUEUE_URL,
        MessageBody=item.model_dump_json(),
    )
    logger.info(
        f"Sent ToExtractItem for {item.mfg_etld1} to extract queue: {EXTRACT_QUEUE_URL}."
    )


async def poll_item_from_extract_queue(sqs_client):
    """
    Receives a single item from the Extract queue.

    :param sqs_client: The SQS client to use for receiving messages.
    """
    logger.info(f"Polling SQS Extract queue: {EXTRACT_QUEUE_URL}")
    response = await sqs_client.receive_message(
        QueueUrl=EXTRACT_QUEUE_URL,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=LONG_POLL_INTERVAL,
    )
    messages = response.get("Messages", [])
    if not messages:
        return None, None
    body = messages[0]["Body"]  # Return the body of the first message
    logger.info(f"Received message body: {body.strip()}")
    receipt_handle = messages[0]["ReceiptHandle"]

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


async def delete_item_from_extract_queue(sqs_client, receipt_handle: str):
    """
    Deletes an item from the Extract queue.

    :param sqs_client: The SQS client to use for deleting messages.
    :param receipt_handle: The receipt handle of the message to delete.
    """
    await sqs_client.delete_message(
        QueueUrl=EXTRACT_QUEUE_URL,
        ReceiptHandle=receipt_handle,
    )
    logger.info(
        f"Deleted item from Extract queue with receipt handle: {receipt_handle}"
    )
