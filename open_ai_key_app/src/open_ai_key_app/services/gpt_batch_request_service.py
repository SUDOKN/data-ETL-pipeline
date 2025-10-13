from bson import ObjectId
from datetime import datetime
import logging
from typing import Optional

from open_ai_key_app.models.field_types import GPTBatchRequestCustomID
from open_ai_key_app.models.db.gpt_batch_request import GPTBatchRequest

logger = logging.getLogger(__name__)


async def find_gpt_batch_request_by_mongo_id(
    gpt_batch_request_custom_id: GPTBatchRequestCustomID,
) -> GPTBatchRequest:
    gpt_batch_request = await GPTBatchRequest.find_one(
        GPTBatchRequest.request.custom_id == gpt_batch_request_custom_id
    )
    if not gpt_batch_request:
        raise ValueError(
            f"GPTBatchRequest with id {gpt_batch_request_custom_id} not found"
        )

    return gpt_batch_request
