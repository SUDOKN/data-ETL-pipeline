from bson import ObjectId
from datetime import datetime
import logging
from typing import Optional

from open_ai_key_app.models.field_types import GPTBatchRequestMongoID
from open_ai_key_app.models.db.gpt_batch_request import GPTBatchRequest

logger = logging.getLogger(__name__)


async def find_gpt_batch_request_by_mongo_id(
    gpt_batch_request_id: GPTBatchRequestMongoID,
) -> GPTBatchRequest:
    gpt_batch_request = await GPTBatchRequest.find_one(
        GPTBatchRequest.id == ObjectId(gpt_batch_request_id)
    )
    if not gpt_batch_request:
        raise ValueError(f"GPTBatchRequest with id {gpt_batch_request_id} not found")

    return gpt_batch_request
