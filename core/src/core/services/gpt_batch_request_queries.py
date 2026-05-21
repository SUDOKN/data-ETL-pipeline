import logging
from typing import Optional

from core.models.db.gpt_batch import GPTBatch
from core.models.db.gpt_batch_request import GPTBatchRequest
from open_ai_key_app.models.field_types import GPTBatchRequestCustomID

logger = logging.getLogger(__name__)


async def find_gpt_batch_request_by_custom_id(
    gpt_batch_request_custom_id: GPTBatchRequestCustomID,
) -> Optional[GPTBatchRequest]:
    gpt_batch_request = await GPTBatchRequest.find_one(
        GPTBatchRequest.request.custom_id == gpt_batch_request_custom_id
    )

    return gpt_batch_request


async def find_completed_gpt_batch_request_by_custom_id(
    gpt_batch_request_custom_id: GPTBatchRequestCustomID,
) -> Optional[GPTBatchRequest]:
    if gpt_batch_request_custom_id is None:
        raise ValueError("gpt_batch_request_custom_id cannot be None")

    gpt_batch_request = await GPTBatchRequest.find_one(
        GPTBatchRequest.request.custom_id == gpt_batch_request_custom_id,
        GPTBatchRequest.batch_id != None,
        GPTBatchRequest.response_blob != None,
    )

    return gpt_batch_request


async def find_gpt_batch_requests_by_custom_ids(
    gpt_batch_request_custom_ids: list[GPTBatchRequestCustomID],
) -> dict[GPTBatchRequestCustomID, GPTBatchRequest]:
    if gpt_batch_request_custom_ids is None:
        raise ValueError("gpt_batch_request_custom_ids cannot be None")
    elif len(gpt_batch_request_custom_ids) == 0:
        raise ValueError("gpt_batch_request_custom_ids cannot be empty")
    elif any(cid is None for cid in gpt_batch_request_custom_ids):
        raise ValueError("gpt_batch_request_custom_ids cannot contain None values")

    request_map: dict[GPTBatchRequestCustomID, GPTBatchRequest] = dict()
    async for gpt_req in GPTBatchRequest.find(
        {"request.custom_id": {"$in": gpt_batch_request_custom_ids}}
    ):
        request_map[gpt_req.request.custom_id] = gpt_req

    return request_map


async def find_gpt_batch_request_ids_only(
    gpt_batch_request_custom_ids: list[GPTBatchRequestCustomID],
) -> set[GPTBatchRequestCustomID]:
    if gpt_batch_request_custom_ids is None:
        raise ValueError("gpt_batch_request_custom_ids cannot be None")
    elif len(gpt_batch_request_custom_ids) == 0:
        raise ValueError("gpt_batch_request_custom_ids cannot be empty")
    elif any(cid is None for cid in gpt_batch_request_custom_ids):
        raise ValueError("gpt_batch_request_custom_ids cannot contain None values")

    gpt_req_ids_found = set()
    collection = GPTBatchRequest.get_pymongo_collection()

    cursor = collection.find(
        {
            "request.custom_id": {"$in": gpt_batch_request_custom_ids},
        },
        projection={"request.custom_id": 1, "_id": 0},
    )

    async for doc in cursor:
        gpt_req_ids_found.add(doc["request"]["custom_id"])

    return gpt_req_ids_found


async def find_completed_gpt_batch_requests_by_custom_ids(
    gpt_batch_request_custom_ids: list[GPTBatchRequestCustomID],
) -> dict[GPTBatchRequestCustomID, GPTBatchRequest]:
    if gpt_batch_request_custom_ids is None:
        raise ValueError("gpt_batch_request_custom_ids cannot be None")
    elif len(gpt_batch_request_custom_ids) == 0:
        raise ValueError("gpt_batch_request_custom_ids cannot be empty")
    elif any(cid is None for cid in gpt_batch_request_custom_ids):
        raise ValueError("gpt_batch_request_custom_ids cannot contain None values")

    request_map: dict[GPTBatchRequestCustomID, GPTBatchRequest] = dict()
    async for gpt_req in GPTBatchRequest.find(
        {
            "request.custom_id": {"$in": gpt_batch_request_custom_ids},
            "batch_id": {"$ne": None},
            "response_blob": {"$ne": None},
        },
    ):
        request_map[gpt_req.request.custom_id] = gpt_req

    return request_map


async def find_completed_gpt_batch_request_ids_only(
    gpt_batch_request_custom_ids: list[GPTBatchRequestCustomID | None],
) -> set[GPTBatchRequestCustomID]:
    if gpt_batch_request_custom_ids is None:
        raise ValueError("gpt_batch_request_custom_ids cannot be None")
    elif len(gpt_batch_request_custom_ids) == 0:
        raise ValueError("gpt_batch_request_custom_ids cannot be empty")
    elif any(cid is None for cid in gpt_batch_request_custom_ids):
        raise ValueError("gpt_batch_request_custom_ids cannot contain None values")

    gpt_req_ids_found = set()
    collection = GPTBatchRequest.get_pymongo_collection()

    cursor = collection.find(
        {
            "request.custom_id": {"$in": gpt_batch_request_custom_ids},
            "batch_id": {"$ne": None},
            "response_blob": {"$ne": None},
        },
        projection={"request.custom_id": 1, "_id": 0},
    )

    async for doc in cursor:
        gpt_req_ids_found.add(doc["request"]["custom_id"])

    return gpt_req_ids_found


async def find_incomplete_gpt_batch_requests_by_custom_ids(
    gpt_batch_request_custom_ids: list[GPTBatchRequestCustomID],
) -> dict[GPTBatchRequestCustomID, GPTBatchRequest]:
    if gpt_batch_request_custom_ids is None:
        raise ValueError("gpt_batch_request_custom_ids cannot be None")
    elif len(gpt_batch_request_custom_ids) == 0:
        raise ValueError("gpt_batch_request_custom_ids cannot be empty")
    elif any(cid is None for cid in gpt_batch_request_custom_ids):
        raise ValueError("gpt_batch_request_custom_ids cannot contain None values")

    request_map: dict[GPTBatchRequestCustomID, GPTBatchRequest] = dict()
    async for gpt_req in GPTBatchRequest.find(
        {
            "request.custom_id": {"$in": gpt_batch_request_custom_ids},
            "batch_id": None,
            "response_blob": None,
        },
    ):
        request_map[gpt_req.request.custom_id] = gpt_req

    return request_map


async def get_custom_ids_for_batch(
    gpt_batch: GPTBatch,
) -> set[GPTBatchRequestCustomID]:
    logger.info(f"Getting custom IDs for GPT batch {gpt_batch.external_batch_id}")
    collection = GPTBatchRequest.get_pymongo_collection()

    query = {"batch_id": gpt_batch.external_batch_id}
    projection = {"request.custom_id": 1, "_id": 0}

    logger.info(
        f"Querying for batch requests with query: {query} and projection: {projection}"
    )

    docs = await collection.find(
        query,
        projection=projection,
    ).to_list(length=None)
    logger.info(
        f"Found {len(docs):,} batch requests for batch {gpt_batch.external_batch_id}"
    )

    custom_ids = [doc["request"]["custom_id"] for doc in docs]

    return set(custom_ids)
