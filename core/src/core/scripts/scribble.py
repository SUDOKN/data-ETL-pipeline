from aiobotocore.session import get_session
import asyncio
import logging
from pymongo import ReplaceOne

from pymongo.errors import BulkWriteError
from core.models.db.manufacturer import Manufacturer
from core.utils.aws.s3.s3_client_util import make_s3_client
from core.utils.aws.s3.scraped_text_util import (
    download_scraped_text_from_s3_by_mfg_etld1,
)
from core.utils.mongo_client import (
    init_db,
)

from open_ai_key_app.models.gpt_model import (
    GPT_4o_mini,
    GPTModel,
)

from core.models.binary_classification_result import (
    BinaryClassificationStats,
    ChunkBinaryClassificationResult,
)
from data_etl_app.services.knowledge.prompt_service import prompt_service
from data_etl_app.utils.chunk_util import (
    get_chunks_respecting_line_boundaries,
)

logger = logging.getLogger(__name__)


async def main():
    await init_db()
    print("Database initialized.")
    collection = Manufacturer.get_pymongo_collection()
    print("Count:", await collection.count_documents({}))
    # cursor = collection.find({})
    # async for doc in cursor:
    #     print(doc)
    doc = await collection.find_one({"etld1": "01com.com"})
    print(doc)
    print("done")


if __name__ == "__main__":
    asyncio.run(main())
