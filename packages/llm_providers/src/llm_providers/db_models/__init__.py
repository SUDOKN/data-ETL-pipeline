"""Beanie document models owned by `llm_providers`.

Apps append this list to their own before calling `infra...mongo_client.init_db`.
"""

from beanie import Document

from llm_providers.db_models.api_key_bundle import APIKeyBundle
from llm_providers.db_models.extraction_error import ExtractionError
from llm_providers.db_models.gpt_batch import GPTBatch
from llm_providers.db_models.gpt_batch_request import GPTBatchRequest
from llm_providers.db_models.scraping_error import ScrapingError

DOCUMENT_MODELS: list[type[Document]] = [
    APIKeyBundle,
    ExtractionError,
    GPTBatch,
    GPTBatchRequest,
    ScrapingError,
]
