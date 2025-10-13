from typing import Optional
from beanie import Document
from pydantic import Field
from datetime import datetime
import logging

from core.models.field_types import MfgETLDType, S3FileVersionIDType
from core.utils.time_util import get_current_time

from open_ai_key_app.models.field_types import GPTBatchRequestCustomID


from data_etl_app.models.deferred_binary_classification import (
    DeferredBinaryClassification,
)
from data_etl_app.models.deferred_keyword_extraction import (
    DeferredKeywordExtraction,
)
from data_etl_app.models.deferred_concept_extraction import (
    DeferredConceptExtraction,
)

logger = logging.getLogger(__name__)


class DeferredManufacturer(Document):
    mfg_etld1: MfgETLDType
    created_at: datetime = Field(default_factory=lambda: get_current_time())
    updated_at: datetime = Field(default_factory=lambda: get_current_time())
    scraped_text_file_version_id: S3FileVersionIDType

    is_manufacturer: Optional[DeferredBinaryClassification]
    is_contract_manufacturer: Optional[DeferredBinaryClassification]
    is_product_manufacturer: Optional[DeferredBinaryClassification]

    addresses: Optional[list[GPTBatchRequestCustomID]]
    business_desc: Optional[GPTBatchRequestCustomID]

    products: Optional[DeferredKeywordExtraction]

    certificates: Optional[DeferredConceptExtraction]
    industries: Optional[DeferredConceptExtraction]
    process_caps: Optional[DeferredConceptExtraction]
    material_caps: Optional[DeferredConceptExtraction]

    class Settings:
        name = "deferred_manufacturers"


"""
Indices for Manufacturers

db.deferred_manufacturers.createIndex(
  {
    mfg_etld1: 1,
  },
  {
    name: "mfg_etld1_unique_idx",
    unique: true
  }
);
"""
