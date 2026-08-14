import logging
from beanie import Document
from datetime import datetime
from pydantic import BaseModel, Field
from typing import List, Optional

from core.field_types import (
    SubjectUniqueIDType,
)
from infra.field_types import (
    S3FileVersionIDType,
)
from infra.models.queue_items.to_scrape_item import Batch
from core.models.extraction_subject import (
    AbstractExtractionSubject,
)
from core.models.extraction_results.concept_extraction_results import (
    ConceptExtractionResults,
)
from core.models.extraction_results.keyword_extraction_results import (
    KeywordExtractionResults,
)
from core.models.extraction_results.binary_classification_result import (
    BinaryClassificationResult,
)
from data_etl_app.models.extraction_results.address_extraction_result import (
    AddressExtractionResult,
)
from data_etl_app.models.extraction_results.business_description_extraction_result import (
    BusinessDescriptionExtractionResult,
)

from pure_utils.time_util import get_current_time

logger = logging.getLogger(__name__)


class Manufacturer(Document, AbstractExtractionSubject):
    etld1: SubjectUniqueIDType  # canonical eTLD+1 derived from the initial URL used to identify the manufacturer
    etld1_accessible_at: (
        SubjectUniqueIDType  # eTLD+1 of the final landing URL reached during scraping
    )

    created_at: datetime = Field(default_factory=lambda: get_current_time())
    updated_at: datetime = Field(default_factory=lambda: get_current_time())
    scraped_text_file_num_tokens: int
    scraped_text_file_version_id: S3FileVersionIDType
    batches: list[Batch]

    name: Optional[str]
    founded_in: Optional[int]
    email_addresses: Optional[List[str]]
    num_employees: Optional[int]
    business_statuses: Optional[List[str]]
    primary_naics: Optional[str]
    secondary_naics: Optional[List[str]]

    # LLM extracted fields
    is_manufacturer: Optional[BinaryClassificationResult]
    is_contract_manufacturer: Optional[BinaryClassificationResult]
    is_product_manufacturer: Optional[BinaryClassificationResult]
    addresses: Optional[AddressExtractionResult]
    business_desc: Optional[BusinessDescriptionExtractionResult]

    products: Optional[KeywordExtractionResults]
    contract_products: Optional[KeywordExtractionResults]
    equipments: Optional[KeywordExtractionResults]

    conformity_attestations: Optional[ConceptExtractionResults]
    industries: Optional[ConceptExtractionResults]
    process_caps: Optional[ConceptExtractionResults]
    material_caps: Optional[ConceptExtractionResults]

    @property
    def subject_unique_id(self) -> str:
        return self.etld1

    class Settings:
        name = "manufacturers"


"""
Indexes for Manufacturers

db.manufacturers.createIndex(
  {
    etld1: 1,
  },
  {
    name: "mfg_etld1_unique_idx",
    unique: true
  }
);

db.manufacturers.createIndex(
  {
    etld1_accessible_at: 1,
  },
  {
    name: "mfg_etld1_accessible_at_idx",
    unique: true
  }
);
"""
