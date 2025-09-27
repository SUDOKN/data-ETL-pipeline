from beanie import Document
import logging
from pydantic import Field
from datetime import datetime
from typing import List, Optional

from core.models.db.manufacturer import Address
from core.models.field_types import MfgETLDType
from core.utils.time_util import get_current_time

from data_etl_app.services.llm_powered.search.llm_search_service import (
    BusinessDescriptionResult,
)

logger = logging.getLogger(__name__)


# At any point, this takes precedence over originally extracted Manufacturer data
# In case we update ontology/re-run extraction, we can notify the user to re-submit the form
class ManufacturerUserForm(Document):
    author_email: str
    mfg_etld1: MfgETLDType  # foreign key to Manufacturer.etld1

    created_at: datetime = Field(default_factory=lambda: get_current_time())
    updated_at: datetime = Field(default_factory=lambda: get_current_time())

    name: Optional[str]

    # # these would exist if the user has provided ground truth for these fields
    # is_manufacturer_gt_id: Optional[str]
    # is_contract_manufacturer_gt_id: Optional[str]
    # is_product_manufacturer_gt_id: Optional[str]

    founded_in: Optional[int]
    email_addresses: Optional[List[str]]
    num_employees: Optional[int]
    business_statuses: Optional[List[str]]
    primary_naics: Optional[str]
    secondary_naics: Optional[List[str]]
    addresses: Optional[List[Address]]

    business_desc: BusinessDescriptionResult
    products: list[str]

    certificates: list[str]
    industries: list[str]
    process_caps: list[str]
    material_caps: list[str]

    notes: Optional[str]

    class Settings:
        name = "manufacturer_user_forms"


"""
Indexes in MongoDB for ManufacturerUserForm:

db.manufacturer_user_forms.createIndex(
  {
    mfg_etld1: 1,
  },
  {
    name: "mfg_user_form_unique_per_etld1",
    unique: true 
  }
)
"""
