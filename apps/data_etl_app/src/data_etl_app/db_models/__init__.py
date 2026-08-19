"""Beanie document models registered by `data_etl_app`.

`DOCUMENT_MODELS` is the full set this app connects with: its own models plus the
models declared by every package it depends on.
"""

from beanie import Document

from core.db_models import DOCUMENT_MODELS as CORE_DOCUMENT_MODELS
from llm_providers.db_models import DOCUMENT_MODELS as LLM_PROVIDER_DOCUMENT_MODELS

from data_etl_app.db_models.binary_ground_truth import BinaryGroundTruth
from data_etl_app.db_models.deferred_manufacturer import DeferredManufacturer
from data_etl_app.db_models.llm_phrase_ground_truth import LLMPhraseGroundTruth
from data_etl_app.db_models.manufacturer import Manufacturer
from data_etl_app.db_models.manufacturer_user_form import ManufacturerUserForm
from data_etl_app.db_models.mep_request import MEPRequest
from data_etl_app.db_models.place import Place
from data_etl_app.db_models.user import User

APP_DOCUMENT_MODELS: list[type[Document]] = [
    BinaryGroundTruth,
    DeferredManufacturer,
    LLMPhraseGroundTruth,
    Manufacturer,
    ManufacturerUserForm,
    MEPRequest,
    Place,
    User,
]

DOCUMENT_MODELS: list[type[Document]] = [
    *APP_DOCUMENT_MODELS,
    *CORE_DOCUMENT_MODELS,
    *LLM_PROVIDER_DOCUMENT_MODELS,
]
