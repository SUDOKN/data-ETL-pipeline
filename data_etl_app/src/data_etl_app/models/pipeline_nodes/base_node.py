import logging
from abc import ABC
from typing import TypeVar, Generic

from data_etl_app.models.types_and_enums import GenericFieldTypeEnum

logger = logging.getLogger(__name__)

# Define a type variable that must be a GenericFieldTypeEnum
GenericFieldTypeVar = TypeVar("GenericFieldTypeVar", bound=GenericFieldTypeEnum)


class BaseNode(ABC, Generic[GenericFieldTypeVar]):
    """Base class for the phase of reconciliation for any deferred field. Assumes extraction is done."""

    def __init__(self, field_type: GenericFieldTypeVar) -> None:
        self.field_type: GenericFieldTypeVar = field_type
