import logging
from datetime import datetime
from abc import abstractmethod

from core.models.db.manufacturer import Manufacturer
from data_etl_app.models.pipeline_nodes.base_node import BaseNode, GenericFieldTypeVar
from core.models.db.deferred_manufacturer import DeferredManufacturer

logger = logging.getLogger(__name__)


# Strategy Pattern
class ReconcileNode(BaseNode[GenericFieldTypeVar]):
    """Base class for the phase of reconciliation for any deferred field. Assumes extraction is done."""

    def __init__(self, field_type: GenericFieldTypeVar) -> None:
        self.field_type: GenericFieldTypeVar = field_type

    @abstractmethod  # Child classes must implement this method
    async def reconcile(
        self,
        mfg: Manufacturer,
        deferred_mfg: DeferredManufacturer,
        timestamp: datetime,
        force: bool = False,
    ) -> Manufacturer:
        """
        Reconcile the manufacturer data.
        """
        logger.info(
            f"[{deferred_mfg.mfg_etld1}] 🔄 ReconcileNode.reconcile() called for field '{self.field_type.name}'"
        )
        logger.info(
            f"[{deferred_mfg.mfg_etld1}] 🧹 Setting deferred_mfg.{self.field_type.name} field to None for cleanup"
        )
        setattr(deferred_mfg, self.field_type.name, None)
