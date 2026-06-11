from __future__ import (
    annotations,
)  # This allows you to write self-referential types without quotes

import logging
from abc import ABC, abstractmethod
from typing import Generic, Optional
from datetime import datetime

from core.models.db.manufacturer import Manufacturer
from core.models.db.deferred_manufacturer import DeferredManufacturer
from core.models.db.gpt_batch_request import GPTBatchRequest
from data_etl_app.models.types_and_enums import (
    LLMExtractedFieldTypeVar,
)
from open_ai_key_app.models.field_types import GPTBatchRequestCustomID
from scraper_app.models.scraped_text_file import ScrapedTextFile

logger = logging.getLogger(__name__)


class BaseNode(ABC, Generic[LLMExtractedFieldTypeVar]):
    """Base class for the phase of reconciliation for any deferred field. Assumes extraction is done."""

    def __init__(
        self,
        field_type: LLMExtractedFieldTypeVar,
        next_node: "BaseNode[LLMExtractedFieldTypeVar] | None",
    ) -> None:
        self.field_type: LLMExtractedFieldTypeVar = field_type
        self.next_node = next_node

    @abstractmethod
    async def execute(
        self,
        mfg: Manufacturer,
        deferred_mfg: DeferredManufacturer,
        scraped_text_file: ScrapedTextFile,
        timestamp: datetime,
        pipeline_context: PipelineContext,
        eager: bool,  # if True, dispatch all batch requests immediately and then check for completion, basically a sync execution of the entire phase
    ) -> None:
        pass


class PipelineContext:
    """Carries shared state for a single pipeline run.

    ``mfg_name`` is pre-populated by the orchestrator before any concept/keyword
    pipeline executes so that evidence nodes can embed the manufacturer name in
    their batch requests without needing it threaded through every method signature.

    The internal ``_results`` dict preserves the existing keying convention of
    ``pipeline_context[NodeClass]`` used throughout the extraction nodes.
    """

    def __init__(
        self,
        mfg_name: Optional[str] = None,
    ) -> None:
        self.mfg_name: Optional[str] = mfg_name
        self._results: dict[
            type[BaseNode], dict[GPTBatchRequestCustomID, GPTBatchRequest]
        ] = {}

    # --- dict-like access so existing ``pipeline_context[NodeClass]`` calls work unchanged ---

    def __getitem__(
        self, key: type[BaseNode]
    ) -> dict[GPTBatchRequestCustomID, GPTBatchRequest]:
        return self._results[key]

    def __setitem__(
        self,
        key: type[BaseNode],
        value: dict[GPTBatchRequestCustomID, GPTBatchRequest],
    ) -> None:
        self._results[key] = value
