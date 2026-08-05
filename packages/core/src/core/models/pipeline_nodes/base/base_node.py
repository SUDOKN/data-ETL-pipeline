from __future__ import (
    annotations,
)  # This allows you to write self-referential types without quotes

import logging
from abc import ABC, abstractmethod
from typing import TypeVar, Generic, Optional
from datetime import datetime

from packages.core.src.core.models.extraction_subject import (
    AbstractExtractionSubject,
    AbstractDeferredExtractionSubject,
)
from packages.llm_providers.src.llm_providers.db_models.gpt_batch_request import (
    GPTBatchRequest,
)
from packages.core.src.core.models.types_and_enums import (
    LLMExtractedFieldTypeVar,
)
from packages.llm_providers.src.llm_providers.field_types import BatchRequestIDType
from packages.infra.src.infra.models.s3.scraped_text_file import ScrapedTextFile

logger = logging.getLogger(__name__)


ResultT = TypeVar("ResultT")


class BaseNode(ABC, Generic[LLMExtractedFieldTypeVar, ResultT]):
    """Base class for the phase of reconciliation for any deferred field. Assumes extraction is done."""

    def __init__(
        self,
        field_type: LLMExtractedFieldTypeVar,
        next_node: "BaseNode[LLMExtractedFieldTypeVar, ResultT] | None",
    ) -> None:
        self.field_type: LLMExtractedFieldTypeVar = field_type
        self.next_node = next_node

    @abstractmethod
    async def execute(
        self,
        subject: AbstractExtractionSubject,
        deferred_subject: AbstractDeferredExtractionSubject,
        scraped_text_file: ScrapedTextFile,
        timestamp: datetime,
        pipeline_context: PipelineContext,
        eager: bool,  # if True, dispatch all batch requests immediately and then check for completion, basically a sync execution of the entire phase
    ) -> None:
        pass


class PipelineContext:
    """Carries shared state for a single pipeline run.

    ``subject_name`` is pre-populated by the orchestrator before any concept/keyword
    pipeline executes so that phrase_relationship nodes can embed the manufacturer name in
    their batch requests without needing it threaded through every method signature.

    The internal ``_results`` dict preserves the existing keying convention of
    ``pipeline_context[NodeClass]`` used throughout the extraction nodes.
    """

    def __init__(
        self,
        subject_name: Optional[str] = None,
    ) -> None:
        self.subject_name: Optional[str] = subject_name
        self._results: dict[
            type[BaseNode], dict[BatchRequestIDType, GPTBatchRequest]
        ] = {}

    # --- dict-like access so existing ``pipeline_context[NodeClass]`` calls work unchanged ---

    def __getitem__(
        self, key: type[BaseNode]
    ) -> dict[BatchRequestIDType, GPTBatchRequest]:
        return self._results[key]

    def __setitem__(
        self,
        key: type[BaseNode],
        value: dict[BatchRequestIDType, GPTBatchRequest],
    ) -> None:
        self._results[key] = value
