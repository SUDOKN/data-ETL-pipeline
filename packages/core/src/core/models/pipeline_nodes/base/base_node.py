from __future__ import (
    annotations,
)  # This allows you to write self-referential types without quotes

import logging
from abc import ABC, abstractmethod
from typing import ClassVar, TypeVar, Generic, Optional
from datetime import datetime

from core.models.extraction_subject import (
    AbstractExtractionSubject,
    AbstractDeferredExtractionSubject,
)
from llm_providers.db_models.gpt_batch_request import (
    GPTBatchRequest,
)
from core.models.field_types import (
    LLMExtractedFieldTypeVar,
)
from core.models.pipeline_nodes.base.pipeline_stage import (
    PipelineStage,
    StageToggles,
)
from llm_providers.field_types import BatchRequestIDType
from scraper.models.s3.scraped_text_file import ScrapedTextFile

logger = logging.getLogger(__name__)


ResultT = TypeVar("ResultT")


class BaseNode(ABC, Generic[LLMExtractedFieldTypeVar, ResultT]):
    """Base class for the phase of reconciliation for any deferred field. Assumes extraction is done."""

    # Which phase this node is, for the per-run stage toggles and for the
    # partial dump a stopped run writes. Declared on the shared base of each
    # phase (e.g. LLMPhraseRelationshipNode), never on the per-field leaves —
    # ContractProductRelationshipNode and ConceptRelationshipNode are the same
    # stage of two different pipelines.
    stage: ClassVar[PipelineStage]

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

    async def stop_if_stage_disabled(
        self,
        subject: AbstractExtractionSubject,
        deferred_subject: AbstractDeferredExtractionSubject,
        timestamp: datetime,
        pipeline_context: PipelineContext,
    ) -> bool:
        """True when this stage is switched off for this run — the caller returns.

        The node does no work AND does not call its successor: the first
        disabled stage is where the run stops. See
        ``core.models.pipeline_nodes.base.pipeline_stage`` for why this is a
        hard stop rather than a pass-through.

        Every ``execute`` calls this first, including the three that override it
        wholesale (the recursive base's eager convergence loop, and the two
        reconcile bases) — a check that lived only in
        ``BaseLLMExtractionNode.execute`` would let those run regardless.
        """
        if pipeline_context.stage_toggles.is_enabled(self.field_type, self.stage):
            return False

        logger.info(
            f"[{subject.subject_unique_id}] 🛑 {self.__class__.__name__} "
            f"('{self.field_type.name}'): stage '{self.stage.value}' is disabled for "
            f"this run. Stopping here; "
            f"{self.next_node.__class__.__name__ if self.next_node else 'nothing further'} "
            f"and everything after it will not run."
        )
        extraction_requests = getattr(deferred_subject, self.field_type.name, None)
        if extraction_requests:
            # Imported here, not at module scope: the dump service needs
            # PipelineContext, which lives in this module.
            from core.services.pipeline_nodes.partial_run_dump import (
                write_partial_run_dump,
            )

            await write_partial_run_dump(
                subject_unique_id=subject.subject_unique_id,
                field_type=self.field_type,
                stopped_at=self.stage,
                disabled_stages=pipeline_context.stage_toggles.disabled_stages(
                    self.field_type
                ),
                extraction_requests=extraction_requests,
                pipeline_context=pipeline_context,
                timestamp=timestamp,
            )
        return True


class PipelineContext:
    """Carries shared state for a single pipeline run.

    ``subject_name`` is pre-populated by the orchestrator before any concept/keyword
    pipeline executes so that phrase_relationship nodes can embed the subject name in
    their batch requests without needing it threaded through every method signature.

    The internal ``_results`` dict preserves the existing keying convention of
    ``pipeline_context[NodeClass]`` used throughout the extraction nodes.

    ``stage_toggles`` decides which stages are allowed to run; the default
    instance disables nothing, so a context built without one runs the whole
    chain. ``stages_completed`` is the ordered record of the stages that did
    run, which is what lets a stopped run dump what it has.
    """

    def __init__(
        self,
        subject_name: Optional[str] = None,
        stage_toggles: Optional[StageToggles] = None,
    ) -> None:
        self.subject_name: Optional[str] = subject_name
        self.stage_toggles: StageToggles = stage_toggles or StageToggles()
        self._results: dict[
            type[BaseNode], dict[BatchRequestIDType, GPTBatchRequest]
        ] = {}
        self._stages_completed: list[tuple[PipelineStage, type[BaseNode]]] = []

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
        # A node publishes here only once all of its own requests are complete,
        # so arrival order IS completion order — which is what the partial dump
        # walks. Re-publishing (the recursive nodes can) must not duplicate the
        # entry or the dump would parse the same stage twice.
        if key not in self._results:
            stage = getattr(key, "stage", None)
            if stage is not None:
                self._stages_completed.append((stage, key))
        self._results[key] = value

    @property
    def stages_completed(self) -> list[tuple[PipelineStage, type[BaseNode]]]:
        """Stages that published a completed request map, in completion order."""
        return list(self._stages_completed)

    def node_class_for(self, stage: PipelineStage) -> Optional[type[BaseNode]]:
        for completed_stage, node_class in self._stages_completed:
            if completed_stage is stage:
                return node_class
        return None
