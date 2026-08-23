from __future__ import annotations

import logging
from abc import abstractmethod
from datetime import datetime

from core.models.extraction_subject import (
    AbstractExtractionSubject,
    AbstractDeferredExtractionSubject,
)

from core.models.chunking_strat import ChunkingStrategy
from core.utils.floor_scan import drop_excluded_pages
from core.models.pipeline_nodes.base.base_node import (
    BaseNode,
    PipelineContext,
    ResultT,
)
from core.models.pipeline_nodes.base.base_llm_extraction_node import (
    BaseLLMExtractionNode,
)
from core.models.field_types import (
    LLMExtractedFieldTypeVar,
)
from pure_utils.dict_diff import find_diffs
from scraper.models.s3.scraped_text_file import ScrapedTextFile
from typing import ClassVar
from core.models.pipeline_nodes.base.pipeline_stage import PipelineStage

logger = logging.getLogger(__name__)

# Timestamps say when a metadata object was built, not what a run will send, and
# they move on every process start. Everything else in the tree decides either
# what a request contains or which request it is, so it has to match.
_VOLATILE_METADATA_FIELDS = {"created_at"}


class StaleExtractionMetadataError(ValueError):
    """A deferred field was built under metadata that is no longer current.

    Extraction always runs on the latest resources — the prompt bytes come from
    whatever ``PromptService`` pinned at startup, the vocabulary from the latest
    ontology — while request custom_ids are stamped from the metadata STORED on
    the deferred field. Let those two drift apart and the pipeline builds a
    request from today's prompt and files it under yesterday's version id, which
    makes the record assert a provenance it does not have. Rather than resolve
    every resource by its stored version (two sources of truth, forever), the
    resume path refuses to run and the invariant becomes: if it ran, stored and
    latest agreed.

    A ValueError subclass so existing handlers keep catching it.
    """


class PrefillNode(BaseNode[LLMExtractedFieldTypeVar, None]):
    stage: ClassVar[PipelineStage] = PipelineStage.prefill

    next_node: BaseLLMExtractionNode

    def raise_if_metadata_is_stale(
        self,
        *,
        subject_unique_id: str,
        stored_metadata_dump: dict,
        latest_metadata_dump: dict,
    ) -> None:
        """Refuse to resume a field whose stored metadata is not the current one.

        Deliberately unfiltered: any difference stops the run. Narrowing this to
        a few "important" fields is what let a republished prompt sail through —
        the narrowed guard could not see a changed ``prompt_version_id``, so a
        resumed subject silently replayed answers the old prompt produced, and an
        A/B measured nothing.
        """
        diffs = find_diffs(
            stored_metadata_dump,
            latest_metadata_dump,
            exclude=_VOLATILE_METADATA_FIELDS,
        )
        if not diffs:
            return

        differing = "\n".join(
            f"  - {field}: {stored!r} -> {latest!r}"
            for field, (stored, latest) in sorted(diffs.items())
        )
        raise StaleExtractionMetadataError(
            f"Older metadata detected for "
            f"{subject_unique_id}.{self.field_type.name}: this field was deferred "
            f"under metadata that no longer matches what is configured now, and "
            f"extraction always runs on the latest resources. Differing fields "
            f"(stored -> latest):\n{differing}\n"
            f"To proceed, set deferred.{self.field_type.name} to null for this "
            f"subject and re-run. Its batch requests do not need deleting: the "
            f"ones whose custom_id still matches the latest metadata are reused, "
            f"so only what actually changed is re-sent."
        )

    def __init__(
        self,
        field_type: LLMExtractedFieldTypeVar,
        chunk_strategy: ChunkingStrategy,
        next_node: BaseLLMExtractionNode,
    ) -> None:
        super().__init__(field_type=field_type, next_node=next_node)
        self.chunk_strategy = chunk_strategy

    def apply_page_exclusion(
        self,
        scraped_text_file: ScrapedTextFile,
        pipeline_context: PipelineContext,
    ) -> ScrapedTextFile:
        """The text this pipeline chunks and every later node reads.

        Under ``ChunkingStrategy.drop_excluded_pages`` the excluded (legal) pages
        are removed from the text BEFORE chunking (2026-08-23, user decision), so
        they cost no budget and never form a window of their own; the trimmed
        text is handed down the chain as a copy of the scraped-text object and
        set on ``PipelineContext.subject_text``, and what was removed is recorded
        on the context for the dump. Runs on EVERY execute — fresh and resumed
        alike — because the stored chunk and window bounds are offsets into the
        trimmed text, and trimming is deterministic under the pinned rule version
        (``page_exclusion_version`` on the strategy, checked by the staleness
        guard). The copy keeps the S3 object's own ``num_tokens``; the removed
        volume is on the exclusion record.
        """
        if not self.chunk_strategy.drop_excluded_pages:
            pipeline_context.subject_text = scraped_text_file.text
            return scraped_text_file
        exclusion = drop_excluded_pages(scraped_text_file.text)
        if exclusion.version != self.chunk_strategy.page_exclusion_version:
            raise ValueError(
                f"Page exclusion rule version {exclusion.version!r} in code does not "
                f"match the strategy's {self.chunk_strategy.page_exclusion_version!r} "
                f"for '{self.field_type.name}'."
            )
        pipeline_context.page_exclusion = exclusion
        if exclusion.dropped:
            logger.info(
                f"[{scraped_text_file.subject_unique_id}] '{self.field_type.name}': dropped "
                f"{len(exclusion.dropped)} excluded page(s), {exclusion.chars_removed:,} of "
                f"{exclusion.chars_before:,} chars, before chunking: "
                f"{[p.url for p in exclusion.dropped]}"
            )
            scraped_text_file = scraped_text_file.model_copy(update={"text": exclusion.text})
        pipeline_context.subject_text = scraped_text_file.text
        return scraped_text_file

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
        await self.next_node.execute(
            subject=subject,
            deferred_subject=deferred_subject,
            scraped_text_file=scraped_text_file,
            pipeline_context=pipeline_context,
            timestamp=timestamp,
            eager=eager,
        )
