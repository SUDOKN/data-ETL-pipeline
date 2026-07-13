from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING

from core.models.db.manufacturer import Manufacturer
from core.models.db.deferred_manufacturer import DeferredManufacturer
from core.models.llm_phrase_extraction_results import (
    ExtractionNodeMetadata,
    RecursiveSearchNodeMetadata,
)
from core.models.keyword_extraction_results import (
    KeywordExtractionMetadata,
)
from core.models.deferred_keyword_extraction import (
    DeferredKeywordExtractionRequests,
    KeywordExtractionRequestBundle,
)
from data_etl_app.models.pipeline_nodes.multi_stage.keyword.keyword_phrase_search_node import (
    KeywordPhraseSearchNode,
)
from data_etl_app.models.pipeline_nodes.base.base_prefill_node import PrefillNode
from data_etl_app.models.types_and_enums import KeywordTypeEnum
from data_etl_app.models.pipeline_nodes.base.base_node import PipelineContext
from data_etl_app.models.chunking_strat import ChunkingStrategy

if TYPE_CHECKING:
    from scraper_app.models.scraped_text_file import ScrapedTextFile


from data_etl_app.utils.chunk_util import get_chunks_respecting_line_boundaries
from data_etl_app.utils.dict_diff import find_diffs

logger = logging.getLogger(__name__)


class KeywordExtractionPrefillNode(PrefillNode[KeywordTypeEnum]):
    next_node: KeywordPhraseSearchNode

    def __init__(
        self,
        field_type: KeywordTypeEnum,
        chunk_strategy: ChunkingStrategy,
        next_node: KeywordPhraseSearchNode,
        ontology_version_id: str,
        llm_phrase_search_metadata: ExtractionNodeMetadata,
        llm_phrase_recursive_search_metadata: RecursiveSearchNodeMetadata,
        llm_phrase_relationship_metadata: ExtractionNodeMetadata,
        llm_phrase_relationship_screening_metadata: ExtractionNodeMetadata,
        llm_phrase_freehand_grounding_metadata: ExtractionNodeMetadata,
    ):
        super().__init__(
            field_type=field_type,
            chunk_strategy=chunk_strategy,
            next_node=next_node,
        )
        self.ontology_version_id = ontology_version_id
        self.llm_phrase_search_metadata = llm_phrase_search_metadata
        self.llm_phrase_recursive_search_metadata = llm_phrase_recursive_search_metadata
        self.llm_phrase_relationship_metadata = llm_phrase_relationship_metadata
        self.llm_phrase_relationship_screening_metadata = (
            llm_phrase_relationship_screening_metadata
        )
        self.llm_phrase_freehand_grounding_metadata = (
            llm_phrase_freehand_grounding_metadata
        )

    async def execute(
        self,
        mfg: Manufacturer,
        deferred_mfg: DeferredManufacturer,
        scraped_text_file: ScrapedTextFile,
        timestamp: datetime,
        pipeline_context: PipelineContext,
        eager: bool,
    ):
        latest_keyword_extraction_metadata = KeywordExtractionMetadata(
            created_at=timestamp,
            chunk_strat=self.chunk_strategy,
            ontology_version_id=self.ontology_version_id,
            llm_phrase_search=self.llm_phrase_search_metadata,
            llm_phrase_recursive_search=self.llm_phrase_recursive_search_metadata,
            llm_phrase_relationship=self.llm_phrase_relationship_metadata,
            llm_phrase_relationship_screening=self.llm_phrase_relationship_screening_metadata,
            llm_phrase_freehand_grounding=self.llm_phrase_freehand_grounding_metadata,
        )

        if not bool(getattr(deferred_mfg, self.field_type.name)):
            chunk_map = await get_chunks_respecting_line_boundaries(
                text=scraped_text_file.text,
                soft_limit_tokens=self.chunk_strategy.max_tokens_per_chunk,
                overlap_ratio=self.chunk_strategy.overlap,
                max_chunks=self.chunk_strategy.max_chunks,
                llm_model=self.llm_phrase_search_metadata.llm_model,
            )

            deferred_keyword_extraction = DeferredKeywordExtractionRequests(
                metadata=latest_keyword_extraction_metadata,
                chunked_request_map={
                    chunk_bounds: KeywordExtractionRequestBundle(
                        llm_phrase_search_req_id=None,
                        llm_phrase_recursive_search_req_ids=[],
                        llm_phrase_relationship_req_id=None,
                        llm_phrase_relationship_screening_req_id=None,
                        llm_phrase_freehand_grounding_req_id=None,
                    )
                    for chunk_bounds, _chunk_text in chunk_map.items()
                },
            )
            setattr(deferred_mfg, self.field_type.name, deferred_keyword_extraction)
            await deferred_mfg.save()
        else:
            deferred_keyword_extraction: DeferredKeywordExtractionRequests = getattr(
                deferred_mfg, self.field_type.name
            )
            existing = deferred_keyword_extraction.metadata.model_dump()
            latest = latest_keyword_extraction_metadata.model_dump()
            diffs = find_diffs(existing, latest, exclude={"created_at"})
            if diffs:
                raise ValueError(
                    f"Cannot proceed {__class__.__name__} for mfg:{mfg.etld1} as "
                    f"Metadata mismatch for {mfg.etld1}.{self.field_type.name} — differing fields: {diffs}"
                )

            logger.info(
                f"Chunking already done, resuming extraction for mfg_etdl1:{mfg.etld1}"
            )

        await self.next_node.execute(
            mfg=mfg,
            deferred_mfg=deferred_mfg,
            scraped_text_file=scraped_text_file,
            timestamp=timestamp,
            pipeline_context=pipeline_context,
            eager=eager,
        )
