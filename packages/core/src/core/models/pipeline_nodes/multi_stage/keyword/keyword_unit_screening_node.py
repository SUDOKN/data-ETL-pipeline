"""The keyword families' unit screening (Step 2, 2026-09-21): one wave over
the freehand pass's minted candidates. A BASE class — the app's per-field
nodes supply the upstream synthesis and freehand maps (and, through the
factory, the field's own catalog: products, contract products, equipments).
"""

from __future__ import annotations

import logging
from datetime import datetime

from llm_providers.db_models.gpt_batch_request import GPTBatchRequest
from llm_providers.field_types import BatchRequestIDType

from core.models.deferred_extraction.deferred_keyword_extraction import (
    KeywordExtractionRequestBundle,
)
from core.models.deferred_extraction.deferred_phrase_extraction_requests import (
    LLMPhraseExtractionRequestBundle,
)
from core.models.pipeline_nodes.base.base_node import PipelineContext
from core.models.pipeline_nodes.multi_stage.base.llm_phrase_unit_screening_node import (
    LLMPhraseUnitScreeningNode,
)
from core.models.rule_catalog import STAGE_FREEHAND_GROUNDING
from core.services.pipeline_nodes.multi_stage.llm_grounding_node_service import (
    get_record_grounding_result,
)
from core.services.pipeline_nodes.multi_stage.stage_derivations import units_for_screening
from core.services.rule_catalog_registry import get_rule_catalog

logger = logging.getLogger(__name__)


class KeywordUnitScreeningNode(LLMPhraseUnitScreeningNode):
    def get_upstream_freehand_grounding_map(
        self, pipeline_context: PipelineContext
    ) -> dict[BatchRequestIDType, GPTBatchRequest]:
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement get_upstream_freehand_grounding_map"
        )

    async def get_chunk_units(
        self,
        subject_unique_id: str,
        chunk_bounds: str,
        extraction_bundle: LLMPhraseExtractionRequestBundle,
        pipeline_context: PipelineContext,
        timestamp: datetime,
    ) -> dict[str, list[str]]:
        if not isinstance(extraction_bundle, KeywordExtractionRequestBundle):
            raise TypeError(f"{self.__class__.__name__}: expected a keyword bundle, got {type(extraction_bundle).__name__}")
        freehand = await get_record_grounding_result(
            stage_label="freehand grounding",
            subject_unique_id=subject_unique_id,
            field_name=self.field_type.name,
            chunk_bounds=chunk_bounds,
            catalog=get_rule_catalog(STAGE_FREEHAND_GROUNDING, self.field_type.name),
            group_req_ids=extraction_bundle.llm_phrase_freehand_grounding_req_ids,
            retry_req_ids=extraction_bundle.llm_phrase_freehand_grounding_retry_req_ids,
            completed_request_map=self.get_upstream_freehand_grounding_map(pipeline_context),
            timestamp=timestamp,
        )
        return units_for_screening(freehand)
