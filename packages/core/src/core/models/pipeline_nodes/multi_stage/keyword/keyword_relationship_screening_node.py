from __future__ import annotations
import logging
from datetime import datetime
from typing import TYPE_CHECKING

from llm_providers.db_models.gpt_batch_request import (
    GPTBatchRequest,
)
from llm_providers.models.file_objects.prompt import Prompt
from core.models.deferred_extraction.deferred_keyword_extraction import (
    KeywordExtractionRequestBundle,
)
from core.models.pipeline_nodes.base.base_node import PipelineContext
from core.models.pipeline_nodes.multi_stage.base.llm_phrase_relationship_screening_node import (
    LLMPhraseRelationshipScreeningNode,
)
from core.models.field_types import ExtractionFieldType
from core.models.rule_catalog import STAGE_FREEHAND_GROUNDING
from core.services.pipeline_nodes.multi_stage.llm_grounding_node_service import (
    get_record_grounding_result,
)
from core.services.pipeline_nodes.multi_stage.pipeline_v2_derivations import (
    candidates_for_screening,
)
from core.services.rule_catalog_registry import get_rule_catalog
from llm_providers.field_types import BatchRequestIDType

if TYPE_CHECKING:
    from core.models.pipeline_nodes.multi_stage.keyword.keyword_reconcile_node import (
        KeywordReconcileNode,
    )

logger = logging.getLogger(__name__)


class KeywordRelationshipScreeningNode(
    LLMPhraseRelationshipScreeningNode[ExtractionFieldType]
):
    """Base class: the keyword families' screening, downstream of freehand
    grounding in v2 — its candidates are the freehand pass's minted labels.

    This is a BASE class: ``get_upstream_phrase_relationship_map`` and
    ``get_upstream_freehand_grounding_map`` are left unimplemented. Concrete
    leaves such as ``PureProductRelationshipScreeningNode`` /
    ``ContractProductRelationshipScreeningNode`` must implement them.
    """

    def __init__(
        self,
        field_type: ExtractionFieldType,
        next_node: KeywordReconcileNode,
        phrase_relationship_screening_prompt: Prompt,
    ):
        super().__init__(
            field_type=field_type,
            phrase_relationship_screening_prompt=phrase_relationship_screening_prompt,
            next_node=next_node,
        )

    def get_upstream_freehand_grounding_map(
        self, pipeline_context: PipelineContext
    ) -> dict[BatchRequestIDType, GPTBatchRequest]:
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement get_upstream_freehand_grounding_map"
        )

    async def get_chunk_candidates_by_record(
        self,
        subject_unique_id: str,
        chunk_bounds: str,
        extraction_bundle: KeywordExtractionRequestBundle,
        pipeline_context: PipelineContext,
        timestamp: datetime,
    ) -> dict[str, list[str]]:
        freehand_results = await get_record_grounding_result(
            stage_label="freehand grounding",
            subject_unique_id=subject_unique_id,
            field_name=self.field_type.name,
            chunk_bounds=chunk_bounds,
            catalog=get_rule_catalog(STAGE_FREEHAND_GROUNDING, self.field_type.name),
            group_req_ids=extraction_bundle.llm_phrase_freehand_grounding_req_ids,
            completed_request_map=self.get_upstream_freehand_grounding_map(
                pipeline_context
            ),
            timestamp=timestamp,
        )
        return candidates_for_screening(freehand_results)
