from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from llm_providers.db_models.gpt_batch_request import GPTBatchRequest
from llm_providers.field_types import BatchRequestIDType
from llm_providers.models.file_objects.prompt import Prompt

from core.models.pipeline_nodes import PipelineContext
from core.models.pipeline_nodes.multi_stage.keyword.keyword_unit_screening_node import (
    KeywordUnitScreeningNode,
)
from data_etl_app.models.types_and_enums import KeywordTypeEnum

if TYPE_CHECKING:
    from data_etl_app.models.pipeline_nodes.multi_stage.keyword.pure_product.pure_product_reconcile_node import (
        PureProductReconcileNode,
    )

logger = logging.getLogger(__name__)


class PureProductUnitScreeningNode(KeywordUnitScreeningNode):
    """Step 2 unit screening for this branch, downstream of freehand grounding:
    one wave over the freehand pass's minted candidates."""

    def __init__(
        self,
        field_type: KeywordTypeEnum,
        next_node: PureProductReconcileNode,
        phrase_unit_screening_prompt: Prompt,
    ):
        super().__init__(
            field_type=field_type,
            next_node=next_node,
            phrase_unit_screening_prompt=phrase_unit_screening_prompt,
        )

    def get_upstream_synthesis_map(
        self, pipeline_context: PipelineContext
    ) -> dict[BatchRequestIDType, GPTBatchRequest]:
        from data_etl_app.models.pipeline_nodes.multi_stage.keyword.pure_product.pure_product_synthesis_node import (
            PureProductSynthesisNode,
        )

        return pipeline_context[PureProductSynthesisNode]

    def get_upstream_freehand_grounding_map(
        self, pipeline_context: PipelineContext
    ) -> dict[BatchRequestIDType, GPTBatchRequest]:
        from data_etl_app.models.pipeline_nodes.multi_stage.keyword.pure_product.pure_product_freehand_grounding_node import (
            PureProductFreehandGroundingNode,
        )

        return pipeline_context[PureProductFreehandGroundingNode]
