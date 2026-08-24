from __future__ import annotations
import logging
from typing import TYPE_CHECKING

from llm_providers.db_models.gpt_batch_request import (
    GPTBatchRequest,
)
from llm_providers.models.file_objects.prompt import Prompt
from core.models.pipeline_nodes import PipelineContext
from core.models.pipeline_nodes.multi_stage.keyword.keyword_relationship_screening_node import (
    KeywordRelationshipScreeningNode,
)
from data_etl_app.models.types_and_enums import KeywordTypeEnum
from llm_providers.field_types import BatchRequestIDType

if TYPE_CHECKING:
    from data_etl_app.models.pipeline_nodes.multi_stage.keyword.pure_product.pure_product_reconcile_node import (
        PureProductReconcileNode,
    )

logger = logging.getLogger(__name__)


class PureProductRelationshipScreeningNode(KeywordRelationshipScreeningNode):
    """Screening for the pure-product branch, downstream of freehand grounding
    in v2 — it judges the freehand pass's minted candidates.

    Uses the pure-product screening prompt (``product_phrase_screening_pure_product``).
    """

    def __init__(
        self,
        field_type: KeywordTypeEnum,
        next_node: PureProductReconcileNode,
        phrase_relationship_screening_prompt: Prompt,
    ):
        super().__init__(
            field_type=field_type,
            phrase_relationship_screening_prompt=phrase_relationship_screening_prompt,
            next_node=next_node,
        )

    def get_upstream_mention_collection_map(
        self, pipeline_context: PipelineContext
    ) -> dict[BatchRequestIDType, GPTBatchRequest]:
        from data_etl_app.models.pipeline_nodes.multi_stage.keyword.pure_product.pure_product_mention_collection_node import (
            PureProductMentionCollectionNode,
        )

        return pipeline_context[PureProductMentionCollectionNode]

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
