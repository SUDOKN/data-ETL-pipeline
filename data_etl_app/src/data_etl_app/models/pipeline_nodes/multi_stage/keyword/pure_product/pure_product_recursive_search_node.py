from __future__ import annotations
import logging
from typing import TYPE_CHECKING

from core.models.db.gpt_batch_request import GPTBatchRequest
from core.models.file_objects.prompt import Prompt
from data_etl_app.models.pipeline_nodes.base.base_node import PipelineContext
from data_etl_app.models.pipeline_nodes.multi_stage.keyword.pure_product.pure_product_phrase_search_node import (
    PureProductPhraseSearchNode,
)
from data_etl_app.models.pipeline_nodes.multi_stage.keyword.base.keyword_recursive_search_node import (
    KeywordRecursiveSearchNode,
)
from core.models.types_and_enums import KeywordTypeEnum
from open_ai_key_app.models.field_types import GPTBatchRequestCustomID

if TYPE_CHECKING:
    from data_etl_app.models.pipeline_nodes.multi_stage.keyword.pure_product.pure_product_relationship_node import (
        PureProductRelationshipNode,
    )

logger = logging.getLogger(__name__)


class PureProductRecursiveSearchNode(KeywordRecursiveSearchNode):
    """Phase 2 for the pure-product branch."""

    def __init__(
        self,
        field_type: KeywordTypeEnum,
        next_node: PureProductRelationshipNode,
        second_search_prompt: Prompt,
    ):
        super().__init__(
            field_type=field_type,
            next_node=next_node,
            second_search_prompt=second_search_prompt,
        )

    def get_upstream_first_search_map(
        self, pipeline_context: PipelineContext
    ) -> dict[GPTBatchRequestCustomID, GPTBatchRequest]:
        return pipeline_context[PureProductPhraseSearchNode]
