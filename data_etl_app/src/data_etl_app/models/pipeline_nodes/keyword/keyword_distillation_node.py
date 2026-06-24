from __future__ import annotations
import logging
from typing import TYPE_CHECKING

from core.models.prompt import Prompt
from data_etl_app.models.types_and_enums import KeywordTypeEnum
from data_etl_app.models.pipeline_nodes.keyword.keyword_search_node import (
    KeywordSearchNode,
)
from data_etl_app.models.pipeline_nodes.distillation_node import DistillationNode

if TYPE_CHECKING:
    from data_etl_app.models.pipeline_nodes.keyword.keyword_reconcile_node import (
        KeywordReconcileNode,
    )

logger = logging.getLogger(__name__)


class KeywordDistillationNode(DistillationNode[KeywordTypeEnum]):
    """Phase 2: LLM distills keywords from the phrases with context of the text.

    Thin wrapper over the shared ``DistillationNode`` that narrows the constructor types and
    points the distillation phase at the upstream ``KeywordSearchNode`` results.
    """

    upstream_search_node_cls = KeywordSearchNode

    def __init__(
        self,
        field_type: KeywordTypeEnum,
        distillation_prompt: Prompt,
        next_node: KeywordReconcileNode,
    ):
        super().__init__(
            field_type=field_type,
            distillation_prompt=distillation_prompt,
            next_node=next_node,
        )
