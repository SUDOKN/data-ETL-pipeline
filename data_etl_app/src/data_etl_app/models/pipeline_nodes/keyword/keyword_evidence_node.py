from __future__ import annotations
import logging
from typing import TYPE_CHECKING

from core.models.prompt import Prompt
from data_etl_app.models.types_and_enums import KeywordTypeEnum
from data_etl_app.models.pipeline_nodes.keyword.keyword_search_node import (
    KeywordSearchNode,
)
from data_etl_app.models.pipeline_nodes.evidence_node import EvidenceNode

if TYPE_CHECKING:
    from data_etl_app.models.pipeline_nodes.keyword.keyword_reconcile_node import (
        KeywordReconcileNode,
    )

logger = logging.getLogger(__name__)


class KeywordEvidenceNode(EvidenceNode[KeywordTypeEnum]):
    """Phase 2: LLM finds evidence in the text.

    Thin wrapper over the shared ``EvidenceNode`` that narrows the constructor types and
    points the evidence phase at the upstream ``KeywordSearchNode`` results.
    """

    upstream_search_node_cls = KeywordSearchNode

    def __init__(
        self,
        field_type: KeywordTypeEnum,
        evidence_prompt: Prompt,
        next_node: KeywordReconcileNode,
    ):
        super().__init__(
            field_type=field_type,
            evidence_prompt=evidence_prompt,
            next_node=next_node,
        )
