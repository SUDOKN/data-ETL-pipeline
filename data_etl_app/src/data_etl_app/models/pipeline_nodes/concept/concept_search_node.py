from __future__ import annotations
import logging
from typing import TYPE_CHECKING

from core.models.prompt import Prompt
from data_etl_app.models.pipeline_nodes.search_node import SearchNode
from data_etl_app.models.types_and_enums import (
    ConceptTypeEnum,
)

if TYPE_CHECKING:
    from data_etl_app.models.pipeline_nodes.concept.concept_evidence_node import (
        ConceptEvidenceNode,
    )


logger = logging.getLogger(__name__)


class ConceptSearchNode(SearchNode[ConceptTypeEnum]):
    """Phase 1: LLM Search for concepts"""

    def __init__(
        self,
        concept_type: ConceptTypeEnum,
        search_prompt: Prompt,
        next_node: ConceptEvidenceNode,
    ):
        super().__init__(
            field_type=concept_type,
            search_prompt=search_prompt,
            next_node=next_node,
        )
