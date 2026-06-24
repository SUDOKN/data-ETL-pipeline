from __future__ import annotations
import logging
from typing import TYPE_CHECKING

from core.models.prompt import Prompt
from data_etl_app.models.types_and_enums import ConceptTypeEnum
from data_etl_app.models.pipeline_nodes.concept.concept_search_node import (
    ConceptSearchNode,
)
from data_etl_app.models.pipeline_nodes.distillation_node import DistillationNode

if TYPE_CHECKING:
    from data_etl_app.models.pipeline_nodes.concept.concept_mapping_node import (
        ConceptMappingNode,
    )

logger = logging.getLogger(__name__)


class ConceptDistillationNode(DistillationNode[ConceptTypeEnum]):
    """Phase 2: LLM distills concepts from the phrases with context of the text.

    Thin wrapper over the shared ``DistillationNode`` that narrows the constructor types and
    points the distillation phase at the upstream ``ConceptSearchNode`` results.
    """

    upstream_search_node_cls = ConceptSearchNode

    def __init__(
        self,
        concept_type: ConceptTypeEnum,
        distillation_prompt: Prompt,
        next_node: ConceptMappingNode,
    ):
        super().__init__(
            field_type=concept_type,
            distillation_prompt=distillation_prompt,
            next_node=next_node,
        )
