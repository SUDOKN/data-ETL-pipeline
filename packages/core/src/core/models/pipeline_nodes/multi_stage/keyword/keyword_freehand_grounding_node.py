from __future__ import annotations
import logging
from typing import TYPE_CHECKING

from llm_providers.models.file_objects.prompt import Prompt
from core.models.pipeline_nodes.multi_stage.base.llm_phrase_freehand_grounding_node import (
    LLMPhraseFreehandGroundingNode,
)
from core.models.field_types import ExtractionFieldType

if TYPE_CHECKING:
    from core.models.pipeline_nodes.multi_stage.keyword.keyword_relationship_screening_node import (
        KeywordRelationshipScreeningNode,
    )

logger = logging.getLogger(__name__)


class KeywordFreehandGroundingNode(LLMPhraseFreehandGroundingNode[ExtractionFieldType]):
    """Base class: the keyword families' enumeration pass — minted candidates
    off the relationship records, ahead of screening (v2 order).

    This is a BASE class: ``get_upstream_phrase_relationship_map`` is left
    unimplemented (inherited from :class:`LLMPhraseFreehandGroundingNode`).
    Concrete leaves such as ``PureProductFreehandGroundingNode`` /
    ``ContractProductFreehandGroundingNode`` must implement it.
    """

    def __init__(
        self,
        field_type: ExtractionFieldType,
        next_node: KeywordRelationshipScreeningNode,
        phrase_freehand_grounding_prompt: Prompt,
    ):
        super().__init__(
            field_type=field_type,
            phrase_freehand_grounding_prompt=phrase_freehand_grounding_prompt,
            next_node=next_node,
        )
