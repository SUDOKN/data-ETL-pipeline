from __future__ import annotations
import logging
from typing import TYPE_CHECKING

from core.models.file_objects.prompt import Prompt
from data_etl_app.models.pipeline_nodes.multi_stage.base.llm_phrase_relationship_screening_node import (
    LLMPhraseRelationshipScreeningNode,
)
from data_etl_app.models.types_and_enums import KeywordTypeEnum

if TYPE_CHECKING:
    from data_etl_app.models.pipeline_nodes.multi_stage.keyword.base.keyword_freehand_grounding_node import (
        KeywordFreehandGroundingNode,
    )

logger = logging.getLogger(__name__)


class KeywordRelationshipScreeningNode(
    LLMPhraseRelationshipScreeningNode[KeywordTypeEnum]
):
    """Base class: phase 4, screen relationship phrases.

    This is a BASE class: ``get_upstream_phrase_relationship_map`` is left
    unimplemented (inherited from :class:`LLMPhraseRelationshipScreeningNode`).
    Concrete leaves such as ``PureProductRelationshipScreeningNode`` /
    ``ContractProductRelationshipScreeningNode`` must implement it.
    """

    def __init__(
        self,
        field_type: KeywordTypeEnum,
        next_node: KeywordFreehandGroundingNode,
        phrase_relationship_screening_prompt: Prompt,
    ):
        super().__init__(
            field_type=field_type,
            phrase_relationship_screening_prompt=phrase_relationship_screening_prompt,
            next_node=next_node,
        )
