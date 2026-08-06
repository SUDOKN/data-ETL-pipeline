from __future__ import annotations
import logging
from typing import TYPE_CHECKING

from llm_providers.models.file_objects.prompt import Prompt
from core.models.types_and_enums import KeywordTypeEnum
from core.models.pipeline_nodes.multi_stage.base.llm_phrase_relationship_node import (
    LLMPhraseRelationshipNode,
)

if TYPE_CHECKING:
    from core.models.pipeline_nodes.multi_stage.keyword.keyword_relationship_screening_node import (
        KeywordRelationshipScreeningNode,
    )

logger = logging.getLogger(__name__)


class KeywordRelationshipNode(LLMPhraseRelationshipNode[KeywordTypeEnum]):
    """Base class: phase 3, LLM distills keywords from the phrases with context of the text.

    This is a BASE class: it narrows the constructor types but leaves
    ``get_upstream_phrase_search_map`` / ``get_upstream_recursive_search_map``
    unimplemented (inherited abstract from :class:`LLMPhraseRelationshipNode`).
    Concrete leaves such as ``PureProductRelationshipNode`` /
    ``ContractProductRelationshipNode`` must implement them.
    """

    def __init__(
        self,
        field_type: KeywordTypeEnum,
        phrase_relationship_prompt: Prompt,
        next_node: KeywordRelationshipScreeningNode,
    ):
        super().__init__(
            field_type=field_type,
            phrase_relationship_prompt=phrase_relationship_prompt,
            next_node=next_node,
        )
