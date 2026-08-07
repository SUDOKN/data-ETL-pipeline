from __future__ import annotations
import logging
from typing import TYPE_CHECKING

from llm_providers.models.file_objects.prompt import Prompt
from core.models.pipeline_nodes.multi_stage.base.llm_phrase_recursive_search_node import (
    LLMPhraseRecursiveSearchNode,
)
from core.models.field_types import (
    ExtractionFieldType,
)

if TYPE_CHECKING:
    from core.models.pipeline_nodes.multi_stage.keyword.keyword_relationship_node import (
        KeywordRelationshipNode,
    )

logger = logging.getLogger(__name__)


class KeywordRecursiveSearchNode(LLMPhraseRecursiveSearchNode[ExtractionFieldType]):
    """Base class for recursive keyword phrase search (round 1 -> relationship phase).

    This is a BASE class: it narrows the constructor types but leaves
    ``get_upstream_first_search_map`` unimplemented (inherited from
    :class:`LLMPhraseRecursiveSearchNode`). Concrete leaves such as
    ``PureProductRecursiveSearchNode`` / ``ContractProductRecursiveSearchNode``
    must implement it to point at their own sibling search-node class.
    """

    def __init__(
        self,
        field_type: ExtractionFieldType,
        next_node: KeywordRelationshipNode,
        second_search_prompt: Prompt,
    ):
        super().__init__(
            field_type=field_type,
            next_node=next_node,
            recursive_search_prompt=second_search_prompt,
        )
