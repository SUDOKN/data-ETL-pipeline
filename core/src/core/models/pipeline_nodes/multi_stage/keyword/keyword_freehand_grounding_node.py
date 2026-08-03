from __future__ import annotations
import logging
from typing import TYPE_CHECKING

from core.models.file_objects.prompt import Prompt
from data_etl_app.models.pipeline_nodes.multi_stage.base.llm_phrase_freehand_grounding_node import (
    LLMPhraseFreehandGroundingNode,
)
from core.models.types_and_enums import KeywordTypeEnum

if TYPE_CHECKING:
    from data_etl_app.models.pipeline_nodes.multi_stage.keyword.base.keyword_reconcile_node import (
        KeywordReconcileNode,
    )

logger = logging.getLogger(__name__)


class KeywordFreehandGroundingNode(LLMPhraseFreehandGroundingNode[KeywordTypeEnum]):
    """Base class: phase 5, ground screened phrases into free-text product labels.

    This is a BASE class: ``get_upstream_phrase_relationship_map`` /
    ``get_upstream_phrase_screening_map`` are left unimplemented (inherited from
    :class:`LLMPhraseFreehandGroundingNode`). Concrete leaves such as
    ``PureProductFreehandGroundingNode`` / ``ContractProductFreehandGroundingNode``
    must implement them.
    """

    def __init__(
        self,
        field_type: KeywordTypeEnum,
        next_node: KeywordReconcileNode,
        phrase_freehand_grounding_prompt: Prompt,
    ):
        super().__init__(
            field_type=field_type,
            phrase_freehand_grounding_prompt=phrase_freehand_grounding_prompt,
            next_node=next_node,
        )
