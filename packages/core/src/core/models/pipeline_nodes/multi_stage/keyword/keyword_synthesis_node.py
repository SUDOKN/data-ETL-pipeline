from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from llm_providers.models.file_objects.prompt import Prompt

from core.models.field_types import ExtractionFieldType
from core.models.pipeline_nodes.multi_stage.base.llm_phrase_synthesis_node import (
    LLMPhraseSynthesisNode,
)

if TYPE_CHECKING:
    from core.models.pipeline_nodes.multi_stage.keyword.keyword_freehand_grounding_node import (
        KeywordFreehandGroundingNode,
    )

logger = logging.getLogger(__name__)


class KeywordSynthesisNode(LLMPhraseSynthesisNode[ExtractionFieldType]):
    """BASE class for the keyword branches' v3 phase 3.2: narrows the
    constructor, leaves the upstream-map accessors abstract — the app-side
    leaves (pure product / contract product / equipment) point at their own
    search nodes, and the contract leaf shares the pure-product identity."""

    def __init__(
        self,
        field_type: ExtractionFieldType,
        phrase_synthesis_prompt: Prompt,
        next_node: KeywordFreehandGroundingNode,
    ):
        super().__init__(
            field_type=field_type,
            phrase_synthesis_prompt=phrase_synthesis_prompt,
            next_node=next_node,
        )
