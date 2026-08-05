from __future__ import annotations
import logging
from typing import TYPE_CHECKING

from packages.llm_providers.src.llm_providers.models.file_objects.prompt import Prompt
from packages.core.src.core.models.pipeline_nodes.multi_stage.keyword.keyword_phrase_search_node import (
    KeywordPhraseSearchNode,
)
from packages.core.src.core.models.types_and_enums import KeywordTypeEnum

if TYPE_CHECKING:
    from apps.data_etl_app.src.data_etl_app.models.pipeline_nodes.multi_stage.keyword.equipment.equipment_recursive_search_node import (
        EquipmentRecursiveSearchNode,
    )

logger = logging.getLogger(__name__)


class EquipmentPhraseSearchNode(KeywordPhraseSearchNode):
    """Phase 1: LLM search for equipment phrases (single-track, no pure/contract split)."""

    def __init__(
        self,
        field_type: KeywordTypeEnum,
        search_prompt: Prompt,
        next_node: EquipmentRecursiveSearchNode,
    ):
        super().__init__(
            field_type=field_type,
            search_prompt=search_prompt,
            next_node=next_node,
        )
