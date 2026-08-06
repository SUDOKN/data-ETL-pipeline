from __future__ import annotations
import logging
from typing import TYPE_CHECKING

from llm_providers.db_models.gpt_batch_request import (
    GPTBatchRequest,
)
from llm_providers.models.file_objects.prompt import Prompt
from core.models.pipeline_nodes import PipelineContext
from core.models.pipeline_nodes.multi_stage.keyword.keyword_relationship_screening_node import (
    KeywordRelationshipScreeningNode,
)
from core.models.types_and_enums import KeywordTypeEnum
from llm_providers.field_types import BatchRequestIDType

if TYPE_CHECKING:
    from data_etl_app.models.pipeline_nodes.multi_stage.keyword.equipment.equipment_freehand_grounding_node import (
        EquipmentFreehandGroundingNode,
    )

logger = logging.getLogger(__name__)


class EquipmentRelationshipScreeningNode(KeywordRelationshipScreeningNode):
    """Phase 4: screen/normalize equipment relationships.

    Uses the equipment screening prompt (``equipment_phrase_relationship_screening``),
    which normalizes surviving phrases to ``<Brand/Model> <generalized category>``
    (or the category alone) and sets ``passed=False`` for any phrase for which no
    generalized category can be determined. Every input phrase is always present
    in the output (see ``LiveScreeningResults``).
    """

    def __init__(
        self,
        field_type: KeywordTypeEnum,
        next_node: EquipmentFreehandGroundingNode,
        phrase_relationship_screening_prompt: Prompt,
    ):
        super().__init__(
            field_type=field_type,
            phrase_relationship_screening_prompt=phrase_relationship_screening_prompt,
            next_node=next_node,
        )

    def get_upstream_phrase_relationship_map(
        self, pipeline_context: PipelineContext
    ) -> dict[BatchRequestIDType, GPTBatchRequest]:
        from data_etl_app.models.pipeline_nodes.multi_stage.keyword.equipment.equipment_relationship_node import (
            EquipmentRelationshipNode,
        )

        return pipeline_context[EquipmentRelationshipNode]
