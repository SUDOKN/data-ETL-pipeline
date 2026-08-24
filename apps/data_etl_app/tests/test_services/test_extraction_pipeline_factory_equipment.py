from datetime import datetime

from llm_providers.models.file_objects.prompt import Prompt
from llm_providers.models.llm_model import GPT_4o_mini
from llm_providers.models.open_ai.gpt_model_params import (
    GPTModelParams,
)

from core.models.chunking_strat import EQUIPMENT_CHUNKING_STRAT
from data_etl_app.models.pipeline_nodes import (
    EquipmentPhraseSearchNode,
    EquipmentRecursiveSearchNode,
    EquipmentMentionCollectionNode,
    EquipmentSynthesisNode,
    EquipmentRelationshipScreeningNode,
    EquipmentFreehandGroundingNode,
    EquipmentReconcileNode,
)
from data_etl_app.models.types_and_enums import KeywordTypeEnum
from data_etl_app.services.extraction_pipeline_factory import (
    ExtractionPipelineFactory,
)


def _make_prompt(name: str) -> Prompt:
    return Prompt(
        s3_version_id="test", name=name, text=f"{name} prompt text", num_tokens=5
    )


def test_keyword_type_enum_has_equipment():
    assert KeywordTypeEnum.equipments == "equipments"
    assert KeywordTypeEnum.equipments in list(KeywordTypeEnum)


def test_create_equipment_extraction_pipeline_builds_expected_node_chain():
    prefill = ExtractionPipelineFactory.create_equipment_extraction_pipeline(
        chunk_strategy=EQUIPMENT_CHUNKING_STRAT,
        ontology_version_id="test-ontology-version",
        search_prompt=_make_prompt("equipment_phrase_search"),
        recursive_search_prompt=_make_prompt("equipment_phrase_recursive_search"),
        phrase_mention_collection_prompt=_make_prompt("equipment_phrase_mention_collection"),
        phrase_synthesis_prompt=_make_prompt("equipment_phrase_synthesis"),
        phrase_relationship_screening_prompt=_make_prompt(
            "equipment_phrase_relationship_screening"
        ),
        phrase_freehand_grounding_prompt=_make_prompt(
            "equipment_phrase_freehand_grounding"
        ),
        llm_model=GPT_4o_mini,
        model_params=GPTModelParams.with_defaults(),
        created_at=datetime.now(),
    )

    assert prefill.field_type == KeywordTypeEnum.equipments

    search_node = prefill.next_node
    assert isinstance(search_node, EquipmentPhraseSearchNode)

    recursive_search_node = search_node.next_node
    assert isinstance(recursive_search_node, EquipmentRecursiveSearchNode)

    # v3 (3.1): mention collection replaced relationship.
    mention_node = recursive_search_node.next_node
    assert isinstance(mention_node, EquipmentMentionCollectionNode)
    assert prefill.llm_phrase_mention_collection_metadata is not None
    assert prefill.llm_phrase_mention_collection_metadata.max_mentions_per_request == (
        ExtractionPipelineFactory.DEFAULT_MENTION_COLLECTION_MAX_MENTIONS_PER_REQUEST
    )
    assert prefill.aggregation_fold_metadata is not None
    assert prefill.aggregation_fold_metadata.verb_fold is False  # keyword field: L2 off
    assert prefill.llm_phrase_mention_collection_metadata.snippet_radius == (
        ExtractionPipelineFactory.DEFAULT_MENTION_COLLECTION_SNIPPET_RADIUS
    )

    # v3 (3.2): synthesis follows mention collection.
    synthesis_node = mention_node.next_node
    assert isinstance(synthesis_node, EquipmentSynthesisNode)
    assert synthesis_node.phrase_synthesis_prompt.name == "equipment_phrase_synthesis"
    assert prefill.llm_phrase_synthesis_metadata is not None
    assert prefill.llm_phrase_synthesis_metadata.max_entries_per_request == (
        ExtractionPipelineFactory.DEFAULT_SYNTHESIS_MAX_ENTRIES_PER_REQUEST
    )
    assert prefill.llm_phrase_synthesis_metadata.include_location is (
        ExtractionPipelineFactory.DEFAULT_SYNTHESIS_INCLUDE_LOCATION
    )

    # v2 tail (unreachable until the Phase 3.3 re-key): the freehand pass
    # ENUMERATES candidates first, screening vets every one of them.
    freehand_grounding_node = synthesis_node.next_node
    assert isinstance(freehand_grounding_node, EquipmentFreehandGroundingNode)

    screening_node = freehand_grounding_node.next_node
    assert isinstance(screening_node, EquipmentRelationshipScreeningNode)

    reconcile_node = screening_node.next_node
    assert isinstance(reconcile_node, EquipmentReconcileNode)
    assert reconcile_node.field_type == KeywordTypeEnum.equipments
