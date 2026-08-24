"""Phase 3.2 of pipeline v3: the synthesis stage wired into the app — prompt
registration, the chain (mention → synthesis → the v2 tail), the shared
contract/pure identity, the location arm and the radius knob as run identity."""

from datetime import datetime
from pathlib import Path
from typing import Any, cast

import pytest
from llm_providers.models.file_objects.prompt import Prompt
from llm_providers.models.llm_model import GPT_4o_mini
from llm_providers.models.open_ai.gpt_model_params import GPTModelParams

from core.models.chunking_strat import (
    MATERIAL_CAP_CHUNKING_STRAT,
    PRODUCT_CHUNKING_STRAT,
)
from core.models.extraction_results.llm_phrase_extraction_results import (
    BatchedSynthesisNodeMetadata,
)
from core.models.extraction_schemas.synthesis import (
    SynthesisEntry,
    SynthesisRecordInput,
)
from core.models.pipeline_nodes import (
    ConceptMentionCollectionNode,
    ConceptSynthesisNode,
)
from data_etl_app.models.pipeline_nodes import (
    ContractProductSynthesisNode,
    PureProductMentionCollectionNode,
    PureProductSynthesisNode,
)
from data_etl_app.models.types_and_enums import ConceptTypeEnum, KeywordTypeEnum
from data_etl_app.services.extraction_pipeline_factory import ExtractionPipelineFactory
from data_etl_app.services.prompt_service import STAGED_PROMPT_FILE_PATHS

_PROMPTS_DIR = (
    Path(__file__).resolve().parents[2]
    / "src"
    / "data_etl_app"
    / "knowledge"
    / "prompts"
    / "final_texts"
    / "static"
)
FIELDS = [
    "conformity_attestation",
    "industry",
    "material_cap",
    "process_cap",
    "product",
    "equipment",
]


def _prompt(name: str) -> Prompt:
    return Prompt(s3_version_id="v1", name=name, text=f"{name} text", num_tokens=3)


def test_synthesis_prompts_are_registered_exist_on_disk_and_are_byte_identical():
    texts = set()
    for field in FIELDS:
        key = f"{field}_phrase_synthesis"
        assert key in STAGED_PROMPT_FILE_PATHS
        path = _PROMPTS_DIR / STAGED_PROMPT_FILE_PATHS[key]
        assert path.is_file()
        texts.add(path.read_text())
    assert len(texts) == 1  # field-agnostic: one static, six pins
    static = texts.pop()
    assert "focal_form" in static and "describes the focal entity" in static
    assert "where given, the location" in static  # one static serves both arms


def _concept_pipeline(**overrides):
    kwargs: dict[str, Any] = dict(
        concept_type=ConceptTypeEnum.material_caps,
        chunk_strategy=MATERIAL_CAP_CHUNKING_STRAT,
        ontology=cast(Any, type("O", (), {"s3_version_id": "ont-1"})()),
        search_prompt=_prompt("s"),
        recursive_search_prompt=_prompt("r"),
        phrase_mention_collection_prompt=_prompt("m"),
        phrase_synthesis_prompt=_prompt("syn"),
        phrase_relationship_screening_prompt=_prompt("scr"),
        phrase_initial_grounding_prompt=_prompt("g"),
        phrase_recursive_grounding_prompt=_prompt("rg"),
        known_concepts=set(),
        llm_model=GPT_4o_mini,
        model_params=GPTModelParams.with_defaults(),
        created_at=datetime(2026, 8, 22),
    )
    kwargs.update(overrides)
    return ExtractionPipelineFactory.create_concept_extraction_pipeline(**kwargs)


def test_concept_chain_runs_synthesis_after_mention_collection_with_the_knobs_as_identity():
    prefill = _concept_pipeline()
    mention = prefill.next_node.next_node.next_node
    assert isinstance(mention, ConceptMentionCollectionNode)
    synthesis = mention.next_node
    assert isinstance(synthesis, ConceptSynthesisNode)
    assert synthesis.phrase_synthesis_prompt.name == "syn"
    assert prefill.llm_phrase_synthesis_metadata is not None
    assert prefill.llm_phrase_synthesis_metadata.prompt_name == "syn"
    assert prefill.llm_phrase_synthesis_metadata.max_entries_per_request == (
        ExtractionPipelineFactory.DEFAULT_SYNTHESIS_MAX_ENTRIES_PER_REQUEST
    )
    assert prefill.llm_phrase_synthesis_metadata.include_location is True
    assert prefill.llm_phrase_mention_collection_metadata is not None
    assert prefill.llm_phrase_mention_collection_metadata.snippet_radius == 0
    # the knobs reach the metadata (= request identity)
    tuned = _concept_pipeline(
        synthesis_include_location=False,
        mention_collection_snippet_radius=2,
        max_synthesis_entries_per_request=20,
    )
    assert tuned.llm_phrase_synthesis_metadata is not None
    assert tuned.llm_phrase_synthesis_metadata.include_location is False
    assert tuned.llm_phrase_synthesis_metadata.max_entries_per_request == 20
    assert tuned.llm_phrase_mention_collection_metadata is not None
    assert tuned.llm_phrase_mention_collection_metadata.snippet_radius == 2
    assert tuned.llm_phrase_synthesis_metadata.to_custom_id_segment().endswith(
        "|gs=20|loc=0"
    )
    assert tuned.llm_phrase_mention_collection_metadata.to_custom_id_segment().endswith(
        "|gs=50|rad=2"
    )


def test_the_synthesis_prompt_is_required():
    with pytest.raises(ValueError, match="phrase_synthesis_prompt is required"):
        _concept_pipeline(phrase_synthesis_prompt=None)


def test_pure_product_chain_and_the_shared_contract_identity():
    prefill = ExtractionPipelineFactory.create_pure_product_extraction_pipeline(
        chunk_strategy=PRODUCT_CHUNKING_STRAT,
        ontology_version_id="ont-1",
        search_prompt=_prompt("s"),
        recursive_search_prompt=_prompt("r"),
        phrase_mention_collection_prompt=_prompt("m"),
        phrase_synthesis_prompt=_prompt("syn"),
        phrase_relationship_screening_prompt=_prompt("scr"),
        phrase_freehand_grounding_prompt=_prompt("fg"),
        llm_model=GPT_4o_mini,
        model_params=GPTModelParams.with_defaults(),
        created_at=datetime(2026, 8, 22),
    )
    mention = prefill.next_node.next_node.next_node
    assert isinstance(mention, PureProductMentionCollectionNode)
    assert isinstance(mention.next_node, PureProductSynthesisNode)

    metadata = cast(
        Any,
        type(
            "M",
            (),
            {
                "llm_phrase_synthesis": BatchedSynthesisNodeMetadata(
                    llm_model=GPT_4o_mini,
                    model_params=GPTModelParams.with_defaults(),
                    prompt_name="product_phrase_synthesis",
                    prompt_version_id="pv1",
                    created_at=datetime(2026, 8, 22),
                    max_entries_per_request=50,
                    include_location=True,
                )
            },
        )(),
    )
    records = [
        SynthesisRecordInput(
            record_id="g4k9x2m",
            focal_form="Paladin",
            entries=[
                SynthesisEntry(
                    location="the products page", snippet="Paladin PW Series doors."
                )
            ],
        )
    ]
    kwargs: dict[str, Any] = dict(
        subject_unique_id="acme.example",
        chunk_bounds="0:1000",
        group_index=0,
        metadata=metadata,
        group_records=records,
    )
    contract = ContractProductSynthesisNode.get_request_custom_id(
        field_type=KeywordTypeEnum.contract_products, **kwargs
    )
    pure = PureProductSynthesisNode.get_request_custom_id(
        field_type=KeywordTypeEnum.products, **kwargs
    )
    assert contract == pure
    assert ">products>llm_phrase_synthesis>chunk>0:1000>group>0>" in contract
    assert "|gs=50|loc=1|ud=" in contract
    retry_contract = ContractProductSynthesisNode.get_request_custom_id(
        field_type=KeywordTypeEnum.contract_products, retry_index=1, **kwargs
    )
    retry_pure = PureProductSynthesisNode.get_request_custom_id(
        field_type=KeywordTypeEnum.products, retry_index=1, **kwargs
    )
    assert retry_contract == retry_pure != pure
    assert (
        ">products>llm_phrase_synthesis>chunk>0:1000>retry>1>group>0>" in retry_contract
    )
