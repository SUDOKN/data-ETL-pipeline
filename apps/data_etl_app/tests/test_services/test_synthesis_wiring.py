"""Phase 3.2 of pipeline v3 (merged with the location task 2026-09-03): the
synthesis stage wired into the app — prompt registration, the chain
(recursive search → synthesis → the tail), the shared contract/pure identity,
and the radius knob as run identity on the fold metadata."""

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
from core.models.extraction_results.extraction_node_metadata import (
    BatchedSynthesisNodeMetadata,
)
from core.models.extraction_schemas.synthesis import (
    SynthesisRecordInput,
)
from core.models.pipeline_nodes import (
    ConceptSynthesisNode,
)
from data_etl_app.models.pipeline_nodes import (
    ContractProductSynthesisNode,
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


def _shared_block(text: str) -> tuple[str, str]:
    """The text outside the one field-specific section (run B, 2026-09-14):
    everything before "## What to settle about the dealing" and everything
    from the next "## " heading on. A static without the section is all
    shared. Mirrors test_synthesis_statics_shared_block.split_shared."""
    heading = "## What to settle about the dealing"
    start = text.find(heading)
    if start < 0:
        return text, ""
    nxt = text.find("\n## ", start + len(heading))
    return text[:start], ("" if nxt < 0 else text[nxt + 1 :])


def test_synthesis_prompts_are_registered_exist_on_disk_and_share_one_block():
    shared = set()
    static = ""
    for field in FIELDS:
        key = f"{field}_phrase_synthesis"
        assert key in STAGED_PROMPT_FILE_PATHS
        path = _PROMPTS_DIR / STAGED_PROMPT_FILE_PATHS[key]
        assert path.is_file()
        static = path.read_text()
        shared.add(_shared_block(static))
    # Six pins, one shared block: the statics differ only inside their field
    # section (byte-identical in full until run B's field sections, 2026-09-14).
    assert len(shared) == 1
    assert "focal_form" in static and "describes the focal entity" in static
    # The per-snippet context task was dropped 2026-09-05 (location is derived
    # in code by the fold); the statics must not ask for it any more.
    assert "snippet_contexts" not in static and "(no introducing line)" not in static
    # Position still informs the text; the shared-block statics (run A, 2026-09-11)
    # say "Find each snippet in the text and say where it sits" — the earlier
    # "let where it sits inform the synthesis" sentence is gone.
    assert "say where it sits" in static


def _concept_pipeline(**overrides):
    kwargs: dict[str, Any] = dict(
        concept_type=ConceptTypeEnum.material_caps,
        chunk_strategy=MATERIAL_CAP_CHUNKING_STRAT,
        ontology=cast(Any, type("O", (), {"s3_version_id": "ont-1"})()),
        search_prompt=_prompt("s"),
        recursive_search_prompt=_prompt("r"),
        phrase_synthesis_prompt=_prompt("syn"),
        phrase_relationship_screening_prompt=_prompt("scr"),
        phrase_initial_grounding_prompt=_prompt("g"),
        phrase_recursive_grounding_prompt=_prompt("rg"),
        phrase_grounding_prompt=_prompt("g2"),
        phrase_proposal_prompt=_prompt("pp"),
        phrase_unit_screening_prompt=_prompt("us"),
        phrase_descent_prompt=_prompt("de"),
        known_concepts=set(),
        llm_model=GPT_4o_mini,
        model_params=GPTModelParams.with_defaults(),
        created_at=datetime(2026, 8, 22),
    )
    kwargs.update(overrides)
    return ExtractionPipelineFactory.create_concept_extraction_pipeline(**kwargs)


def test_concept_chain_runs_synthesis_after_recursive_search_with_the_knobs_as_identity():
    prefill = _concept_pipeline()
    synthesis = prefill.next_node.next_node.next_node
    assert isinstance(synthesis, ConceptSynthesisNode)
    assert synthesis.phrase_synthesis_prompt.name == "syn"
    assert prefill.llm_phrase_synthesis_metadata is not None
    assert prefill.llm_phrase_synthesis_metadata.prompt_name == "syn"
    assert prefill.llm_phrase_synthesis_metadata.max_entries_per_request == (
        ExtractionPipelineFactory.DEFAULT_SYNTHESIS_MAX_ENTRIES_PER_REQUEST
    )
    assert prefill.aggregation_fold_metadata is not None
    assert prefill.aggregation_fold_metadata.snippet_radius == (
        ExtractionPipelineFactory.DEFAULT_SNIPPET_RADIUS
    )
    # the knobs reach the metadata (= request identity)
    tuned = _concept_pipeline(
        snippet_radius=2,
        max_synthesis_entries_per_request=20,
    )
    assert tuned.llm_phrase_synthesis_metadata is not None
    assert tuned.llm_phrase_synthesis_metadata.max_entries_per_request == 20
    assert tuned.aggregation_fold_metadata is not None
    assert tuned.aggregation_fold_metadata.snippet_radius == 2
    assert tuned.llm_phrase_synthesis_metadata.to_custom_id_segment().endswith(
        "|gs=20"
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
        phrase_synthesis_prompt=_prompt("syn"),
        phrase_relationship_screening_prompt=_prompt("scr"),
        phrase_freehand_grounding_prompt=_prompt("fg"),
        phrase_unit_screening_prompt=_prompt("us"),
        llm_model=GPT_4o_mini,
        model_params=GPTModelParams.with_defaults(),
        created_at=datetime(2026, 8, 22),
    )
    assert isinstance(prefill.next_node.next_node.next_node, PureProductSynthesisNode)

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
                )
            },
        )(),
    )
    records = [
        SynthesisRecordInput(
            record_id="g4k9x2m",
            focal_form="Paladin",
            snippets=["Paladin PW Series doors."],
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
    assert "|gs=50|ud=" in contract
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
