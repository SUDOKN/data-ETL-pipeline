"""The cutover's wiring (Step 2, substep 6a, 2026-09-22): the factory builds
the Step 2 chains, the prefill nodes carry the new stage metadata with the run
flags, and the prompt service knows every new prompt name under the S3 key the
assembler publishes it to.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, cast

from core.models.chunking_strat import MATERIAL_CAP_CHUNKING_STRAT, PRODUCT_CHUNKING_STRAT
from core.models.pipeline_nodes import (
    ConceptDescentNode,
    ConceptGroundingNode,
    ConceptProposalNode,
    ConceptReconcileNode,
    ConceptSynthesisNode,
)
from llm_providers.models.file_objects.prompt import Prompt
from llm_providers.models.llm_model import GPT_4o_mini
from llm_providers.models.open_ai.gpt_model_params import GPTModelParams

from data_etl_app.models.pipeline_nodes import (
    ContractProductReconcileNode,
    ContractProductUnitScreeningNode,
    EquipmentReconcileNode,
    EquipmentUnitScreeningNode,
    PureProductFreehandGroundingNode,
    PureProductReconcileNode,
    PureProductUnitScreeningNode,
)
from data_etl_app.models.types_and_enums import KeywordTypeEnum
from core.models.pipeline_nodes.base.base_node import PipelineContext
from data_etl_app.models.types_and_enums import ConceptTypeEnum
from data_etl_app.services.extraction_pipeline_factory import ExtractionPipelineFactory
from data_etl_app.services.prompt_assembly_service import load_all_catalogs, prompt_s3_key
from data_etl_app.services.prompt_service import STAGED_PROMPT_FILE_PATHS, PromptService


def _prompt(name: str) -> Prompt:
    return Prompt(text=name, s3_version_id=f"v-{name}", name=name, num_tokens=1)


def _walk(node: Any) -> list[type]:
    out: list[type] = []
    while node is not None:
        out.append(type(node))
        node = getattr(node, "next_node", None)
    return out


def _concept(**overrides: Any):
    kwargs: dict[str, Any] = dict(
        concept_type=ConceptTypeEnum.material_caps,
        chunk_strategy=MATERIAL_CAP_CHUNKING_STRAT,
        ontology=cast(Any, type("O", (), {"s3_version_id": "ont-1"})()),
        search_prompt=_prompt("s"), recursive_search_prompt=_prompt("r"), phrase_synthesis_prompt=_prompt("syn"),
        phrase_relationship_screening_prompt=_prompt("scr"), phrase_initial_grounding_prompt=_prompt("g"),
        phrase_recursive_grounding_prompt=_prompt("rg"),
        phrase_grounding_prompt=_prompt("g2"), phrase_proposal_prompt=_prompt("pp"),
        phrase_unit_screening_prompt=_prompt("us"), phrase_descent_prompt=_prompt("de"),
        known_concepts=set(), llm_model=GPT_4o_mini, model_params=GPTModelParams.with_defaults(),
        created_at=datetime(2026, 9, 22),
    )
    kwargs.update(overrides)
    return ExtractionPipelineFactory.create_concept_extraction_pipeline(**kwargs)


def test_concept_chain_runs_grounding_proposal_descent_then_reconcile():
    prefill = _concept()
    chain = _walk(prefill)
    tail = chain[chain.index(ConceptSynthesisNode):]
    assert tail == [ConceptSynthesisNode, ConceptGroundingNode, ConceptProposalNode, ConceptDescentNode, ConceptReconcileNode]
    # the run flags: proposal pass ON and the leaf step ON by default
    assert prefill.llm_phrase_proposal_metadata is not None
    assert prefill.llm_phrase_descent_metadata is not None and prefill.llm_phrase_descent_metadata.leaf_step is True
    assert prefill.llm_phrase_grounding_metadata is not None and prefill.llm_phrase_grounding_metadata.prompt_name == "g2"
    assert prefill.llm_phrase_unit_screening_metadata is not None and prefill.llm_phrase_unit_screening_metadata.max_pairs_per_request == 50


def test_concept_run_flags_off():
    prefill = _concept(proposal_pass_enabled=False, leaf_step=False)
    assert prefill.llm_phrase_proposal_metadata is None
    assert prefill.llm_phrase_descent_metadata is not None and prefill.llm_phrase_descent_metadata.leaf_step is False


def test_keyword_chain_runs_freehand_unit_screening_then_reconcile():
    prefill = ExtractionPipelineFactory.create_pure_product_extraction_pipeline(
        chunk_strategy=PRODUCT_CHUNKING_STRAT, ontology_version_id="ont-1",
        search_prompt=_prompt("s"), recursive_search_prompt=_prompt("r"), phrase_synthesis_prompt=_prompt("syn"),
        phrase_relationship_screening_prompt=_prompt("scr"), phrase_freehand_grounding_prompt=_prompt("fg"),
        phrase_unit_screening_prompt=_prompt("us"),
        llm_model=GPT_4o_mini, model_params=GPTModelParams.with_defaults(), created_at=datetime(2026, 9, 22),
    )
    chain = _walk(prefill)
    assert chain[-3:] == [PureProductFreehandGroundingNode, PureProductUnitScreeningNode, PureProductReconcileNode]
    assert prefill.llm_phrase_unit_screening_metadata is not None and prefill.llm_phrase_unit_screening_metadata.prompt_name == "us"


def test_every_step2_catalog_is_registered_under_its_published_key():
    catalogs = load_all_catalogs()
    step2 = {name: c for name, c in catalogs.items() if c.stage in ("phrase_grounding", "phrase_proposal", "phrase_unit_screening", "phrase_descent")}
    assert len(step2) == 19
    for name, catalog in step2.items():
        assert STAGED_PROMPT_FILE_PATHS[name] == prompt_s3_key(catalog), name
        assert isinstance(getattr(PromptService, f"{name}_prompt"), property), name


def test_keyword_reconcile_reads_the_unit_screening_map():
    """6b: the three app reconcile nodes read unit screening's completed
    requests, not the retired relationship screening's."""
    for reconcile, screening, field in (
        (PureProductReconcileNode, PureProductUnitScreeningNode, KeywordTypeEnum.products),
        (ContractProductReconcileNode, ContractProductUnitScreeningNode, KeywordTypeEnum.contract_products),
        (EquipmentReconcileNode, EquipmentUnitScreeningNode, KeywordTypeEnum.equipments),
    ):
        ctx = PipelineContext(subject_name="Acme")
        ctx[screening] = {"req-1": cast(Any, object())}
        assert set(reconcile(field_type=field).get_upstream_screening_map(ctx)) == {"req-1"}
