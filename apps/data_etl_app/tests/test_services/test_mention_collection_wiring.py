"""Phase 3.1 of pipeline v3: the mention stage wired into the app — prompt
registration, the per-field verb-fold dial, the shared contract/pure identity."""

from datetime import datetime
from pathlib import Path
from typing import Any, cast

from llm_providers.models.file_objects.prompt import Prompt
from llm_providers.models.llm_model import GPT_4o_mini
from llm_providers.models.open_ai.gpt_model_params import GPTModelParams

from core.models.chunking_strat import MATERIAL_CAP_CHUNKING_STRAT
from core.models.extraction_results.llm_phrase_extraction_results import (
    BatchedMentionCollectionNodeMetadata,
)
from core.models.pipeline_nodes import ConceptMentionCollectionNode
from core.utils.form_normalizer import NORMALIZER_VERSION
from data_etl_app.models.pipeline_nodes import (
    ContractProductMentionCollectionNode,
    PureProductMentionCollectionNode,
)
from data_etl_app.models.types_and_enums import ConceptTypeEnum, KeywordTypeEnum
from data_etl_app.services.extraction_pipeline_factory import ExtractionPipelineFactory
from data_etl_app.services.prompt_service import STAGED_PROMPT_FILE_PATHS

_PROMPTS_DIR = (
    Path(__file__).resolve().parents[2]
    / "src" / "data_etl_app" / "knowledge" / "prompts" / "final_texts" / "static"
)
FIELDS = ["conformity_attestation", "industry", "material_cap", "process_cap", "product", "equipment"]


def _prompt(name: str) -> Prompt:
    return Prompt(s3_version_id="v1", name=name, text=f"{name} text", num_tokens=3)


def test_mention_collection_prompts_are_registered_and_exist_on_disk():
    for field in FIELDS:
        key = f"{field}_phrase_mention_collection"
        assert key in STAGED_PROMPT_FILE_PATHS
        assert (_PROMPTS_DIR / STAGED_PROMPT_FILE_PATHS[key]).is_file()


def test_verb_fold_dial_is_on_for_material_and_process_only():
    on = {f for f in ConceptTypeEnum if ExtractionPipelineFactory._aggregation_fold_metadata(f).verb_fold}
    assert on == {ConceptTypeEnum.material_caps, ConceptTypeEnum.process_caps}
    fold = ExtractionPipelineFactory._aggregation_fold_metadata(KeywordTypeEnum.products)
    assert fold.verb_fold is False and fold.normalizer_version == NORMALIZER_VERSION


def test_concept_chain_runs_mention_collection_after_recursive_search():
    prefill = ExtractionPipelineFactory.create_concept_extraction_pipeline(
        concept_type=ConceptTypeEnum.material_caps,
        chunk_strategy=MATERIAL_CAP_CHUNKING_STRAT,
        ontology=cast(Any, type("O", (), {"s3_version_id": "ont-1"})()),
        search_prompt=_prompt("s"),
        recursive_search_prompt=_prompt("r"),
        phrase_relationship_prompt=_prompt("rel"),
        phrase_mention_collection_prompt=_prompt("m"),
        phrase_relationship_screening_prompt=_prompt("scr"),
        phrase_initial_grounding_prompt=_prompt("g"),
        phrase_recursive_grounding_prompt=_prompt("rg"),
        known_concepts=set(),
        llm_model=GPT_4o_mini,
        model_params=GPTModelParams.with_defaults(),
        created_at=datetime(2026, 8, 21),
    )
    mention = prefill.next_node.next_node.next_node
    assert isinstance(mention, ConceptMentionCollectionNode)
    assert mention.phrase_mention_collection_prompt.name == "m"
    assert prefill.aggregation_fold_metadata is not None
    assert prefill.aggregation_fold_metadata.verb_fold is True
    assert prefill.llm_phrase_mention_collection_metadata is not None
    assert prefill.llm_phrase_mention_collection_metadata.prompt_name == "m"


def test_contract_products_share_the_pure_product_mention_identity():
    metadata = cast(
        Any,
        type(
            "M",
            (),
            {
                "llm_phrase_mention_collection": BatchedMentionCollectionNodeMetadata(
                    llm_model=GPT_4o_mini,
                    model_params=GPTModelParams.with_defaults(),
                    prompt_name="product_phrase_mention_collection",
                    prompt_version_id="pv1",
                    created_at=datetime(2026, 8, 21),
                    max_forms_per_request=30,
                )
            },
        )(),
    )
    kwargs: dict[str, Any] = dict(
        subject_unique_id="acme.example",
        chunk_bounds="0:1000",
        sub_bounds="0:493",
        group_index=0,
        metadata=metadata,
        group_forms=["Aluminum", "aluminum"],
    )
    contract = ContractProductMentionCollectionNode.get_request_custom_id(
        field_type=KeywordTypeEnum.contract_products, **kwargs
    )
    pure = PureProductMentionCollectionNode.get_request_custom_id(
        field_type=KeywordTypeEnum.products, **kwargs
    )
    assert contract == pure
    assert ">products>llm_phrase_mention_collection>chunk>0:1000>sub>0:493>group>0>" in contract
    assert "|gs=30|ud=" in contract
    # a different form list is a different question
    other = PureProductMentionCollectionNode.get_request_custom_id(
        field_type=KeywordTypeEnum.products, **{**kwargs, "group_forms": ["Aluminum"]}
    )
    assert other != pure
