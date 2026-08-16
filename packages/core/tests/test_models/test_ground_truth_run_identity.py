"""Run identity: the constructor, the hash, and the field-set tripwire.

The tripwire is the point of this file: ground-truth documents are keyed by
``ExplicitRunIdentity`` (I1), so a field quietly added to any metadata model is
a config axis that runs could differ on while their ground truths collide.
Every field below is recorded as either IN the identity or consciously OUT;
growing a model fails the test until this file says where the field goes.
"""

from datetime import datetime
from typing import Any

import pytest
from pydantic import ValidationError

from llm_providers.models.llm_model import GPT_4o_mini
from llm_providers.models.open_ai.gpt_model_params import GPTModelParams

from core.models.chunking_strat import ChunkingStrategy
from core.models.extraction_results.concept_extraction_results import (
    BatchedInitialGroundingNodeMetadata,
    ConceptExtractionMetadata,
)
from core.models.extraction_results.keyword_extraction_results import (
    BatchedFreehandGroundingNodeMetadata,
    KeywordExtractionMetadata,
)
from core.models.extraction_results.llm_phrase_extraction_results import (
    BaseExtractionMetadata,
    BatchedRelationshipNodeMetadata,
    BatchedScreeningNodeMetadata,
    ExtractionNodeMetadata,
    LLMPhraseExtractionMetadata,
    RecursiveSearchNodeMetadata,
)
from core.models.ground_truth.run_identity import (
    ExplicitRunIdentity,
    NodeIdentity,
)

# ---------------------------------------------------------------------------
# The tripwire: every metadata field is either in the identity or consciously
# excluded. A new field failing here is the test working — decide where it
# goes, then record it.
# ---------------------------------------------------------------------------

# IN the identity, via NodeIdentity (llm_model → its name, everything else
# verbatim — model_params stored whole, per "what was the mismatch?").
_NODE_IDENTITY_SOURCES = {
    "llm_model",
    "model_params",
    "prompt_version_id",
    "catalog_version",
}
# Consciously OUT: created_at is when, not what; prompt_name is addressing —
# prompt_version_id already pins the bytes.
_NODE_EXCLUDED = {"created_at", "prompt_name"}

_NODE_FIELD_SNAPSHOT = {
    ExtractionNodeMetadata: _NODE_IDENTITY_SOURCES | _NODE_EXCLUDED,
    RecursiveSearchNodeMetadata: _NODE_IDENTITY_SOURCES
    | _NODE_EXCLUDED
    | {"max_rounds"},
    BatchedRelationshipNodeMetadata: _NODE_IDENTITY_SOURCES
    | _NODE_EXCLUDED
    | {"max_phrases_per_request"},
    BatchedScreeningNodeMetadata: _NODE_IDENTITY_SOURCES
    | _NODE_EXCLUDED
    | {"max_pairs_per_request"},
    BatchedFreehandGroundingNodeMetadata: _NODE_IDENTITY_SOURCES
    | _NODE_EXCLUDED
    | {"max_pairs_per_request"},
    BatchedInitialGroundingNodeMetadata: _NODE_IDENTITY_SOURCES
    | _NODE_EXCLUDED
    | {"max_pairs_per_request"},
}

_SHARED_NODES = {
    "llm_phrase_search",
    "llm_phrase_recursive_search",
    "llm_phrase_relationship",
    "llm_phrase_relationship_screening",
}

_METADATA_FIELD_SNAPSHOT = {
    # chunk_strat and ontology_version_id are IN; created_at is OUT.
    BaseExtractionMetadata: {"created_at", "chunk_strat", "ontology_version_id"},
    LLMPhraseExtractionMetadata: set(BaseExtractionMetadata.model_fields)
    | _SHARED_NODES,
    KeywordExtractionMetadata: set(BaseExtractionMetadata.model_fields)
    | _SHARED_NODES
    | {"llm_phrase_freehand_grounding"},
    ConceptExtractionMetadata: set(BaseExtractionMetadata.model_fields)
    | _SHARED_NODES
    | {"llm_phrase_initial_grounding", "llm_phrase_recursive_grounding"},
}


@pytest.mark.parametrize(
    "model,expected",
    [*_NODE_FIELD_SNAPSHOT.items(), *_METADATA_FIELD_SNAPSHOT.items()],
    ids=lambda x: x.__name__ if isinstance(x, type) else "",
)
def test_tripwire_every_metadata_field_is_consciously_placed(model, expected):
    actual = set(model.model_fields)
    assert actual == expected, (
        f"{model.__name__} field set changed: added {actual - expected or '{}'}, "
        f"removed {expected - actual or '{}'}. Decide whether each joins "
        f"ExplicitRunIdentity or is consciously excluded, then update this "
        f"snapshot AND run_identity.py together."
    )


# ---------------------------------------------------------------------------
# from_metadata
# ---------------------------------------------------------------------------

_COMMON: dict[str, Any] = dict(
    llm_model=GPT_4o_mini,
    model_params=GPTModelParams.with_defaults(),
    prompt_name="some_prompt",
    prompt_version_id="s3-version-1",
    created_at=datetime(2026, 8, 15),
)

_BASE: dict[str, Any] = dict(
    created_at=datetime(2026, 8, 15),
    chunk_strat=ChunkingStrategy(overlap=0.1, max_chunks=5, max_tokens_per_chunk=1000),
    ontology_version_id="onto-v7",
    llm_phrase_search=ExtractionNodeMetadata(**_COMMON),
    llm_phrase_recursive_search=RecursiveSearchNodeMetadata(**_COMMON, max_rounds=3),
    llm_phrase_relationship=BatchedRelationshipNodeMetadata(
        **_COMMON, max_phrases_per_request=50
    ),
    llm_phrase_relationship_screening=BatchedScreeningNodeMetadata(
        **_COMMON, max_pairs_per_request=15
    ),
)


def _keyword_metadata() -> KeywordExtractionMetadata:
    return KeywordExtractionMetadata(
        **_BASE,
        llm_phrase_freehand_grounding=BatchedFreehandGroundingNodeMetadata(
            **{**_COMMON, "catalog_version": "equipment_phrase_freehand_grounding.2"},
            max_pairs_per_request=50,
        ),
    )


def _concept_metadata() -> ConceptExtractionMetadata:
    return ConceptExtractionMetadata(
        **_BASE,
        llm_phrase_initial_grounding=BatchedInitialGroundingNodeMetadata(
            **{**_COMMON, "catalog_version": "industry_phrase_initial_grounding.3"},
            max_pairs_per_request=15,
        ),
        llm_phrase_recursive_grounding=ExtractionNodeMetadata(
            **{**_COMMON, "catalog_version": "industry_phrase_recursive_grounding.3"},
        ),
    )


def test_keyword_metadata_builds_a_keyword_identity():
    identity = ExplicitRunIdentity.from_metadata(_keyword_metadata())
    assert identity.llm_phrase_freehand_grounding is not None
    assert identity.llm_phrase_freehand_grounding.max_pairs_per_request == 50
    assert identity.llm_phrase_initial_grounding is None
    assert identity.llm_phrase_recursive_grounding is None


def test_concept_metadata_builds_a_concept_identity():
    identity = ExplicitRunIdentity.from_metadata(_concept_metadata())
    assert identity.llm_phrase_freehand_grounding is None
    assert identity.llm_phrase_initial_grounding is not None
    assert (
        identity.llm_phrase_initial_grounding.catalog_version
        == "industry_phrase_initial_grounding.3"
    )
    assert identity.llm_phrase_recursive_grounding is not None


def test_identity_carries_the_what_of_each_node():
    identity = ExplicitRunIdentity.from_metadata(_concept_metadata())
    search = identity.llm_phrase_search
    assert search.llm_model == "gpt-4o-mini"
    assert search.prompt_version_id == "s3-version-1"
    assert search.catalog_version is None  # search has no catalog
    assert identity.llm_phrase_recursive_search.max_rounds == 3
    assert identity.llm_phrase_relationship.max_phrases_per_request == 50
    assert identity.chunk_strat.max_tokens_per_chunk == 1000
    assert identity.ontology_version_id == "onto-v7"


def test_identity_round_trips():
    identity = ExplicitRunIdentity.from_metadata(_keyword_metadata())
    dumped = identity.model_dump(mode="json")
    assert ExplicitRunIdentity.model_validate(dumped) == identity


# ---------------------------------------------------------------------------
# Grounding-family validator
# ---------------------------------------------------------------------------


def _node_identity() -> NodeIdentity:
    return NodeIdentity.from_node(ExtractionNodeMetadata(**_COMMON))


def test_concept_grounding_nodes_must_come_paired():
    identity = ExplicitRunIdentity.from_metadata(_concept_metadata())
    with pytest.raises(ValidationError, match="come together"):
        ExplicitRunIdentity(
            **{
                **identity.model_dump(),
                "llm_phrase_recursive_grounding": None,
            }
        )


def test_freehand_and_ontology_grounding_are_exclusive():
    concept = ExplicitRunIdentity.from_metadata(_concept_metadata())
    with pytest.raises(ValidationError, match="never both"):
        ExplicitRunIdentity(
            **{
                **concept.model_dump(),
                "llm_phrase_freehand_grounding": _node_identity().model_dump(),
            }
        )


# ---------------------------------------------------------------------------
# Explicit params: the mismatch itself is visible, not just its existence
# ---------------------------------------------------------------------------


def test_params_are_stored_whole():
    identity = ExplicitRunIdentity.from_metadata(_keyword_metadata())
    assert identity.llm_phrase_search.model_params == GPTModelParams.with_defaults()


def test_a_changed_param_is_readable_off_the_identity_diff():
    changed = ExtractionNodeMetadata(
        **{
            **_COMMON,
            "model_params": GPTModelParams.with_defaults().model_copy(
                update={"max_completion_tokens": 9000}
            ),
        }
    )
    a = NodeIdentity.from_node(ExtractionNodeMetadata(**_COMMON))
    b = NodeIdentity.from_node(changed)
    assert a != b
    assert a.model_params.max_completion_tokens == 7500
    assert b.model_params.max_completion_tokens == 9000


def test_canonical_digest_is_stable_across_dict_insertion_order():
    """The digest is what the unique index keys on, so dict-order noise inside
    response_format must never split one identity into two index keys."""
    a = GPTModelParams.with_defaults().with_response_format(
        {"type": "json_schema", "json_schema": {"x": 1}}
    )
    b = GPTModelParams.with_defaults().with_response_format(
        {"json_schema": {"x": 1}, "type": "json_schema"}
    )
    meta_a = KeywordExtractionMetadata(
        **{**_BASE, "llm_phrase_search": ExtractionNodeMetadata(**{**_COMMON, "model_params": a})},
        llm_phrase_freehand_grounding=BatchedFreehandGroundingNodeMetadata(
            **_COMMON, max_pairs_per_request=50
        ),
    )
    meta_b = KeywordExtractionMetadata(
        **{**_BASE, "llm_phrase_search": ExtractionNodeMetadata(**{**_COMMON, "model_params": b})},
        llm_phrase_freehand_grounding=BatchedFreehandGroundingNodeMetadata(
            **_COMMON, max_pairs_per_request=50
        ),
    )
    assert (
        ExplicitRunIdentity.from_metadata(meta_a).canonical_digest()
        == ExplicitRunIdentity.from_metadata(meta_b).canonical_digest()
    )


def test_canonical_digest_sees_any_changed_param():
    base = ExplicitRunIdentity.from_metadata(_keyword_metadata())
    changed = base.model_copy(
        update={
            "llm_phrase_search": NodeIdentity.from_node(
                ExtractionNodeMetadata(
                    **{
                        **_COMMON,
                        "model_params": GPTModelParams.with_defaults().model_copy(
                            update={"max_completion_tokens": 9000}
                        ),
                    }
                )
            )
        }
    )
    assert base.canonical_digest() != changed.canonical_digest()


def test_params_equality_ignores_dict_insertion_order():
    """Dict equality is order-insensitive, so two identities configured with the
    same response_format written in different key orders are the SAME identity at
    the model level. Index-key mechanics are the Document layer's problem."""
    a = GPTModelParams.with_defaults().with_response_format(
        {"type": "json_schema", "json_schema": {"x": 1}}
    )
    b = GPTModelParams.with_defaults().with_response_format(
        {"json_schema": {"x": 1}, "type": "json_schema"}
    )
    node_a = NodeIdentity.from_node(ExtractionNodeMetadata(**{**_COMMON, "model_params": a}))
    node_b = NodeIdentity.from_node(ExtractionNodeMetadata(**{**_COMMON, "model_params": b}))
    assert node_a == node_b
