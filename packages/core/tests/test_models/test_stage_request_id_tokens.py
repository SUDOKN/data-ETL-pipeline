"""The stage segment of a request's custom ID, as a scope you can name.

A custom ID is ``{subject}>{field}>{stage_token}>...``, so the stage a request
belongs to is already written into its identity. ``STAGE_REQUEST_ID_TOKEN`` is
where that token is DEFINED — nodes interpolate it rather than spelling it out —
which is what lets a query name a stage without the map drifting from the IDs
actually written. The AST test at the bottom is what keeps that true: it fails
the moment a node goes back to a literal.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

import core
from core.models.pipeline_nodes.base.pipeline_stage import (
    STAGE_REQUEST_ID_TOKEN,
    PipelineStage,
    request_id_tokens_from,
)


def test_a_stage_resolves_to_its_own_token_only():
    assert request_id_tokens_from(PipelineStage.relationship) == [
        "llm_phrase_relationship"
    ]


def test_downstream_widens_to_every_later_stage():
    tokens = request_id_tokens_from(
        PipelineStage.relationship, and_downstream=True
    )

    assert set(tokens) == {
        "llm_phrase_relationship",
        "llm_phrase_relationship_screening",
        "llm_phrase_initial_grounding",
        "llm_phrase_freehand_grounding",
        "llm_phrase_recursive_grounding",
    }


def test_downstream_leaves_upstream_stages_alone():
    """The point of the flag: what survives is exactly the stage's upstream."""
    tokens = request_id_tokens_from(PipelineStage.screening, and_downstream=True)

    assert "llm_search" not in tokens
    assert "llm_recursive_search" not in tokens
    assert "llm_phrase_relationship" not in tokens


def test_the_first_llm_stage_and_downstream_is_everything_the_field_issued():
    tokens = request_id_tokens_from(
        PipelineStage.phrase_search, and_downstream=True
    )

    assert set(tokens) == set(STAGE_REQUEST_ID_TOKEN.values())


def test_the_grounding_tier_comes_as_a_pair():
    """Initial and freehand share a rank, as they do in ``stop_after``. They are
    alternatives, never both in one field's chain, so naming both costs nothing:
    the token for the flavour a field does not use matches no request of its."""
    tokens = request_id_tokens_from(
        PipelineStage.initial_grounding, and_downstream=True
    )

    assert "llm_phrase_freehand_grounding" in tokens
    assert "llm_phrase_initial_grounding" in tokens


def test_a_stage_that_issues_no_requests_is_refused():
    """Rather than resolving to "nothing" and reporting a delete of zero."""
    with pytest.raises(ValueError, match="issues no LLM batch requests"):
        request_id_tokens_from(PipelineStage.reconcile)

    with pytest.raises(ValueError, match="issues no LLM batch requests"):
        request_id_tokens_from(PipelineStage.prefill)


def test_downstream_of_the_last_stage_is_refused():
    with pytest.raises(ValueError, match="No stage at or after"):
        request_id_tokens_from(PipelineStage.reconcile, and_downstream=True)


def test_no_token_is_a_prefix_of_another_without_its_delimiter():
    """Tokens are matched with a trailing ``>``, which is what separates
    ``llm_phrase_relationship`` from ``llm_phrase_relationship_screening``. This
    pins that a new token can never be ambiguous even before the delimiter is
    applied by more than that one shared boundary."""
    tokens = list(STAGE_REQUEST_ID_TOKEN.values())

    assert len(tokens) == len(set(tokens))
    for token in tokens:
        overlapping = [
            other
            for other in tokens
            if other != token and other.startswith(token)
        ]
        # Overlap is allowed, but only where the longer token continues past a
        # segment boundary the delimiter will restore.
        for other in overlapping:
            assert other[len(token)] == "_"


_NODE_ROOT = Path(core.__file__).parent / "models" / "pipeline_nodes"
_LITERAL_TOKEN = re.compile(r">llm[_a-z]*>")


def _custom_id_builders() -> list[tuple[Path, ast.FunctionDef]]:
    found: list[tuple[Path, ast.FunctionDef]] = []
    for path in _NODE_ROOT.rglob("*.py"):
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.FunctionDef)
                and node.name == "get_request_custom_id"
                and node.body
                and not isinstance(node.body[-1], ast.Pass)
            ):
                found.append((path, node))
    return found


def test_no_node_spells_its_stage_token_out_by_hand():
    offenders = []
    for path, builder in _custom_id_builders():
        source = ast.unparse(builder)
        if _LITERAL_TOKEN.search(source) or "STAGE_REQUEST_ID_TOKEN" not in source:
            offenders.append(path.name)

    assert not offenders, (
        f"{offenders} build a custom ID without STAGE_REQUEST_ID_TOKEN. A literal "
        f"token there drifts from the map that queries and deletes are scoped by, "
        f"and the drift only shows up as a delete that silently matched nothing."
    )


def test_the_tripwire_actually_inspects_something():
    """A rename of the builder would otherwise turn the test above into a
    vacuous pass over an empty list."""
    builders = _custom_id_builders()

    assert len(builders) == len(STAGE_REQUEST_ID_TOKEN)
