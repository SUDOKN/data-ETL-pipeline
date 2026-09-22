"""Phase 0.4 of pipeline v2 (PIPELINE_V2_PLAN.md): the record-keyed wire schemas,
built from the DRAFT catalogs in rule_catalog_v2/ and held to the same two
invariants the v1 catalogs are held to — strict mode accepts the schema, and the
worked example the prompt shows decodes under the schema the model is sent."""

import copy
from typing import Any

import pytest
from pydantic import ValidationError

from core.models.extraction_schemas.catalog_wire_schema import (
    flatten_rule_slots,
    response_format_for,
    response_model_for,
    screening_response_model,
)
from core.models.extraction_schemas.response_format_util import (
    assert_strict_schema_supported,
)
from core.models.rule_catalog import RuleCatalog

from data_etl_app.services.prompt_assembly_service import (
    CATALOG_DIR,
    EXAMPLE_BUILDER_BY_STAGE,
    load_catalog,
)

# The record-keyed stages: every deployed catalog except binary classification.
V2_CATALOGS = {
    path.stem: catalog
    for path in sorted(CATALOG_DIR.glob("*.json"))
    for catalog in [load_catalog(path)]
    if catalog.stage != "binary_classification"
}


def _example(catalog: RuleCatalog) -> dict:
    """The v2 worked example, raw from its builder. Entity tokens inside the
    placeholder strings stay unresolved — every such string starts with ``<`` or
    ``{{``, and the fill below replaces or passes them without reading them."""
    return EXAMPLE_BUILDER_BY_STAGE[catalog.stage](catalog)


def _fill_placeholders(node: Any, schema: dict[str, Any], root: dict[str, Any]) -> Any:
    """The example with every ``<...>`` slot replaced by a value legal at that
    position, read off the schema itself.

    A twin of the helper in test_prompt_assembly_service — sibling test imports
    are blocked by importlib mode, and the two files merge at the per-stage
    cutover, which is when this copy dies.
    """
    while "$ref" in schema:
        target = root
        for part in schema["$ref"].lstrip("#/").split("/"):
            target = target[part]
        schema = target

    if "anyOf" in schema:
        for branch in schema["anyOf"]:
            filled = _fill_placeholders(node, branch, root)
            if filled is not None:
                return filled
        return None

    if isinstance(node, dict):
        properties = schema.get("properties", {})
        if set(node) != set(properties):
            return None  # not this branch — or an example/schema key drift
        return {
            key: _fill_placeholders(value, properties[key], root)
            for key, value in node.items()
        }
    if isinstance(node, list):
        return [_fill_placeholders(item, schema["items"], root) for item in node]
    if isinstance(node, str):
        if "enum" in schema:
            return node if node in schema["enum"] else schema["enum"][0]
        if not node.startswith("<"):
            return node
        return {"integer": 0, "number": 0, "boolean": False}.get(
            schema.get("type", ""), "x"
        )
    return node


def _filled_example(catalog: RuleCatalog) -> dict:
    schema = response_format_for(catalog)["json_schema"]["schema"]
    filled = _fill_placeholders(_example(catalog), schema, schema)
    assert filled is not None, (
        f"{catalog.prompt_name}: v2 example matches no branch of the v2 schema"
    )
    return filled


def test_every_phrase_catalog_is_deployed():
    """Sanity for the parametrization below: a catalog failing to load would
    otherwise silently shrink the coverage of every test in this file. The two
    freehand catalogs (products, equipments) plus the 19 structural ones of
    Step 2 (four grounding, seven unit screening, four descent, four
    proposal-pass); the four families they replaced were retired at the
    cutover (2026-09-22)."""
    assert len(V2_CATALOGS) == 21


@pytest.mark.parametrize("prompt_name", sorted(V2_CATALOGS))
def test_the_v2_schema_is_one_strict_mode_accepts(prompt_name):
    assert_strict_schema_supported(
        response_format_for(V2_CATALOGS[prompt_name]), where=prompt_name
    )


@pytest.mark.parametrize("prompt_name", sorted(V2_CATALOGS))
def test_the_v2_example_decodes_under_the_v2_schema(prompt_name):
    """The 2026-08-11 invariant, carried into v2: the prompt's worked example and
    the decoder must agree on the shape — including the required-nullable
    ``explanation`` a populated grounding entry must show as null."""
    catalog = V2_CATALOGS[prompt_name]
    response_model_for(catalog).model_validate(_filled_example(catalog))


def _freehand_grounding() -> RuleCatalog:
    return V2_CATALOGS["equipment_phrase_freehand_grounding"]


def test_grounding_explanation_key_cannot_be_omitted():
    """The structural declination rests on the key always arriving: an entry
    without it would make 'yielded nothing' silent again."""
    catalog = _freehand_grounding()
    model = response_model_for(catalog)
    good = _filled_example(catalog)

    dropped = copy.deepcopy(good)
    del dropped["groundings"][0]["explanation"]
    with pytest.raises(ValidationError):
        model.model_validate(dropped)


def test_grounding_empty_entry_carries_its_explanation():
    """The none-branch of every grounding example is empty units + a non-null
    explanation — the shape the skeletons instruct."""
    for name, catalog in V2_CATALOGS.items():
        if catalog.stage not in (
            "phrase_initial_grounding",
            "phrase_recursive_grounding",
            "phrase_oov_grounding",
            "phrase_freehand_grounding",
        ):
            continue
        entries = _example(catalog)["groundings"]
        empties = [
            entry
            for entry in entries
            if not (entry.get("options") or entry.get("candidates"))
        ]
        assert len(empties) == 1, name
        assert empties[0]["explanation"], name
        populated = [entry for entry in entries if entry not in empties]
        assert all(entry["explanation"] is None for entry in populated), name


def test_flatten_rule_slots_reads_a_v2_candidate_unit():
    """Storage stays list[AppliedRule]: a decoded freehand candidate flattens
    to its catalog's reportable rules in document order."""
    catalog = _freehand_grounding()
    parsed = response_model_for(catalog).model_validate(_filled_example(catalog))
    unit = next(entry.candidates[0] for entry in parsed.groundings if entry.candidates)  # type: ignore[attr-defined]
    applied = flatten_rule_slots(catalog, unit)
    reportable = [rule.id for rule in catalog.walk_rules() if rule.reportable]
    assert applied and [rule.rule_id for rule in applied] == [r for r in reportable if r in {a.rule_id for a in applied}]


