import json
import re
from typing import Iterator

import pytest

from core.models.extraction_schemas.applied_rule import AppliedRule
from core.models.extraction_schemas.catalog_wire_schema import (
    CHOSEN_SLOT,
    GUARDS_SLOT,
    NO_CANDIDATE,
    response_format_for,
    response_model_for,
)
from core.models.extraction_schemas.response_format_util import (
    assert_strict_schema_supported,
)
from core.models.rule_catalog import RuleCatalog
from core.services.applied_rule_validation import (
    passed_implied_by,
    validate_applied_rules,
)

from data_etl_app.models.types_and_enums import ConceptTypeEnum
from data_etl_app.services.prompt_service import (
    SINGLE_STAGE_PROMPT_FILE_PATHS,
    STAGED_PROMPT_FILE_PATHS,
)
from data_etl_app.services.prompt_assembly_service import (
    PARENT_ENTITY_TOKEN,
    SKELETON_BY_STAGE,
    PromptAssemblyError,
    _dumps_example,
    _sentinel_branch_id,
    load_all_catalogs,
    prompt_s3_key,
    render_prompt,
    rendered_sha256,
)

CATALOGS = load_all_catalogs()

# "<whichever of A, B applied>" / "<... you found violated>" — a slot the model
# fills, in a rule id or an outcome.
_ALTERNATIVES = re.compile(r"^<whichever of (.+?)(?: applied| you found violated)>$")


def _rendered_example(catalog: RuleCatalog) -> dict:
    """The worked JSON response out of the rendered prompt, parsed."""
    return json.loads(render_prompt(catalog).split("```json")[1].split("```")[0])


def _example_units(catalog: RuleCatalog, example: dict) -> Iterator[tuple[str, dict]]:
    """``(where, unit)`` for every unit in the example that carries rule slots —
    the whole response in binary classification, the entry itself in screening,
    each option or category in grounding.

    Rule slots are hoisted onto the unit now (see ``catalog_wire_schema``), so a
    unit IS the thing that carries them rather than holding an ``applied_rules``
    list. Screening's no-candidate entries carry none at all, by design.
    """
    if catalog.stage == "binary_classification":  # one object, one unit
        yield "response", example
        return
    for entries in example.values():
        for entry in entries:
            if catalog.stage == "phrase_relationship_screening":
                if entry.get("outcome") == NO_CANDIDATE:
                    continue
                yield entry["phrase"], entry
                continue
            for unit in entry.get("options", entry.get("categories", [])):
                label = unit.get("option", unit.get("category"))
                yield f"{entry['phrase']} / {label}", unit


def _rules_of(catalog: RuleCatalog, unit: dict) -> list[dict]:
    """A unit's hoisted slots back as ``{rule_id, outcome, explanation}`` dicts.

    The same reconstruction ``flatten_rule_slots`` performs at parse time, over the
    example's placeholder text rather than a decoded model — so these tests keep
    asserting about the STORED shape, which is what the parser and the catalog's
    report policy are both expressed in.
    """
    rules: list[dict] = []
    for rule in catalog.walk_rules():
        if rule.report_when == "always" and rule.id in unit:
            rules.append({"rule_id": rule.id, **unit[rule.id]})
    if CHOSEN_SLOT in unit:
        rules.append({"rule_id": unit[CHOSEN_SLOT]["rule_id"], "outcome": "chosen",
                      "explanation": unit[CHOSEN_SLOT]["explanation"]})
    for fired in unit.get(GUARDS_SLOT, []):
        rules.append({"rule_id": fired["rule_id"], "outcome": "violated",
                      "explanation": fired["explanation"]})
    return rules


def _resolve_slots(rule: dict) -> dict:
    """Pick one alternative wherever the example left the choice to the model."""
    resolved = dict(rule)
    for field in ("rule_id", "outcome"):
        match = _ALTERNATIVES.match(resolved[field])
        if match:
            resolved[field] = match.group(1).split(", ")[0]
    return resolved


def _applied_rules_of(catalog: RuleCatalog, unit: dict) -> list[AppliedRule]:
    """One unit's rules, with the model's slots resolved to one alternative."""
    return [
        AppliedRule.model_validate(_resolve_slots(rule))
        for rule in _rules_of(catalog, unit)
    ]


def _fill_placeholders(node, schema: dict, root: dict):
    """The example with every ``<...>`` slot replaced by a value legal at that
    position, read off the schema itself.

    The example is deliberately full of slots the model fills — decision #22 keeps
    the matching ladder and the condition outcomes open rather than naming one, so
    the example anchors no branch. That makes it un-decodable as written, which is
    correct and is why this resolves against the schema instead of guessing.
    """
    while "$ref" in schema:
        target = root
        for part in schema["$ref"].lstrip("#/").split("/"):
            target = target[part]
        schema = target

    if "anyOf" in schema:
        # Pick the branch whose tag matches the entry's own, so a no-candidate entry
        # is not filled in as a judged one.
        for branch in schema["anyOf"]:
            filled = _fill_placeholders(node, branch, root)
            if filled is not None:
                return filled
        return None

    if isinstance(node, dict):
        properties = schema.get("properties", {})
        if set(node) != set(properties):
            return None  # not this branch
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
        # A slot in a field that is not a string. Every slot is written as prose in
        # angle brackets, so `"confidence": "<integer from 0 to 100>"` is a string
        # standing in for an int — the convention, not a defect. Strict decoding
        # emits the real type regardless.
        return {"integer": 0, "number": 0, "boolean": False}.get(
            schema.get("type"), "x"
        )
    return node


@pytest.mark.parametrize("prompt_name", sorted(CATALOGS))
def test_the_worked_example_decodes_under_the_schema_the_model_is_sent(prompt_name):
    """The invariant the 2026-08-11 abort came down to: the prompt and the parser
    must agree on the shape.

    They are generated from one catalog now, so this pins that they stay generated
    from it — an example showing a field the schema forbids, or omitting one it
    requires, is a whole group request lost every time the model copies it.
    """
    catalog = CATALOGS[prompt_name]
    schema = response_format_for(catalog)["json_schema"]["schema"]

    filled = _fill_placeholders(_rendered_example(catalog), schema, schema)
    assert filled is not None, f"{prompt_name}: example matches no branch of the schema"

    response_model_for(catalog).model_validate(filled)


@pytest.mark.parametrize("prompt_name", sorted(CATALOGS))
def test_the_generated_schema_is_one_strict_mode_accepts(prompt_name):
    """Catalog-generated schemas can outgrow OpenAI's limits as rules are added, and
    the failure mode is every request for that stage rejected at run time. Checked
    here so a catalog edit that crosses a limit fails in CI instead."""
    assert_strict_schema_supported(
        response_format_for(CATALOGS[prompt_name]), where=prompt_name
    )


def test_derived_paths_agree_with_prompt_service():
    """The assembler derives output paths from the stage instead of reading
    STAGED_PROMPT_FILE_PATHS, because that map also feeds PROMPT_NAMES and would
    break PromptService init for a prompt not yet in S3. They must still agree for
    every prompt the service already knows about."""
    for prompt_name, catalog in CATALOGS.items():
        registered = STAGED_PROMPT_FILE_PATHS.get(
            prompt_name
        ) or SINGLE_STAGE_PROMPT_FILE_PATHS.get(prompt_name)
        if registered is None:
            continue  # not published yet; added to the service in the publish step
        assert prompt_s3_key(catalog) == registered


def test_every_catalog_is_keyed_by_its_prompt_name():
    assert CATALOGS, "no rule catalogs found"
    for prompt_name, catalog in CATALOGS.items():
        assert catalog.prompt_name == prompt_name
        assert catalog.catalog_version.startswith(prompt_name)


def test_every_catalog_stage_has_a_skeleton():
    for catalog in CATALOGS.values():
        assert catalog.stage in SKELETON_BY_STAGE


@pytest.mark.parametrize("prompt_name", sorted(CATALOGS))
def test_catalog_renders_without_unresolved_placeholders(prompt_name):
    """render_prompt raises on any placeholder that is neither resolved at assembly
    time nor a known runtime token, so reaching the assertions means it is clean."""
    catalog = CATALOGS[prompt_name]
    text = render_prompt(catalog)

    assert text.strip()
    assert "{{entity_noun}}" not in text
    assert "{{entity_relationships." not in text
    assert PARENT_ENTITY_TOKEN not in text
    assert catalog.entity_noun in text


@pytest.mark.parametrize(
    "prompt_name",
    sorted(
        name
        for name, catalog in CATALOGS.items()
        if catalog.stage == "phrase_recursive_grounding"
    ),
)
def test_recursive_grounding_keeps_the_tokens_the_service_substitutes(prompt_name):
    """llm_recursive_grounding_service substitutes these per recursion level, so
    they must survive rendering. Sourcing them from ConceptTypeEnum here is what
    stops the prompt and the service from drifting apart."""
    catalog = CATALOGS[prompt_name]
    text = render_prompt(catalog)

    concept_type = ConceptTypeEnum(catalog.field_types[0])
    parent_token, children_token = concept_type.recursive_grounding_placeholders

    assert parent_token in text
    assert children_token in text


def test_rendering_is_deterministic():
    catalog = CATALOGS["industry_phrase_relationship_screening"]
    assert rendered_sha256(render_prompt(catalog)) == rendered_sha256(
        render_prompt(catalog)
    )


def test_publish_metadata_does_not_change_what_renders():
    """published is written back after upload; if it fed the render, recording it
    would change the next render and never reach a fixed point."""
    catalog = CATALOGS["industry_phrase_relationship_screening"]
    before = render_prompt(catalog)

    republished = catalog.model_copy(deep=True)
    republished.published.s3_version_id = "some-s3-version-id"
    republished.published.rendered_sha256 = rendered_sha256(before)

    assert render_prompt(republished) == before


def test_reportable_rules_are_listed_and_notes_are_not():
    """Notes render as bare sub-bullets. Giving them an id would invite the model
    to report one, which parse-time validation rejects."""
    catalog = CATALOGS["industry_phrase_recursive_grounding"]
    text = render_prompt(catalog)

    for rule in catalog.walk_rules():
        if rule.reportable:
            assert f"[{rule.id}]" in text, f"{rule.id} missing from rendered prompt"
        else:
            assert f"[{rule.id}]" not in text, f"note {rule.id} rendered with an id"
            assert rule.text in text, f"note {rule.id} text missing entirely"


def test_report_block_separates_always_from_chosen_and_violated():
    catalog = CATALOGS["industry_phrase_recursive_grounding"]
    text = render_prompt(catalog)

    always = sorted(catalog.always_reported_rule_ids())
    assert set(always) == {"RGR-Q1", "RGR-Q2", "RGR-Q3", "RGR-QC1"}
    for rule_id in always:
        assert re.search(rf"whichever outcome it reached:[^\n]*{rule_id}", text)

    # The matching ladder is a single choice, not four independent reports — one
    # field on the wire, so the prompt names the field rather than a count.
    assert re.search(r'"chosen" holds the single branch[^\n]*RGR-M1', text)


def test_the_example_serialiser_matches_json_dumps_where_it_inlines_nothing():
    """It hand-rolls indent=2 so it can collapse chosen nodes, which is only safe
    while the parts it does NOT collapse stay byte-identical to the real thing."""
    nothing_inlined = {
        "a": [1, "two", None, True],
        "b": {"c": {"d": []}, "e": {}},
        "f": [{"g": "h"}, [[1], []]],
    }

    assert _dumps_example(nothing_inlined) == json.dumps(nothing_inlined, indent=2)


@pytest.mark.parametrize("prompt_name", sorted(CATALOGS))
def test_the_example_inlines_rule_objects_and_empty_entries(prompt_name):
    """The model copies the example's formatting, so these one-liners are what keep
    rule objects and rejected phrases off six lines each in every completion.
    Reading the JSON back cannot see formatting, hence the assertion on text."""
    example = render_prompt(CATALOGS[prompt_name]).split("```json")[1].split("```")[0]

    inlined = 0
    for line in example.splitlines():
        stripped = line.strip()
        # A rule's slot, under the field named for its id; or a fired guard / the
        # chosen branch, which name the id inside because the model picks it.
        if '"explanation":' in stripped:
            assert stripped.endswith(("},", "}")), (
                f"rule object was split across lines: {stripped[:60]}..."
            )
            inlined += 1
        if f'"{NO_CANDIDATE}"' in stripped:
            assert stripped.startswith("{"), (
                f"no-candidate entry was split across lines: {stripped[:60]}..."
            )

    assert inlined, "no rule objects to check"


@pytest.mark.parametrize("prompt_name", sorted(CATALOGS))
def test_output_example_would_survive_parse_time_validation(prompt_name):
    """The example is the shape the model copies, so anything it shows that the
    parser would reject costs a whole group request. Running it through the real
    validator is what stops the two drifting apart.

    Placeholder slots are resolved to one of their alternatives first: an unresolved
    ``<whichever of ...>`` is not a rule id and the validator would rightly refuse it.
    """
    catalog = CATALOGS[prompt_name]

    for where, unit in _example_units(catalog, _rendered_example(catalog)):
        validate_applied_rules(
            catalog=catalog,
            applied_rules=_applied_rules_of(catalog, unit),
            where=where,
        )


@pytest.mark.parametrize("prompt_name", sorted(CATALOGS))
def test_every_example_rule_asks_for_a_real_explanation(prompt_name):
    """The explanation is the whole of the justification since evidence was dropped
    (2026-08-11), and the example is what the model copies. A slot that does not ask
    for the sources to be quoted teaches the shape that made the field worth keeping
    unenforced in the first place.

    ``not_triggered`` is the deliberate exception: nothing was evaluated, so there is
    nothing to quote and its placeholder says only why."""
    catalog = CATALOGS[prompt_name]

    checked = 0
    for _, unit in _example_units(catalog, _rendered_example(catalog)):
        for rule in _rules_of(catalog, unit):
            explanation = rule["explanation"]
            assert explanation.strip(), f"{prompt_name}: {rule['rule_id']}"
            checked += 1
            if rule["outcome"] == "not_triggered":
                continue
            # What there is to cite differs by stage, so the slot must name the
            # catalog's own evidence_source rather than any fixed phrase.
            assert f"citing {catalog.evidence_source}" in explanation, (
                f"{prompt_name}: {rule['rule_id']} shows an explanation slot "
                f"that does not name the sources to cite: {explanation!r}"
            )

    assert checked, f"{prompt_name}: example carries no rules to check"


@pytest.mark.parametrize("prompt_name", sorted(CATALOGS))
def test_output_example_leaves_the_matching_ladder_open(prompt_name):
    """Naming a branch teaches the model to echo that branch back. The ladder is a
    decision it has to make, so the example lists the alternatives instead — except
    for the escape hatch, whose scenario IS that branch."""
    catalog = CATALOGS[prompt_name]
    example = render_prompt(catalog).split("```json")[1]
    sentinel_branch = _sentinel_branch_id(catalog)

    named = [
        rule.id
        for rule in catalog.walk_rules()
        if rule.report_when == "when_chosen"
        and rule.id != sentinel_branch
        and f'"{rule.id}"' in example
    ]
    branches = [
        rule.id for rule in catalog.walk_rules() if rule.report_when == "when_chosen"
    ]
    # One branch left after the sentinel is not a choice, so naming it is honest.
    if len(set(branches) - {sentinel_branch}) > 1:
        assert not named, f"example names ladder branches {named}"


@pytest.mark.parametrize("prompt_name", sorted(CATALOGS))
def test_output_example_reports_every_always_reported_rule(prompt_name):
    """The single anonymous rule object the example used to show read as 'report one
    rule', which fails validation for every catalog here."""
    catalog = CATALOGS[prompt_name]
    required = catalog.always_reported_rule_ids()

    for where, unit in _example_units(catalog, _rendered_example(catalog)):
        reported = {rule["rule_id"] for rule in _rules_of(catalog, unit)}
        assert required <= reported, f"{where} omits {sorted(required - reported)}"


@pytest.mark.parametrize(
    "prompt_name",
    sorted(
        name
        for name, catalog in CATALOGS.items()
        if catalog.stage == "phrase_relationship_screening"
    ),
)
def test_screening_example_covers_every_verdict_path(prompt_name):
    """`passed` is derived from the reported rules rather than reported, so the
    example has to show each way that derivation can come out: qualified, a
    condition that did not hold, a guard, and no candidate at all."""
    catalog = CATALOGS[prompt_name]
    entries = _rendered_example(catalog)["screenings"]
    assert len(entries) == 4

    verdicts = [
        passed_implied_by(catalog, _applied_rules_of(catalog, entry))
        for entry in entries
    ]
    assert verdicts == [True, False, False, False]
    # The other branch of the union: no candidate, so no rule slots at all, and an
    # explanation carrying the whole of its record.
    assert entries[-1]["outcome"] == NO_CANDIDATE
    assert "identified_entity" not in entries[-1]
    assert entries[-1]["explanation"]


@pytest.mark.parametrize(
    "prompt_name",
    sorted(
        name
        for name, catalog in CATALOGS.items()
        if catalog.stage == "phrase_relationship_screening"
    ),
)
def test_screening_example_shows_a_guard_overriding_satisfied_conditions(prompt_name):
    """The point of the guard entry: every condition holds and the phrase is still
    rejected. If its conditions ever stop being all-satisfied it stops showing the
    one thing no other entry does."""
    catalog = CATALOGS[prompt_name]
    guard_entry = _rendered_example(catalog)["screenings"][2]
    rules = _applied_rules_of(catalog, guard_entry)

    conditions = {rule.id for rule in catalog.walk_rules() if rule.kind == "condition"}
    guards = {rule.id for rule in catalog.walk_rules() if rule.kind == "guard"}

    assert all(
        rule.outcome == "satisfied" for rule in rules if rule.rule_id in conditions
    )
    assert [rule.outcome for rule in rules if rule.rule_id in guards] == ["violated"]
    assert not passed_implied_by(catalog, rules)


@pytest.mark.parametrize(
    "prompt_name",
    sorted(name for name, catalog in CATALOGS.items() if catalog.sentinel_tag),
)
def test_output_example_shows_the_sentinel_spelled_as_the_parser_expects(prompt_name):
    """The sentinel reaching the results as if it were a discovered label is the bug
    this spelling guards against; showing it in the example is where the model copies
    it from."""
    catalog = CATALOGS[prompt_name]
    entries = _rendered_example(catalog)["groundings"]

    labels = [
        label
        for entry in entries
        for unit in entry.get("options", entry.get("categories", []))
        for label in [unit.get("option", unit.get("category"))]
    ]
    assert catalog.sentinel_tag in labels


def test_catalog_covering_two_field_types_cannot_use_the_parent_token():
    """The parent token is concept-specific, so it cannot be resolved for a catalog
    that serves more than one field type."""
    catalog = CATALOGS["industry_phrase_recursive_grounding"].model_copy(deep=True)
    catalog.field_types = ["industries", "conformity_attestations"]

    with pytest.raises(PromptAssemblyError, match="exactly one field type"):
        render_prompt(catalog)


def test_rule_kind_fixes_its_reporting_policy():
    """A catalog cannot declare a rule the parser would then refuse to accept."""
    with pytest.raises(ValueError, match="report_when"):
        RuleCatalog.model_validate(
            {
                "catalog_version": "x.1",
                "prompt_name": "x",
                "stage": "phrase_relationship_screening",
                "field_types": ["industries"],
                "entity_noun": "thing",
                "entity_relationships": {
                    "base": "do",
                    "third_person": "does",
                    "gerund": "doing",
                },
                "outcome_vocab": {"guard": ["clear", "violated", "not_triggered"]},
                "sections": [
                    {
                        "section_id": "guards",
                        "heading": "h",
                        "combinator": "any",
                        "rules": [
                            {
                                "id": "G1",
                                "kind": "guard",
                                "reportable": True,
                                "report_when": "always",  # guards report on_violation
                                "text": "t",
                            }
                        ],
                    }
                ],
                "published": {},
            }
        )
