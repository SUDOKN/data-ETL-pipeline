import json
import re
from typing import Iterator

import pytest

from core.models.extraction_schemas.applied_rule import AppliedRule
from core.models.rule_catalog import RuleCatalog
from core.services.applied_rule_validation import (
    passed_implied_by,
    validate_applied_rules,
)

from data_etl_app.models.types_and_enums import ConceptTypeEnum
from data_etl_app.services.prompt_service import STAGED_PROMPT_FILE_PATHS
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


def _example_units(example: dict) -> Iterator[tuple[str, list[dict]]]:
    """``(where, applied_rules)`` for every unit in the example that reports rules —
    the entry itself in screening, each option or category in grounding."""
    for entries in example.values():
        for entry in entries:
            if "applied_rules" in entry:
                yield entry["phrase"], entry["applied_rules"]
                continue
            for unit in entry.get("options", entry.get("categories", [])):
                label = unit.get("option", unit.get("category"))
                yield f"{entry['phrase']} / {label}", unit["applied_rules"]


def _resolve_slots(rule: dict) -> dict:
    """Pick one alternative wherever the example left the choice to the model."""
    resolved = dict(rule)
    for field in ("rule_id", "outcome"):
        match = _ALTERNATIVES.match(resolved[field])
        if match:
            resolved[field] = match.group(1).split(", ")[0]
    return resolved


def _applied_rules_of(entry: dict) -> list[AppliedRule]:
    """One entry's rules, with the model's slots resolved to one alternative."""
    return [
        AppliedRule.model_validate(_resolve_slots(rule))
        for rule in entry["applied_rules"]
    ]


def test_derived_paths_agree_with_prompt_service():
    """The assembler derives output paths from the stage instead of reading
    STAGED_PROMPT_FILE_PATHS, because that map also feeds PROMPT_NAMES and would
    break PromptService init for a prompt not yet in S3. They must still agree for
    every prompt the service already knows about."""
    for prompt_name, catalog in CATALOGS.items():
        registered = STAGED_PROMPT_FILE_PATHS.get(prompt_name)
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

    # The matching ladder is a single choice, not four independent reports.
    assert re.search(r"exactly one of these[^\n]*RGR-M1", text)


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

    assert '"rule_id"' in example, "no rule objects to check"
    for line in example.splitlines():
        stripped = line.strip()
        if '"rule_id"' in stripped:
            assert stripped.endswith(("},", "}")), (
                f"rule object was split across lines: {stripped[:60]}..."
            )
        if '"identified_entity": null' in stripped:
            assert stripped.startswith("{"), (
                f"null-candidate entry was split across lines: {stripped[:60]}..."
            )


@pytest.mark.parametrize("prompt_name", sorted(CATALOGS))
def test_output_example_would_survive_parse_time_validation(prompt_name):
    """The example is the shape the model copies, so anything it shows that the
    parser would reject costs a whole group request. Running it through the real
    validator is what stops the two drifting apart.

    Placeholder slots are resolved to one of their alternatives first: an unresolved
    ``<whichever of ...>`` is not a rule id and the validator would rightly refuse it.
    """
    catalog = CATALOGS[prompt_name]

    for where, applied_rules in _example_units(_rendered_example(catalog)):
        if not applied_rules:
            continue  # the null-entity entry reports nothing, by design

        validate_applied_rules(
            catalog=catalog,
            applied_rules=[
                AppliedRule.model_validate(_resolve_slots(rule))
                for rule in applied_rules
            ],
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
    example = _rendered_example(CATALOGS[prompt_name])

    checked = 0
    for entries in example.values():
        for entry in entries:
            units = entry.get("options", entry.get("categories"))
            unit_rules = (
                [entry["applied_rules"]]
                if units is None
                else [unit["applied_rules"] for unit in units]
            )
            for rules in unit_rules:
                for rule in rules:
                    explanation = rule["explanation"]
                    assert explanation.strip(), f"{prompt_name}: {rule['rule_id']}"
                    checked += 1
                    if rule["outcome"] == "not_triggered":
                        continue
                    assert "citing the phrase and its relationship summary" in (
                        explanation
                    ), (
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

    for where, applied_rules in _example_units(_rendered_example(catalog)):
        if not applied_rules:
            continue
        reported = {rule["rule_id"] for rule in applied_rules}
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
        passed_implied_by(catalog, _applied_rules_of(entry)) for entry in entries
    ]
    assert verdicts == [True, False, False, False]
    assert entries[-1]["identified_entity"] is None
    assert entries[-1]["applied_rules"] == []


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
    rules = _applied_rules_of(guard_entry)

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
    catalog.field_types = ["industries", "certificates"]

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
