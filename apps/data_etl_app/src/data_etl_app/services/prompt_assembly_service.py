"""Render extraction prompts from rule catalogs.

This is a BUILD STEP, not a runtime path: ``assemble_prompts.py`` renders the
catalogs into ``new_prompts/`` and publishes them to S3, and the pipeline goes on
reading prompts from S3 exactly as before. Keeping it out of the request path is
what preserves ``Prompt.s3_version_id`` as real provenance.

Placeholders resolve in two waves. ``{{entity_noun}}`` and
``{{entity_relationships.*}}`` come from the catalog header and are resolved HERE.
``{{parent_entity}}`` / ``{{types_of_parent_entity}}`` are rewritten here into the
concept-specific tokens that ``llm_recursive_grounding_service`` substitutes per
recursion level, and must survive rendering unresolved.

``{{output_example}}`` is BUILT here rather than written into the skeletons. It
used to be a literal, on the grounds that it was prompt wording varying only by
stage. That stopped being true once the example had to name rule ids: the ids
differ per catalog (equipment screening carries three guards where the other six
carry one), and a shared skeleton cannot name them.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Callable, Optional

from core.models.rule_catalog import NOTE_KIND, RuleCatalog, RuleNode, RuleSection

from data_etl_app.models.types_and_enums import ConceptTypeEnum

_PROMPTS_DIR = Path(__file__).resolve().parent.parent / "knowledge" / "prompts"
CATALOG_DIR = _PROMPTS_DIR / "rule_catalog"
SKELETON_DIR = _PROMPTS_DIR / "skeletons"

SKELETON_BY_STAGE = {
    "phrase_relationship_screening": "screening.skeleton.txt",
    "phrase_initial_grounding": "initial_grounding.skeleton.txt",
    "phrase_recursive_grounding": "recursive_grounding.skeleton.txt",
    "phrase_freehand_grounding": "freehand_grounding.skeleton.txt",
}

# The S3 key a prompt is published under. Derived from the stage rather than read
# out of PromptService.STAGED_PROMPT_FILE_PATHS, because that map also feeds
# PROMPT_NAMES: listing a prompt there before it exists in S3 would break
# PromptService init for every bot. test_prompt_assembly_service asserts the two
# agree for prompts that are already published.
STAGE_DIR_BY_STAGE = {
    "phrase_relationship_screening": "4_phrase_relationship_screening",
    "phrase_freehand_grounding": "5_freehand_grounding",
    "phrase_initial_grounding": "5_initial_grounding",
    "phrase_recursive_grounding": "6_recursive_grounding",
}

# Rendered prompts are written HERE. `final_texts/` is build output and is
# gitignored; `assembled/` holds what this module renders from catalogs, while
# `static/` holds the hand-written prompts that have no catalog (search,
# recursive search, relationship, single-stage). Both publish to the same flat
# `multi_stage/...` S3 keys — the assembled/static split is local only.
ASSEMBLED_PROMPTS_DIR = _PROMPTS_DIR / "final_texts" / "assembled"
STATIC_PROMPTS_DIR = _PROMPTS_DIR / "final_texts" / "static"


def prompt_s3_key(catalog: RuleCatalog) -> str:
    """The S3 key this prompt publishes to — the same key its hand-written
    predecessor occupies."""
    stage_dir = STAGE_DIR_BY_STAGE.get(catalog.stage)
    if stage_dir is None:
        raise PromptAssemblyError(
            f"{catalog.prompt_name}: no output directory registered for stage "
            f"{catalog.stage!r}"
        )
    return f"multi_stage/{stage_dir}/{catalog.prompt_name}.txt"


def assembled_prompt_path(catalog: RuleCatalog) -> Path:
    """Where the rendered prompt is written locally, mirroring the S3 layout."""
    return ASSEMBLED_PROMPTS_DIR / prompt_s3_key(catalog)

# Written by llm_recursive_grounding_service at request-build time, so they must
# still be present in the rendered .txt.
PARENT_ENTITY_TOKEN = "{{parent_entity}}"
TYPES_OF_PARENT_ENTITY_TOKEN = "{{types_of_parent_entity}}"

_RULES_BLOCK = "{{rules_block}}"
_REPORT_BLOCK = "{{report_block}}"
_OUTPUT_EXAMPLE = "{{output_example}}"

# What each outcome MEANS, where the word alone is ambiguous. A section combinator
# of "all" says every member must hold but not which outcomes count as holding —
# left unsaid, that gets resolved differently from one phrase to the next and the
# stored record stops being comparable. Keyed by kind, appended under that kind's
# vocabulary line. Kinds reported only on one outcome need no gloss: the report
# policy above already says when to write it.
OUTCOME_GLOSS_BY_KIND: dict[str, str] = {
    "condition": (
        'Only "satisfied" counts as the condition holding. Write "not_triggered" '
        "when there was nothing to evaluate it against — for example when no "
        "{{entity_noun}} was identified for it to refer to — and \"failed\" when you "
        "evaluated it and it did not hold. Neither of those counts as holding."
    ),
}


class PromptAssemblyError(Exception):
    """A catalog and its skeleton could not be rendered into a prompt."""


def load_catalog(path: Path) -> RuleCatalog:
    try:
        catalog = RuleCatalog.model_validate_json(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise PromptAssemblyError(f"{path.name}: invalid rule catalog: {exc}") from exc

    if catalog.prompt_name != path.stem:
        raise PromptAssemblyError(
            f"{path.name}: prompt_name {catalog.prompt_name!r} does not match the "
            f"filename. Catalogs are keyed by prompt name, one per prompt file."
        )
    return catalog


def load_all_catalogs(catalog_dir: Path = CATALOG_DIR) -> dict[str, RuleCatalog]:
    return {
        catalog.prompt_name: catalog
        for catalog in (load_catalog(p) for p in sorted(catalog_dir.glob("*.json")))
    }


def build_rule_catalog_lookup(
    catalog_dir: Path = CATALOG_DIR,
) -> Callable[[str, str], Optional[RuleCatalog]]:
    """A ``(stage, field_type) -> RuleCatalog`` lookup for
    ``core.services.rule_catalog_registry``, so the parse functions in ``core`` can
    validate applied rules without importing app-owned files.

    Catalogs are read once here rather than per call; they are static build inputs.
    """
    by_stage_and_field: dict[tuple[str, str], RuleCatalog] = {}
    for catalog in load_all_catalogs(catalog_dir).values():
        for field_type in catalog.field_types:
            key = (catalog.stage, field_type)
            if key in by_stage_and_field:
                raise PromptAssemblyError(
                    f"two catalogs claim {key}: "
                    f"{by_stage_and_field[key].prompt_name} and "
                    f"{catalog.prompt_name}. (stage, field_type) must identify "
                    f"exactly one prompt."
                )
            by_stage_and_field[key] = catalog

    def lookup(stage: str, field_type: str) -> Optional[RuleCatalog]:
        return by_stage_and_field.get((stage, field_type))

    return lookup


def load_skeleton(stage: str, skeleton_dir: Path = SKELETON_DIR) -> str:
    filename = SKELETON_BY_STAGE.get(stage)
    if filename is None:
        raise PromptAssemblyError(
            f"no skeleton registered for stage {stage!r}; known stages: "
            f"{sorted(SKELETON_BY_STAGE)}"
        )
    return (skeleton_dir / filename).read_text(encoding="utf-8")


def rendered_sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _resolve_entity_placeholders(text: str, catalog: RuleCatalog) -> str:
    relationships = catalog.entity_relationships
    return (
        text.replace("{{entity_noun}}", catalog.entity_noun)
        .replace("{{entity_relationships.base}}", relationships.base)
        .replace("{{entity_relationships.third_person}}", relationships.third_person)
        .replace("{{entity_relationships.gerund}}", relationships.gerund)
    )


def _resolve_runtime_tokens(text: str, catalog: RuleCatalog) -> str:
    """Rewrite the generic parent tokens into the concept-specific ones the
    recursive-grounding service substitutes at request-build time. Sourced from
    ``ConceptTypeEnum`` so the two can never disagree."""
    if PARENT_ENTITY_TOKEN not in text and TYPES_OF_PARENT_ENTITY_TOKEN not in text:
        return text

    if len(catalog.field_types) != 1:
        raise PromptAssemblyError(
            f"{catalog.prompt_name}: uses {PARENT_ENTITY_TOKEN} but covers "
            f"{catalog.field_types}; the parent token is concept-specific so the "
            f"catalog must cover exactly one field type."
        )

    try:
        concept_type = ConceptTypeEnum(catalog.field_types[0])
    except ValueError as exc:
        raise PromptAssemblyError(
            f"{catalog.prompt_name}: uses {PARENT_ENTITY_TOKEN} but field type "
            f"{catalog.field_types[0]!r} is not a concept type"
        ) from exc

    parent_token, children_token = concept_type.recursive_grounding_placeholders
    return text.replace(PARENT_ENTITY_TOKEN, parent_token).replace(
        TYPES_OF_PARENT_ENTITY_TOKEN, children_token
    )


def _render_rule(rule: RuleNode, indent: str) -> list[str]:
    """One rule and its children, each led by its own id.

    NO outline numbers (dropped 2026-08-11). A rule was previously "1. [SCR-1] …",
    which gave every node two names — one stable and reported, one positional and
    not — and only the id is ever referred to, by the report block, by the response,
    and by stored records. The ordinal that mattered is already in the ids of an
    ordered section (M1 before M2), and document order carries the rest.

    Notes render as sub-bullets WITHOUT an id: they are guidance folded into the
    parent, never reported, and showing an id for them invites the model to report
    one and fail validation.
    """
    lines = [f"{indent}[{rule.id}] {rule.text}"]

    child_indent = indent + "   "
    for child in rule.children:
        if child.kind == NOTE_KIND:
            lines.append(f"{child_indent}- {child.text}")
            continue
        lines.append(f"{child_indent}[{child.id}] {child.text}")
    return lines


def _render_section(section: RuleSection) -> str:
    lines = [section.heading]
    for rule in section.rules:
        lines.extend(_render_rule(rule, indent=""))
    return "\n".join(lines)


def _render_rules_block(catalog: RuleCatalog) -> str:
    return "\n\n".join(_render_section(section) for section in catalog.sections)


def _ids_by_report_when(catalog: RuleCatalog) -> dict[str, list[str]]:
    grouped: dict[str, list[str]] = {}
    for rule in catalog.walk_rules():
        if not rule.reportable:
            continue
        grouped.setdefault(rule.report_when, []).append(rule.id)
    return grouped


def _render_report_block(catalog: RuleCatalog) -> str:
    grouped = _ids_by_report_when(catalog)
    lines = [
        "### Rules you must report:",
        # A structured `evidence` array of quoted slices stood here until
        # 2026-08-11, machine-checked against the source. The prohibition list that
        # policed it ("never reworded, never assembled from separate places, never
        # added to") never converged: each failure walked through whichever gap the
        # list had not named yet, because a model that needs a span the source does
        # not contain has no compliant move available. What is left is the positive
        # half — cite the sources, quote what you relied on — carried in prose,
        # where a paraphrase is a weakness an annotator can see rather than a parse
        # failure that costs the whole group request.
        "For every rule you report, give the rule id, the outcome you reached, and "
        "your explanation. Each explanation must cite the phrase and its "
        "relationship summary, quoting the words you relied on.",
        "",
    ]

    if any(section.combinator == "all" for section in catalog.sections):
        lines[2:2] = [
            "",
            "Conditions are read in order and build on one another, so a later "
            "condition's explanation does not need to re-establish what an earlier "
            "one already did. Quote only the words that bear on the rule you are "
            "reporting.",
        ]

    if grouped.get("always"):
        lines.append(
            f"- Report every one of these, whichever outcome it reached: "
            f"{', '.join(grouped['always'])}."
        )
    if grouped.get("when_chosen"):
        lines.append(
            f"- Report exactly one of these — the one you chose, with outcome "
            f"\"chosen\": {', '.join(grouped['when_chosen'])}."
        )
    if grouped.get("on_violation"):
        lines.append(
            f"- Report any of these ONLY when you find it violated: "
            f"{', '.join(grouped['on_violation'])}."
        )

    lines.append("")
    lines.append("The outcome you give must be one allowed for that rule:")
    for kind, outcomes in catalog.outcome_vocab.items():
        ids = sorted(
            rule.id for rule in catalog.walk_rules() if rule.kind == kind
        )
        lines.append(f"- {', '.join(ids)} → {', '.join(outcomes)}")
        gloss = OUTCOME_GLOSS_BY_KIND.get(kind)
        if gloss:
            lines.append(f"  {gloss}")

    return "\n".join(lines)


# --- Output example ---------------------------------------------------------
#
# One worked JSON response, covering the shapes a phrase's entry can take. It is
# CONCRETE about what the catalog fixes — which rules must appear, and the single
# outcome a guard or a preference can carry — and a placeholder everywhere the
# model still has to decide. Naming a branch of the matching ladder would teach it
# to echo that branch back, so the ladder stays a placeholder listing its
# alternatives; the sentinel branch is the exception, because the scenario that
# shows it IS that branch.
#
# An entry earns its place only by producing a different `applied_rules` shape.
# That is what keeps this from growing into a worked corpus: a guard adds a rule
# object identical in shape to every other one, so no scenario exists for it.
#
# Nothing here names a real industry, process, material, or machine. The phrase
# slot carries the scenario label instead, which is how the hand-written v4
# prompts stayed generic while showing more than one case.

# Each placeholder names both sources rather than deferring to the report block
# above, because this is the line the model is looking at while it writes the field.
# It is nearly free: a placeholder is prompt text, and the model replaces it rather
# than emitting it, so the length lands on the cached side and not on the completion.
# What it can still do is anchor how long an explanation runs, which is a reason to
# keep the wording tight — not a reason to make it refer elsewhere.
#
# `not_triggered` is the one outcome with nothing to cite: the rule never got a
# candidate to be about, so it explains why instead of quoting.
_EXPLANATION_BY_OUTCOME = {
    "satisfied": "<why this held, citing the phrase and its relationship summary>",
    "failed": "<why this did not hold, citing the phrase and its relationship summary>",
    "not_triggered": "<why there was nothing here to evaluate>",
    "chosen": "<why this is the branch that applied, citing the phrase and its relationship summary>",
    "violated": "<why this guard fired, citing the phrase and its relationship summary>",
}
_FALLBACK_EXPLANATION = (
    "<why this is the outcome, citing the phrase and its relationship summary>"
)

# How the always-reported conditions come out under each scenario.
_HELD = "held"
_DID_NOT_HOLD = "did_not_hold"
_NOTHING_IDENTIFIED = "nothing_identified"


def _example_rule(rule_id: str, outcome: str) -> dict[str, Any]:
    return {
        "rule_id": rule_id,
        "outcome": outcome,
        "explanation": _EXPLANATION_BY_OUTCOME.get(outcome, _FALLBACK_EXPLANATION),
    }


def _outcome_placeholder(outcomes: list[str]) -> str:
    """The outcome slot for a rule whose scenario does not fix its outcome —
    granularity being judged, say, which the act of matching does not decide."""
    reachable = [outcome for outcome in outcomes if outcome != "not_triggered"]
    if len(reachable) == 1:
        return reachable[0]
    return f"<whichever of {', '.join(reachable)} applied>"


def _condition_outcomes(catalog: RuleCatalog, mode: str) -> dict[str, str]:
    conditions = [rule for rule in catalog.walk_rules() if rule.kind == "condition"]

    if mode == _HELD:
        return {rule.id: "satisfied" for rule in conditions}
    if mode == _NOTHING_IDENTIFIED:
        return {rule.id: "not_triggered" for rule in conditions}

    # Conditions are a chain in document order, so the shape a rejection takes is
    # satisfied* failed? not_triggered* — the shape _check_condition_chain enforces.
    # The SECOND condition is the one shown failing: the first failing would mean
    # nothing was identified at all, which is the null-entity scenario instead.
    fails_at = 1 if len(conditions) > 1 else 0
    outcomes = {}
    for position, rule in enumerate(conditions):
        if position < fails_at:
            outcomes[rule.id] = "satisfied"
        elif position == fails_at:
            outcomes[rule.id] = "failed"
        else:
            outcomes[rule.id] = "not_triggered"
    return outcomes


def _sentinel_branch_id(catalog: RuleCatalog) -> Optional[str]:
    """The preference rule that tells the model to return the sentinel label.
    ``RuleCatalog`` already guarantees exactly one when ``sentinel_tag`` is set."""
    if catalog.sentinel_tag is None:
        return None
    return next(
        (
            rule.id
            for rule in catalog.walk_rules()
            if rule.kind == "preference" and catalog.sentinel_tag in rule.text
        ),
        None,
    )


def _ladder_placeholder(catalog: RuleCatalog) -> Optional[str]:
    """The rule_id slot for the branch taken, with the escape hatch left out — the
    scenarios that reach here identified something, so the sentinel branch is not
    among the alternatives."""
    sentinel = _sentinel_branch_id(catalog)
    branches = [
        rule.id
        for rule in catalog.walk_rules()
        if rule.report_when == "when_chosen" and rule.id != sentinel
    ]
    if not branches:
        return None
    if len(branches) == 1:
        return branches[0]
    return f"<whichever of {', '.join(branches)} applied>"


def _guard_placeholder(catalog: RuleCatalog) -> Optional[str]:
    """The rule_id slot for the guard that fired. Which guard is the model's
    finding, so a catalog declaring several lists them rather than picking one."""
    guards = [
        rule.id for rule in catalog.walk_rules() if rule.report_when == "on_violation"
    ]
    if not guards:
        return None
    if len(guards) == 1:
        return guards[0]
    return f"<whichever of {', '.join(guards)} you found violated>"


def _example_applied_rules(
    catalog: RuleCatalog,
    *,
    mode: str,
    branch: Optional[str] = None,
    guard: Optional[str] = None,
) -> list[dict[str, Any]]:
    """Every rule this scenario must report, in the order the rules are stated.

    Emitting them in document order puts the chosen branch where the matching
    ladder sits among the sections, and a fired guard after the conditions it
    overrides, so the example reads in the same sequence as the rules above it.
    """
    condition_outcomes = _condition_outcomes(catalog, mode)
    applied: list[dict[str, Any]] = []
    branch_emitted = False
    guard_emitted = False

    for rule in catalog.walk_rules():
        if rule.report_when == "always":
            outcome = condition_outcomes.get(rule.id)
            if outcome is None:
                # A non-condition reported regardless of outcome — granularity, say.
                outcome = (
                    "not_triggered"
                    if mode == _NOTHING_IDENTIFIED
                    else _outcome_placeholder(catalog.outcome_vocab[rule.kind])
                )
            applied.append(_example_rule(rule.id, outcome))
        elif (
            rule.report_when == "when_chosen" and branch is not None and not branch_emitted
        ):
            applied.append(_example_rule(branch, "chosen"))
            branch_emitted = True
        elif (
            rule.report_when == "on_violation" and guard is not None and not guard_emitted
        ):
            applied.append(_example_rule(guard, "violated"))
            guard_emitted = True

    return applied


def _screening_example(catalog: RuleCatalog) -> dict[str, Any]:
    """One entry per way the verdict can come out, since screening does not report
    a verdict and `passed_implied_by` reads it off these rules.

    The guard entry is the only one where every condition is "satisfied" and the
    phrase is still rejected — the guard alone does it. Nothing else in the example
    shows that, which is why it is here despite being shape-identical to the rest.

    Guards belong to this stage only. Grounding drops an option that fails rather
    than reporting it, so a rejected unit there has no entry to appear in.
    """
    entries = [
        {
            "phrase": "<phrase copied verbatim from the input, whose candidate qualified>",
            "identified_entity": "<the {{entity_noun}} you judged>",
            "applied_rules": _example_applied_rules(catalog, mode=_HELD),
        },
        {
            "phrase": "<another phrase, whose candidate did not qualify>",
            "identified_entity": "<the {{entity_noun}} you got furthest with>",
            "applied_rules": _example_applied_rules(catalog, mode=_DID_NOT_HOLD),
        },
    ]

    guard = _guard_placeholder(catalog)
    if guard is not None:
        entries.append(
            {
                "phrase": "<another phrase, whose candidate every condition held for, but which a guard ruled out>",
                "identified_entity": "<the {{entity_noun}} the guard ruled out>",
                "applied_rules": _example_applied_rules(
                    catalog, mode=_HELD, guard=guard
                ),
            }
        )

    entries.append(
        {
            "phrase": "<another phrase, which offered no candidate at all>",
            "identified_entity": None,
            "applied_rules": [],
        }
    )
    return {"screenings": entries}


def _grounding_example(
    catalog: RuleCatalog,
    *,
    unit_key: str,
    units_key: str,
    first_label: str,
    second_label: str,
) -> dict[str, Any]:
    """Single / several / escape hatch, for whichever of those a stage has.

    ``unit_key`` is the singular wire field — ``option`` where the stage chooses
    from a supplied list, ``category`` where it names one itself — and ``units_key``
    the array that holds them. Both are spelled out rather than pluralised here,
    because the wire field is ``categories`` and no rule turns one into the other.
    """
    ladder = _ladder_placeholder(catalog)

    def unit(label: str, *, mode: str, branch: Optional[str]) -> dict[str, Any]:
        return {
            unit_key: label,
            "applied_rules": _example_applied_rules(catalog, mode=mode, branch=branch),
        }

    entries: list[dict[str, Any]] = [
        {
            "phrase": "<phrase copied verbatim from the input>",
            units_key: [unit(first_label, mode=_HELD, branch=ladder)],
        },
        {
            "phrase": "<another phrase, from which you identified more than one>",
            units_key: [
                unit(first_label, mode=_HELD, branch=ladder),
                unit(second_label, mode=_HELD, branch=ladder),
            ],
        },
    ]

    sentinel_branch = _sentinel_branch_id(catalog)
    if sentinel_branch is not None:
        entries.append(
            {
                "phrase": "<another phrase, from which none could be identified>",
                units_key: [
                    unit(
                        catalog.sentinel_tag,
                        mode=_NOTHING_IDENTIFIED,
                        branch=sentinel_branch,
                    )
                ],
            }
        )

    return {"groundings": entries}


def _option_grounding_example(catalog: RuleCatalog) -> dict[str, Any]:
    return _grounding_example(
        catalog,
        unit_key="option",
        units_key="options",
        first_label='<an option, copied verbatim from its "name" field>',
        second_label=(
            "<a second option for the same phrase — copied verbatim likewise, or "
            "one you proposed when none of the provided ones fit>"
        ),
    )


def _category_grounding_example(catalog: RuleCatalog) -> dict[str, Any]:
    return _grounding_example(
        catalog,
        unit_key="category",
        units_key="categories",
        first_label="<the {{entity_noun}} you named>",
        second_label="<a second, distinct {{entity_noun}} named from the same phrase>",
    )


EXAMPLE_BUILDER_BY_STAGE = {
    "phrase_relationship_screening": _screening_example,
    "phrase_initial_grounding": _option_grounding_example,
    "phrase_recursive_grounding": _option_grounding_example,
    "phrase_freehand_grounding": _category_grounding_example,
}


# Which nodes print on one line. The model copies the example's FORMATTING as well
# as its shape — an example with one-line rule objects came back with one-line rule
# objects — so this is not cosmetics, it sets the shape of every completion the
# prompt anchors, which is uncached output and where the cost actually is.
#
# The pull is toward minifying the whole thing, and that was tried and measured
# WORSE (2026-08-11): a fully minified example is outside the pretty-printed idiom
# gpt-4.1 emits by default, so it ignored the example wholesale, expanded the spans
# back over six lines, and cost ~4% of the completion. The example has to stay
# recognisably pretty-printed. Collapsing individual nodes within it is the part
# that carries; the two below were each worth ~3-5%, on opposite kinds of batch —
# rule objects when most phrases qualify, null entries when most are rejected.
def _prints_on_one_line(node: Any) -> bool:
    if not isinstance(node, dict):
        return False
    if "rule_id" in node:
        return True  # a rule object: three short fields
    # An entry that identified no candidate: two short fields and an empty list,
    # which indent=2 otherwise spends five lines on.
    return "identified_entity" in node and node["identified_entity"] is None


def _dumps_example(node: Any, _level: int = 0) -> str:
    """``json.dumps(node, indent=2)`` except that ``_prints_on_one_line`` nodes are
    written compactly. Byte-identical to json.dumps for everything it does not
    inline, which is what the round-trip test pins."""
    if _prints_on_one_line(node):
        return json.dumps(node, ensure_ascii=False)

    pad, inner = "  " * _level, "  " * (_level + 1)
    if isinstance(node, dict) and node:
        body = ",\n".join(
            f"{inner}{json.dumps(key, ensure_ascii=False)}: "
            f"{_dumps_example(value, _level + 1)}"
            for key, value in node.items()
        )
        return f"{{\n{body}\n{pad}}}"
    if isinstance(node, list) and node:
        body = ",\n".join(f"{inner}{_dumps_example(item, _level + 1)}" for item in node)
        return f"[\n{body}\n{pad}]"
    return json.dumps(node, ensure_ascii=False)


def _render_output_example(catalog: RuleCatalog) -> str:
    builder = EXAMPLE_BUILDER_BY_STAGE.get(catalog.stage)
    if builder is None:
        raise PromptAssemblyError(
            f"{catalog.prompt_name}: no output example registered for stage "
            f"{catalog.stage!r}"
        )

    return _dumps_example(builder(catalog))


def render_prompt(catalog: RuleCatalog, skeleton_text: Optional[str] = None) -> str:
    """Render ``catalog`` into finished prompt text.

    ``published`` is deliberately not an input: recording where a rendered prompt
    landed in S3 must never change what renders next time.
    """
    skeleton = (
        load_skeleton(catalog.stage) if skeleton_text is None else skeleton_text
    )

    text = skeleton.replace(_RULES_BLOCK, _render_rules_block(catalog))
    text = text.replace(_REPORT_BLOCK, _render_report_block(catalog))
    # Before the entity pass: the example's own slots are written with
    # {{entity_noun}} and resolve in it.
    text = text.replace(_OUTPUT_EXAMPLE, _render_output_example(catalog))
    text = _resolve_entity_placeholders(text, catalog)
    text = _resolve_runtime_tokens(text, catalog)

    _assert_fully_resolved(text, catalog)
    return text


def _assert_fully_resolved(text: str, catalog: RuleCatalog) -> None:
    """Every placeholder must be either resolved or a known runtime token. An
    unresolved token would otherwise reach the model as literal braces."""
    import re

    allowed: set[str] = set()
    if catalog.stage == "phrase_recursive_grounding" and catalog.field_types:
        try:
            parent, children = ConceptTypeEnum(
                catalog.field_types[0]
            ).recursive_grounding_placeholders
        except ValueError:
            parent = children = ""
        allowed = {parent.strip("{}"), children.strip("{}")} - {""}

    unresolved = {
        placeholder
        for placeholder in re.findall(r"{{([^}]+)}}", text)
        if placeholder not in allowed
    }
    if unresolved:
        raise PromptAssemblyError(
            f"{catalog.prompt_name}: unresolved placeholders {sorted(unresolved)}"
        )
