"""Build a stage's wire schema from the catalog that writes its prompt.

``report_when`` is a CARDINALITY specification and always has been: ``always``
means exactly one report, ``when_chosen`` exactly one across the ladder,
``on_violation`` zero or one, ``never`` zero. Until now the wire shape was a flat
``list[AppliedRule]`` that expressed none of that, so every cardinality the
catalog declared had to be re-checked by hand after the fact — and a violation
cost a whole group request. ``check_applied_rules`` was a hand-written type
checker for a type the schema declined to express.

What broke on 2026-08-11 was the cheapest possible instance: one phrase reported
``SCR-1: failed`` and stopped, omitting the SCR-2 and SCR-3 the catalog requires
whichever outcome they reach. One entry out of 597 in that run, on a phrase that
was going to be rejected either way, aborted the manufacturer.

So the catalog generates the schema too. Rules the catalog fixes become required
properties named for their rule ids, which OpenAI strict mode cannot omit; rules
it leaves open stay open. This is decision #12 ("an outcome vocabulary may only
list outcomes the parser accepts") applied to membership instead of to
vocabulary, and enforced by the decoder instead of by catalog-authoring care.

What this makes structurally impossible, and therefore deletes from validation:
a missing always-reported rule, an unknown rule id, a duplicate rule id, an
outcome its kind does not allow, and a ladder that reports none or several
branches. What survives is what is genuinely semantic — the condition chain, and
explanations left blank.

STORAGE IS UNCHANGED. ``flatten_rule_slots`` turns these models back into the
``list[AppliedRule]`` that Mongo, the trail dumps and every downstream reader
already speak. ``AppliedRule`` says wire and stored shapes were collapsed because
nothing justified two types; what justifies them now is that the wire type
carries a guarantee the stored one has no way to express and no need to, being
read rather than decoded. The difference stops at the parse boundary.

SLOTS ARE HOISTED onto the entry rather than nested under an ``applied_rules``
object. Nesting reads better, but grounding would then be root → entry → option →
applied_rules → rule → scalars, which is five object levels against OpenAI's
documented limit of five, before counting the arrays in between. Hoisting costs a
grouping key and buys a level everywhere. ``assert_no_reserved_name_collision``
keeps the two namespaces apart.
"""

from __future__ import annotations

from typing import Any, Literal, Optional, Union, cast

from pydantic import BaseModel, ConfigDict, Field, create_model

from core.models.extraction_schemas.applied_rule import AppliedRule
from core.models.extraction_schemas.grounding import is_sentinel_grounding_label
from core.models.extraction_schemas.response_format_util import (
    build_gpt_response_format,
)
from core.models.rule_catalog import (
    ONLY_REACHABLE_OUTCOME_BY_REPORT_WHEN,
    STAGE_BINARY_CLASSIFICATION,
    STAGE_FREEHAND_GROUNDING,
    STAGE_INITIAL_GROUNDING,
    STAGE_RECURSIVE_GROUNDING,
    STAGE_RELATIONSHIP_SCREENING,
    RuleCatalog,
)

# Wire field names an entry owns, which a rule id must therefore never be. Rule
# ids are hyphenated and upper-case by convention (SCR-1, IGR-Q1, MFG-G2) so a
# collision is not reachable today; asserted anyway, because the failure it would
# cause is a rule silently overwriting the field that carries the phrase.
RESERVED_WIRE_FIELD_NAMES = frozenset(
    {
        "phrase",
        "option",
        "category",
        "options",
        "categories",
        "screenings",
        "groundings",
        "outcome",
        "explanation",
        "identified_entity",
        "confidence",
        "guards",
        "chosen",
    }
)

# The slot that holds the branch a matching ladder took, and the one that holds
# whichever guards fired. Named here because the parser, the schema builder and
# the rendered output example must all agree on them.
CHOSEN_SLOT = "chosen"
GUARDS_SLOT = "guards"


# Generated models get real base classes so that what is STAGE-fixed stays statically
# known while only what is CATALOG-fixed is dynamic. Without them every generated
# model is just `type[BaseModel]`, and each parse site loses `parsed.screenings` /
# `parsed.groundings` / `report.confidence` to the type checker — the fields that are
# the same for every catalog in the stage and were never the dynamic part.
#
# The item types stay `Any`: an entry's rule slots ARE per-catalog, so there is no
# static type for them and pretending otherwise would be worse than admitting it.


class WireEntry(BaseModel):
    """Base for every generated entry model — carries the config they all need.

    ``populate_by_name`` because rule slots are declared under sanitised Python
    names and aliased back to their rule ids, which are not identifiers.
    """

    model_config = ConfigDict(extra="forbid", populate_by_name=True)


class SentinelWireEntry(WireEntry):
    """The escape-hatch arm of a grounding stage: the unit that says nothing was
    identified. It carries NO rule slots.

    A catalog's sentinel branch (RGR-M4, FGR-M2) is reached exactly when there is
    no identified type for the conditions to be about, so the prompt asks for
    every condition as ``not_triggered`` — four reports whose content is fixed by
    the branch itself. Nothing reads them: ``is_sentinel_grounding_label`` skips
    the unit in ``get_deepest_concepts_and_oov``, in initial grounding, in the
    reconcile nodes and in the trail dumps, all downstream of the parse that
    validates it.

    On 2026-08-18 that ceremony cost a manufacturer. One sentinel unit out of 44
    in the run answered RGR-Q1 with RGR-Q3's subject and RGR-Q2 with RGR-Q1's —
    "Q2 satisfied behind Q1 failed" — and the condition-chain check aborted the
    subject over a self-contradiction in a report that is discarded three lines
    later. The other 42 reported the canonical ``not_triggered`` the prompt asks
    for, so this is a rare slip rather than a misread instruction, and a third
    restatement of an instruction the prompt already gives twice would not have
    caught it.

    So the sentinel becomes a BRANCH rather than a unit with fixed content, which
    is decision #21's shape — the same move ``build_screening_response_model``
    made for ``no_candidate``, and for the same reason: a branch cannot be
    half-taken. There is no slot left to contradict. It keeps its
    ``explanation``, which is the only part of the old four reports a reader ever
    wanted.
    """

    explanation: str


class ScreeningWireResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    screenings: list[Any]


class GroundingWireResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    groundings: list[Any]


class BinaryWireReport(WireEntry):
    """Binary classification's stage-fixed fields; its rules are added per catalog.

    ``identified_entity`` is nullable here and not on screening's judged branch:
    there is no second branch to move the null case into, because a whole-text
    classification always has rules to report (locked #25). Declared Optional with
    no default so it stays in ``required``, which strict mode demands.
    """

    identified_entity: Optional[str]
    confidence: int


def _python_name(rule_id: str) -> str:
    """A rule id as a Python attribute. The id itself stays the wire name via an
    alias — ``SCR-1`` is not an identifier, and the stored record keys on the id."""
    return rule_id.replace("-", "_").replace(".", "_")


def assert_no_reserved_name_collision(catalog: RuleCatalog) -> None:
    clashing = sorted(
        rule.id
        for rule in catalog.walk_rules()
        if rule.id in RESERVED_WIRE_FIELD_NAMES
    )
    if clashing:
        raise ValueError(
            f"{catalog.prompt_name}: rule ids {clashing} collide with wire field "
            f"names. Rule slots are hoisted onto the entry object, so a rule id "
            f"may not be one of {sorted(RESERVED_WIRE_FIELD_NAMES)}."
        )


def _outcome_report_model(
    catalog: RuleCatalog, kind: str
) -> type[BaseModel]:
    """``{outcome, explanation}`` for one rule kind, with that kind's vocabulary as
    an enum. The vocabulary is already pinned to what the parser accepts (#12), so
    turning it into a Literal moves that guarantee from parse time to decode time."""
    outcomes = catalog.outcome_vocab[kind]
    return create_model(
        f"{_model_prefix(catalog)}{kind.capitalize()}Report",
        __config__=ConfigDict(extra="forbid"),
        outcome=(Literal[tuple(outcomes)], ...),  # type: ignore[valid-type]
        explanation=(str, ...),
    )


def _fired_report_model(
    catalog: RuleCatalog, *, name: str, rule_ids: list[str]
) -> type[BaseModel]:
    """``{rule_id, explanation}`` for the kinds whose outcome is not a choice.

    A guard is reported only to say it fired and a ladder branch only to say it
    was taken, so ``violated`` and ``chosen`` are the single value each can carry.
    Asking the model to type a constant spends completion tokens on a field that
    has no information in it; ``flatten_rule_slots`` puts the outcome back.
    """
    return create_model(
        name,
        __config__=ConfigDict(extra="forbid"),
        rule_id=(Literal[tuple(rule_ids)], ...),  # type: ignore[valid-type]
        explanation=(str, ...),
    )


def _model_prefix(catalog: RuleCatalog) -> str:
    return "".join(part.capitalize() for part in catalog.prompt_name.split("_"))


def _always_reported(catalog: RuleCatalog) -> list[Any]:
    return [rule for rule in catalog.walk_rules() if rule.report_when == "always"]


def _rules_with_report_when(catalog: RuleCatalog, report_when: str) -> list[Any]:
    return [rule for rule in catalog.walk_rules() if rule.report_when == report_when]


def rule_slot_fields(catalog: RuleCatalog) -> dict[str, tuple[Any, Any]]:
    """The rule-reporting fields to splice into any entry model for ``catalog``.

    One required field per always-reported rule, named for the rule id. A required
    ``chosen`` where the catalog declares a ladder — "exactly one branch" is the
    field's existence rather than a count checked afterwards. An optional-by-
    emptiness ``guards`` list where it declares guards, which is the one place the
    model genuinely chooses membership.
    """
    assert_no_reserved_name_collision(catalog)

    fields: dict[str, tuple[Any, Any]] = {}

    for rule in _always_reported(catalog):
        report_model = _outcome_report_model(catalog, rule.kind)
        fields[_python_name(rule.id)] = (report_model, Field(alias=rule.id))

    chosen_ids = [rule.id for rule in _rules_with_report_when(catalog, "when_chosen")]
    if chosen_ids:
        fields[CHOSEN_SLOT] = (
            _fired_report_model(
                catalog,
                name=f"{_model_prefix(catalog)}ChosenBranch",
                rule_ids=chosen_ids,
            ),
            ...,
        )

    guard_ids = [rule.id for rule in _rules_with_report_when(catalog, "on_violation")]
    if guard_ids:
        fields[GUARDS_SLOT] = (
            list[  # type: ignore[misc]
                _fired_report_model(
                    catalog,
                    name=f"{_model_prefix(catalog)}FiredGuard",
                    rule_ids=guard_ids,
                )
            ],
            ...,
        )

    return fields


def build_entry_model(
    catalog: RuleCatalog,
    *,
    name: str,
    own_fields: dict[str, tuple[Any, Any]],
    base: type[WireEntry] = WireEntry,
) -> type[WireEntry]:
    """An entry model carrying ``own_fields`` plus ``catalog``'s rule slots."""
    # dict[str, Any] rather than the precise dict[str, tuple[Any, Any]]: field names
    # reach `create_model` as **kwargs, and its signature reserves `__doc__`,
    # `__module__`, `__validators__` and `__cls_kwargs__`, so a splat of anything
    # narrower than Any is read as possibly assigning a field definition to one of
    # those. Widening here keeps `rule_slot_fields` honest about what it returns.
    fields: dict[str, Any] = {**own_fields, **rule_slot_fields(catalog)}
    # `__base__` rather than `__config__` — the two are mutually exclusive, and the
    # base carries the config for every generated model.
    return create_model(name, __base__=base, **fields)


def _is_sentinel_unit(entry: BaseModel) -> bool:
    """Whether ``entry`` is the escape hatch, however it arrived.

    The generated arm is the shape the prompt now asks for, and it carries no rule
    slots at all. The label check behind it closes what the schema cannot: the
    rule-bearing arm types its unit key as a plain ``str``, and strict-mode JSON
    Schema has no way to say "any string except this one" — an enum of the real
    options would, but those are per-request (see the option-axis note in
    ``echo_surfaces``). So a unit labelled with the sentinel AND carrying a full
    ladder still satisfies the rule-bearing arm.

    Its rules are dropped rather than validated, which is the same judgement the
    six downstream readers already make by skipping the unit outright: nothing
    reads a sentinel's rules, so a contradiction among them is not a finding and
    must not cost the subject. What the model was asked for is the arm; this is
    only what happens when it answers in the older shape anyway.
    """
    if isinstance(entry, SentinelWireEntry):
        return True
    for unit_key in ("option", "category"):
        label = getattr(entry, unit_key, None)
        if isinstance(label, str) and is_sentinel_grounding_label(label):
            return True
    return False


def flatten_rule_slots(catalog: RuleCatalog, entry: BaseModel) -> list[AppliedRule]:
    """The rule slots on ``entry``, as the stored ``list[AppliedRule]``.

    Emitted in catalog document order, with the chosen branch and any fired guards
    at the position their kind occupies among the sections — the same order the
    rendered output example uses, so stored records read in the order the prompt
    stated the rules.
    """
    if _is_sentinel_unit(entry):
        return []

    applied: list[AppliedRule] = []
    chosen_emitted = False
    guards_emitted = False

    for rule in catalog.walk_rules():
        if rule.report_when == "always":
            report = getattr(entry, _python_name(rule.id))
            applied.append(
                AppliedRule(
                    rule_id=rule.id,
                    outcome=report.outcome,
                    explanation=report.explanation,
                )
            )
        elif rule.report_when == "when_chosen" and not chosen_emitted:
            chosen = getattr(entry, CHOSEN_SLOT, None)
            if chosen is not None:
                applied.append(
                    AppliedRule(
                        rule_id=chosen.rule_id,
                        outcome=ONLY_REACHABLE_OUTCOME_BY_REPORT_WHEN["when_chosen"],
                        explanation=chosen.explanation,
                    )
                )
            chosen_emitted = True
        elif rule.report_when == "on_violation" and not guards_emitted:
            for fired in getattr(entry, GUARDS_SLOT, None) or []:
                applied.append(
                    AppliedRule(
                        rule_id=fired.rule_id,
                        outcome=ONLY_REACHABLE_OUTCOME_BY_REPORT_WHEN["on_violation"],
                        explanation=fired.explanation,
                    )
                )
            guards_emitted = True

    return applied


# ---------------------------------------------------------------------------
# Per-stage response models
# ---------------------------------------------------------------------------

# Which branch a screening entry took. The model picks one and each is complete,
# which is the whole point: "no candidate" used to be spelled by setting
# identified_entity to null AND leaving applied_rules empty — two independent
# channels for one fact, exactly the defect that got the `passed` field removed
# from this schema. On 2026-08-11 they disagreed, and a phrase reported null with
# one rule attached matched neither the shortcut nor a full report.
NO_CANDIDATE = "no_candidate"
JUDGED = "judged"


def build_screening_response_model(catalog: RuleCatalog) -> type[ScreeningWireResponse]:
    """Screening's wire schema: one tagged union per phrase.

    Locked decision #21 stands — a phrase that identified nothing still reports no
    conditions, because there was no candidate for them to be about. What changes
    is that the shortcut is now a BRANCH rather than the absence of content, so it
    cannot be half-taken. It also gains an ``explanation``: 45% of entries in the
    run that prompted this rejected with no recorded reason at all, and the report
    that broke the parser was the model trying to volunteer one.

    ``identified_entity`` is non-null on the judged branch by construction. A
    rejected phrase still names the candidate it got furthest with, so null was
    only ever the no-candidate case — which now has its own branch and no slot for
    it. That deletes the "passed but named no identified_entity" check too.
    """
    prefix = _model_prefix(catalog)

    no_candidate = create_model(
        f"{prefix}NoCandidateEntry",
        __base__=WireEntry,
        outcome=(Literal[NO_CANDIDATE], ...),
        phrase=(str, ...),
        explanation=(str, ...),
    )
    judged = build_entry_model(
        catalog,
        name=f"{prefix}JudgedEntry",
        own_fields={
            "outcome": (Literal[JUDGED], ...),
            "phrase": (str, ...),
            "identified_entity": (str, ...),
        },
    )
    return create_model(
        f"{prefix}Response",
        __base__=ScreeningWireResponse,
        screenings=(list[Union[no_candidate, judged]], ...),  # type: ignore[valid-type]
    )


def _sentinel_arm(
    catalog: RuleCatalog, *, name: str, unit_key: str
) -> Optional[type[SentinelWireEntry]]:
    """``catalog``'s escape-hatch arm, or None where it declares no sentinel.

    The unit key is pinned to the sentinel label itself, which is what keeps the
    two arms disjoint without a tag field: ``extra="forbid"`` on both means a
    sentinel unit cannot satisfy the rule-bearing arm (its required rule slots are
    absent) and a rule-bearing unit cannot satisfy this one (its rule slots are
    extra), whatever label it carries.
    """
    if catalog.sentinel_tag is None:
        return None
    return create_model(
        name,
        __base__=SentinelWireEntry,
        **{unit_key: (Literal[catalog.sentinel_tag], ...)},  # type: ignore[call-overload]
    )


def _units_field(
    unit: type[WireEntry], sentinel: Optional[type[SentinelWireEntry]]
) -> Any:
    """The per-phrase list of units, widened to the escape-hatch arm where the
    catalog has one. Sentinel first: it is the narrower of the two."""
    if sentinel is None:
        return list[unit]  # type: ignore[valid-type]
    return list[Union[sentinel, unit]]  # type: ignore[valid-type]


def build_category_grounding_response_model(
    catalog: RuleCatalog,
) -> type[GroundingWireResponse]:
    """Freehand grounding: the model names the category itself."""
    prefix = _model_prefix(catalog)
    category = build_entry_model(
        catalog,
        name=f"{prefix}Category",
        own_fields={"category": (str, ...)},
    )
    sentinel = _sentinel_arm(
        catalog, name=f"{prefix}SentinelCategory", unit_key="category"
    )
    entry = create_model(
        f"{prefix}Entry",
        __base__=WireEntry,
        phrase=(str, ...),
        categories=(_units_field(category, sentinel), ...),
    )
    return create_model(
        f"{prefix}Response",
        __base__=GroundingWireResponse,
        groundings=(list[entry], ...),  # type: ignore[valid-type]
    )


def build_option_grounding_response_model(
    catalog: RuleCatalog,
) -> type[GroundingWireResponse]:
    """Initial and recursive grounding: the model chooses from supplied options."""
    prefix = _model_prefix(catalog)
    option = build_entry_model(
        catalog,
        name=f"{prefix}Option",
        own_fields={"option": (str, ...)},
    )
    sentinel = _sentinel_arm(
        catalog, name=f"{prefix}SentinelOption", unit_key="option"
    )
    entry = create_model(
        f"{prefix}Entry",
        __base__=WireEntry,
        phrase=(str, ...),
        options=(_units_field(option, sentinel), ...),
    )
    return create_model(
        f"{prefix}Response",
        __base__=GroundingWireResponse,
        groundings=(list[entry], ...),  # type: ignore[valid-type]
    )


def build_binary_classification_response_model(
    catalog: RuleCatalog,
) -> type[BinaryWireReport]:
    """Binary classification: one unit per request, so the report IS the response.

    Its two stage-fixed fields live on ``BinaryWireReport``; only the rule slots
    are added here.
    """
    return build_entry_model(
        catalog,
        name=f"{_model_prefix(catalog)}Report",
        own_fields={},
        base=BinaryWireReport,
    )


_RESPONSE_MODEL_BUILDER_BY_STAGE = {
    STAGE_RELATIONSHIP_SCREENING: build_screening_response_model,
    STAGE_INITIAL_GROUNDING: build_option_grounding_response_model,
    STAGE_RECURSIVE_GROUNDING: build_option_grounding_response_model,
    STAGE_FREEHAND_GROUNDING: build_category_grounding_response_model,
    STAGE_BINARY_CLASSIFICATION: build_binary_classification_response_model,
}

# Keyed by catalog identity, not by stage: rule ids and outcome vocabularies differ
# per catalog, and two freehand catalogs already differ in whether they declare a
# `quality` kind at all. Versioned so an edited catalog yields a new model rather
# than a stale cached one — which is also why `catalog_version` is enough to
# version the schema: the schema is a pure function of the catalog and of nothing
# else. Anything that makes that untrue needs its own digest in the prompt pin.
_RESPONSE_MODEL_CACHE: dict[tuple[str, str], type[BaseModel]] = {}


def response_model_for(catalog: RuleCatalog) -> type[BaseModel]:
    """The wire model for ``catalog``'s stage, built once per catalog version."""
    key = (catalog.prompt_name, catalog.catalog_version)
    cached = _RESPONSE_MODEL_CACHE.get(key)
    if cached is not None:
        return cached

    builder = _RESPONSE_MODEL_BUILDER_BY_STAGE.get(catalog.stage)
    if builder is None:
        raise ValueError(
            f"{catalog.prompt_name}: stage {catalog.stage!r} has no wire schema "
            f"builder. Known stages: {sorted(_RESPONSE_MODEL_BUILDER_BY_STAGE)}"
        )

    model = builder(catalog)
    _RESPONSE_MODEL_CACHE[key] = model
    return model


def response_format_for(catalog: RuleCatalog) -> dict:
    """``catalog``'s wire model as an OpenAI strict ``response_format`` dict."""
    return build_gpt_response_format(
        response_model_for(catalog),
        name=f"{catalog.prompt_name}_result",
    )


# Stage-typed views of the same cached model, for the parse sites. `response_model_for`
# dispatches on a stage it only knows at run time, so its return type can be no
# narrower than BaseModel — which would cost every parser static knowledge of the
# container field it immediately reads. Each of these asserts the stage the caller
# already established by looking the catalog up under it.


def screening_response_model(catalog: RuleCatalog) -> type[ScreeningWireResponse]:
    return cast(type[ScreeningWireResponse], response_model_for(catalog))


def grounding_response_model(catalog: RuleCatalog) -> type[GroundingWireResponse]:
    return cast(type[GroundingWireResponse], response_model_for(catalog))


def binary_classification_response_model(
    catalog: RuleCatalog,
) -> type[BinaryWireReport]:
    return cast(type[BinaryWireReport], response_model_for(catalog))
