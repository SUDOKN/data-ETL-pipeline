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
from core.models.extraction_schemas.response_format_util import (
    build_gpt_response_format,
)
from core.models.rule_catalog import (
    ONLY_REACHABLE_OUTCOME_BY_REPORT_WHEN,
    STAGE_BINARY_CLASSIFICATION,
    STAGE_DESCENT,
    STAGE_FREEHAND_GROUNDING,
    STAGE_GROUNDING,
    STAGE_INITIAL_GROUNDING,
    STAGE_OOV_GROUNDING,
    STAGE_PROPOSAL,
    STAGE_RECURSIVE_GROUNDING,
    STAGE_RELATIONSHIP_SCREENING,
    STAGE_UNIT_SCREENING,
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
        # v2 (record-keyed) wire names.
        "record_id",
        "candidate",
        "candidates",
        # Step 2 (structural) wire names.
        "matched",
        "proposed",
        "unmatched",
        "records",
        "quote",
        "label",
        "accepted",
        "not_accepted",
        "evidence",
        "failed_rule",
        "match",
        "proposals",
        "subject",
    }
)

# The evidence-distance label a unit-screening unit attaches to each accepted
# record (design draft §5.2, user decision 2026-09-20): "named" when the
# record's own words name the candidate, "inferred" when the candidate is one
# plain step from what the record names. Metadata, never a gate, until the
# census has calibrated it against the judges.
EVIDENCE_DISTANCE_NAMED = "named"
EVIDENCE_DISTANCE_INFERRED = "inferred"
EVIDENCE_DISTANCES = (EVIDENCE_DISTANCE_NAMED, EVIDENCE_DISTANCE_INFERRED)

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


class ScreeningWireResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    screenings: list[Any]


class GroundingWireResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    groundings: list[Any]


class RecordGroundingStructuralWireResponse(BaseModel):
    """Step 2 grounding, descent and the proposal pass (user decision 2026-09-21:
    record-major, never option-major): one entry per record carrying the option
    or options its subject matches, each with the quote and the matching rule,
    any proposal, and a required-nullable declination explanation that is
    non-null exactly when both lists are empty. Units for screening and descent
    are grouped from these entries in code."""

    model_config = ConfigDict(extra="forbid")

    groundings: list[Any]


class UnitScreeningWireResponse(BaseModel):
    """Step 2 screening: one entry per unit (a candidate label with the records
    read as evidencing it), each record accepted with its evidence distance or
    not accepted with the first rule that failed it."""

    model_config = ConfigDict(extra="forbid")

    screenings: list[Any]


class ProposalEntry(BaseModel):
    """A name the model gives a thing the vocabulary lacks, with the words of
    the record that evidence it and why no option covers it."""

    model_config = ConfigDict(extra="forbid")

    label: str
    quote: str
    explanation: str


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


def flatten_rule_slots(catalog: RuleCatalog, entry: BaseModel) -> list[AppliedRule]:
    """The rule slots on ``entry``, as the stored ``list[AppliedRule]``.

    Emitted in catalog document order, with the chosen branch and any fired guards
    at the position their kind occupies among the sections — the same order the
    rendered output example uses, so stored records read in the order the prompt
    stated the rules.
    """
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


def build_binary_classification_response_model(
    catalog: RuleCatalog,
) -> type[BinaryWireReport]:
    """Binary classification: one unit per request, so the report IS the response.

    Its two stage-fixed fields live on ``BinaryWireReport``; only the rule slots
    are added here.
    """
    return cast(
        type[BinaryWireReport],
        build_entry_model(
            catalog,
            name=f"{_model_prefix(catalog)}Report",
            own_fields={},
            base=BinaryWireReport,
        ),
    )


# The record-keyed (v2) builders below serve every phrase stage; binary
# classification keeps its whole-text report shape.
_RESPONSE_MODEL_BUILDER_BY_STAGE = {
    STAGE_RELATIONSHIP_SCREENING: lambda catalog: build_screening_response_model_v2(
        catalog
    ),
    STAGE_INITIAL_GROUNDING: lambda catalog: _build_option_grounding_v2(catalog),
    STAGE_RECURSIVE_GROUNDING: lambda catalog: _build_option_grounding_v2(catalog),
    STAGE_OOV_GROUNDING: lambda catalog: _build_candidate_grounding_v2(catalog),
    STAGE_FREEHAND_GROUNDING: lambda catalog: _build_candidate_grounding_v2(catalog),
    STAGE_BINARY_CLASSIFICATION: build_binary_classification_response_model,
    # Step 2: the structural families.
    STAGE_GROUNDING: lambda catalog: build_record_grounding_structural_response_model(catalog),
    STAGE_DESCENT: lambda catalog: build_record_grounding_structural_response_model(catalog),
    STAGE_PROPOSAL: lambda catalog: build_record_grounding_structural_response_model(catalog),
    STAGE_UNIT_SCREENING: lambda catalog: build_unit_screening_response_model(catalog),
}

# The stages whose catalogs must declare ``reporting: structural`` — their wire
# carries no rule slots — against the ones that must not.
STRUCTURAL_STAGES = frozenset(
    {STAGE_GROUNDING, STAGE_DESCENT, STAGE_PROPOSAL, STAGE_UNIT_SCREENING}
)

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

    expected = "structural" if catalog.stage in STRUCTURAL_STAGES else "per_rule"
    if catalog.reporting != expected:
        raise ValueError(
            f"{catalog.prompt_name}: stage {catalog.stage!r} reports {expected}, "
            f"but the catalog declares reporting={catalog.reporting!r}"
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


def record_grounding_structural_response_model(
    catalog: RuleCatalog,
) -> type[RecordGroundingStructuralWireResponse]:
    return cast(type[RecordGroundingStructuralWireResponse], response_model_for(catalog))


def unit_screening_response_model(
    catalog: RuleCatalog,
) -> type[UnitScreeningWireResponse]:
    return cast(type[UnitScreeningWireResponse], response_model_for(catalog))


# ---------------------------------------------------------------------------
# Record-keyed response models (pipeline v2)
# ---------------------------------------------------------------------------
#
# Every phrase-stage entry is keyed by the masked record_id instead of the
# phrase, and the declination is structural: an empty unit list plus a
# required-nullable entry-level ``explanation`` replaced the sentinel arm.
# Screening is per-candidate — candidates are SUPPLIED by grounding, so the old
# no_candidate branch and ``identified_entity`` have no counterpart.


def build_screening_response_model_v2(
    catalog: RuleCatalog,
) -> type[ScreeningWireResponse]:
    """v2 screening: one entry per record, one judged unit per supplied candidate.

    No union: a record with no candidates is never sent, and every sent
    candidate is judged — the hold enforces both axes at parse time, since
    strict mode cannot pin a response to the request's own record and candidate
    sets.
    """
    prefix = _model_prefix(catalog)
    candidate = build_entry_model(
        catalog,
        name=f"{prefix}JudgedCandidate",
        own_fields={"candidate": (str, ...)},
    )
    entry = create_model(
        f"{prefix}RecordEntry",
        __base__=WireEntry,
        record_id=(str, ...),
        candidates=(list[candidate], ...),  # type: ignore[valid-type]
    )
    return create_model(
        f"{prefix}Response",
        __base__=ScreeningWireResponse,
        screenings=(list[entry], ...),  # type: ignore[valid-type]
    )


def build_record_grounding_response_model(
    catalog: RuleCatalog, *, unit_key: str, units_key: str
) -> type[GroundingWireResponse]:
    """v2 grounding (in-vocab, recursive, OOV, freehand): record-keyed entries.

    ``explanation`` is required-nullable at the entry level and is meaningful
    exactly when the unit list is empty — the structural declination that
    replaced the sentinel. Strict mode cannot express that correlation, so the
    parse side enforces it; the schema's contribution is that the key cannot be
    omitted, which is what lets the empty case never be silent.
    """
    prefix = _model_prefix(catalog)
    unit = build_entry_model(
        catalog,
        name=f"{prefix}{unit_key.capitalize()}Unit",
        own_fields={unit_key: (str, ...)},
    )
    entry = create_model(
        f"{prefix}RecordEntry",
        __base__=WireEntry,
        record_id=(str, ...),
        **{units_key: (list[unit], ...)},  # type: ignore[arg-type]
        explanation=(Optional[str], ...),
    )
    return create_model(
        f"{prefix}Response",
        __base__=GroundingWireResponse,
        groundings=(list[entry], ...),  # type: ignore[valid-type]
    )


def _build_option_grounding_v2(catalog: RuleCatalog) -> type[GroundingWireResponse]:
    return build_record_grounding_response_model(
        catalog, unit_key="option", units_key="options"
    )


def _build_candidate_grounding_v2(catalog: RuleCatalog) -> type[GroundingWireResponse]:
    return build_record_grounding_response_model(
        catalog, unit_key="candidate", units_key="candidates"
    )


# ---------------------------------------------------------------------------
# Structural response models (Step 2 of the grounding redesign, 2026-09-21)
# ---------------------------------------------------------------------------
#
# No rule slots. A rule is held by where an entry lands and what it quotes: an
# option under a record must be a vocabulary label (membership) and carry the
# words of that record that evidence it (the evidence rule); a proposal is the
# ladder's escape hatch by being in ``proposals`` (the ``proposal`` kind); a
# unit's ``not_accepted`` record names the first condition or guard that failed
# it. What the schema fixes per catalog is only the two id vocabularies: the
# matching branches a ``match`` may name, and the rules a ``failed_rule`` may.


def matching_branch_ids(catalog: RuleCatalog) -> list[str]:
    """The ladder branches an option may report as its ``match``: every
    ``preference`` rule. The ``proposal`` kind is excluded on purpose — it is
    reported by the ``proposals`` list, never by id."""
    return [rule.id for rule in catalog.walk_rules() if rule.kind == "preference"]


def failable_rule_ids(catalog: RuleCatalog) -> list[str]:
    """The rules a not-accepted record may name: every condition and guard, in
    document order — which is also the order the prompt asks them to be tried."""
    return [
        rule.id for rule in catalog.walk_rules() if rule.kind in ("condition", "guard")
    ]


def build_record_grounding_structural_response_model(
    catalog: RuleCatalog,
) -> type[RecordGroundingStructuralWireResponse]:
    """Grounding, descent and the proposal pass: ``groundings[{record_id,
    options[{option, quote, match}], proposals[{label, quote, explanation}],
    explanation}]``. ``explanation`` is required-nullable and meaningful exactly
    when both lists are empty (the structural declination, as in the v2 wire);
    strict mode cannot express that correlation, so the parser holds it."""
    prefix = _model_prefix(catalog)
    branches = matching_branch_ids(catalog)
    if not branches:
        raise ValueError(
            f"{catalog.prompt_name}: a record-major grounding catalog needs at least "
            f"one preference rule for an option to report as its match"
        )
    option = create_model(
        f"{prefix}OptionEntry",
        __config__=ConfigDict(extra="forbid"),
        option=(str, ...),
        quote=(str, ...),
        match=(Literal[tuple(branches)], ...),  # type: ignore[valid-type]
    )
    entry = create_model(
        f"{prefix}RecordEntry",
        __base__=WireEntry,
        record_id=(str, ...),
        options=(list[option], ...),  # type: ignore[valid-type]
        proposals=(list[ProposalEntry], ...),
        explanation=(Optional[str], ...),
    )
    return create_model(
        f"{prefix}Response",
        __base__=RecordGroundingStructuralWireResponse,
        groundings=(list[entry], ...),  # type: ignore[valid-type]
    )


def build_unit_screening_response_model(
    catalog: RuleCatalog,
) -> type[UnitScreeningWireResponse]:
    """Unit-major screening: ``screenings[{option, accepted[{record_id,
    evidence, quote}], not_accepted[{record_id, failed_rule, quote}]}]``."""
    prefix = _model_prefix(catalog)
    failable = failable_rule_ids(catalog)
    if not failable:
        raise ValueError(
            f"{catalog.prompt_name}: a unit-screening catalog needs at least one "
            f"condition or guard for a not-accepted record to name"
        )
    accepted = create_model(
        f"{prefix}AcceptedRecord",
        __config__=ConfigDict(extra="forbid"),
        record_id=(str, ...),
        evidence=(Literal[EVIDENCE_DISTANCES], ...),  # type: ignore[valid-type]
        quote=(str, ...),
    )
    not_accepted = create_model(
        f"{prefix}NotAcceptedRecord",
        __config__=ConfigDict(extra="forbid"),
        record_id=(str, ...),
        failed_rule=(Literal[tuple(failable)], ...),  # type: ignore[valid-type]
        quote=(str, ...),
    )
    unit = create_model(
        f"{prefix}Unit",
        __config__=ConfigDict(extra="forbid"),
        option=(str, ...),
        accepted=(list[accepted], ...),  # type: ignore[valid-type]
        not_accepted=(list[not_accepted], ...),  # type: ignore[valid-type]
    )
    return create_model(
        f"{prefix}Response",
        __base__=UnitScreeningWireResponse,
        screenings=(list[unit], ...),  # type: ignore[valid-type]
    )
