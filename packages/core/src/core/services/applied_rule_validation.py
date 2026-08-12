"""Check a model's reported rules against the catalog that asked for them.

Every failure here raises. Callers sit inside the parse path, so a raise routes to
``record_response_parse_error`` and fails that one group request, which can then be
re-run on its own.

What is checked is the REPORT's internal consistency against the catalog: that
every rule id is known and reportable, that each is reported once, that its
outcome is one the catalog allows for its kind, that guards and preferences are
reported only under the conditions the catalog names, that every
always-reported rule is present, that exactly one branch of a matching ladder is
chosen, and that a chain of conditions does not claim to hold behind one that did
not.

What is NOT checked is the ``explanation`` prose. Until 2026-08-11 each rule also
carried a structured ``evidence`` array of quoted slices, and roughly half of this
module was a matcher that located each slice in its declared source, in order,
under a normalising ladder. That went with the field. The prompts still ask for
the phrase and its relationship summary to be cited and for the relied-on words to
be quoted, but a paraphrase is now a weakness a reader can see rather than a parse
failure that costs a whole group request.
"""

from __future__ import annotations

from typing import NamedTuple, Optional

from core.models.extraction_schemas.applied_rule import AppliedRule
from core.models.rule_catalog import RuleCatalog


class AppliedRuleValidationError(ValueError):
    """A reported rule contradicts the catalog that asked for it."""


def _condition_chain_problems(
    catalog: RuleCatalog, applied_rules: list[AppliedRule], where: str
) -> list[str]:
    """Every condition reported ``satisfied`` behind one that did not hold.

    Screening's conditions are a CHAIN, not a flat list: each one's subject is what
    the previous one established. SCR-2 speaks of "the identified {entity}", which
    exists only if SCR-1 held; SCR-3 asks which party holds the involvement that
    SCR-2 established. So "SCR-2 failed, SCR-3 satisfied" says no one was shown to
    serve the industry AND that it was the manufacturer who served it. The verdict
    comes out right either way — the failed condition already rejects it — but the
    record is self-contradictory, and rule-level credit assignment reads the record.

    Document order within an ``all`` section IS the chain; nothing else declares it.
    That leaves this enforcing dependency on the grounding sections too, where the
    qualification conditions are closer to a flat conjunction. It costs nothing
    there: a tag whose qualification failed is dropped rather than reported, so no
    legitimate report has a non-satisfied qualification condition at all. Revisit if
    grounding ever starts retaining rejected tags, because then "Q1 failed but the
    option still matched" becomes a diagnosis worth recording.
    """
    outcomes = {applied.rule_id: applied.outcome for applied in applied_rules}
    problems: list[str] = []

    for section in catalog.sections:
        if section.combinator != "all":
            continue

        blocked_by: Optional[tuple[str, str]] = None
        for rule in (r for top in section.rules for r in top.walk()):
            if rule.kind != "condition":
                continue
            outcome = outcomes.get(rule.id)
            if outcome is None:
                continue
            if outcome == "satisfied":
                if blocked_by is not None:
                    blocker, blocker_outcome = blocked_by
                    problems.append(
                        f"{catalog.prompt_name}: {where}: reported {rule.id!r} as "
                        f"'satisfied', but {blocker!r} earlier in "
                        f"{section.section_id!r} was {blocker_outcome!r}. "
                        f"{rule.id!r} builds on what {blocker!r} establishes, so it "
                        f"cannot hold when {blocker!r} did not — report it as "
                        f"'not_triggered'."
                    )
            elif blocked_by is None:
                blocked_by = (rule.id, outcome)

    return problems


def passed_implied_by(
    catalog: RuleCatalog, applied_rules: list[AppliedRule]
) -> bool:
    """Whether the reported rules add up to an accept.

    This IS the verdict — screening does not ask the model for one. The rules are
    the decision procedure the catalog defines, so reading the answer off them is
    the only way the record and the verdict cannot disagree.

    The two ways a unit can be rejected, and nothing else: a condition that did not
    hold, or a guard that fired. ``not_triggered`` is not ``satisfied`` — a
    condition with nothing to evaluate did not hold, so it blocks just as a
    ``failed`` one does. Guards appear only when violated, so the mere presence of
    one is the rejection.

    An empty list rejects rather than vacuously accepting. Validation requires every
    always-reported rule to be present, so the only report that reaches here with no
    rules is the null-entity shortcut in the screening parser — a phrase that offered
    no candidate at all, which is the plainest reject there is.
    """
    if not applied_rules:
        return False

    rules_by_id = catalog.rules_by_id()
    for applied in applied_rules:
        kind = rules_by_id[applied.rule_id].kind
        if kind == "condition" and applied.outcome != "satisfied":
            return False
        if kind == "guard":
            return False
    return True


class AppliedRuleReport(NamedTuple):
    """The outcome of checking one unit's rules.

    ``problems`` empty means the report satisfies its catalog. The rules themselves
    are the caller's own ``applied_rules`` — there is nothing to hand back, since
    checking no longer transforms them.
    """

    problems: list[str]


def check_applied_rules(
    *,
    catalog: RuleCatalog,
    applied_rules: list[AppliedRule],
    where: str,
) -> AppliedRuleReport:
    """Every way ``applied_rules`` fails ``catalog``, not merely the first.

    Checks the report's internal consistency only. There is no verdict to check it
    against: screening derives ``passed`` from these rules via ``passed_implied_by``
    rather than asking the model to declare one, and grounding drops a category
    instead of flagging it.

    Exhaustive because the caller is measuring, not just gating. Failing at the
    first problem makes the observed defect rate a censored sample — the run that
    prompted this reported one bad rule out of a fifteen-phrase request and never
    looked at the thirteen phrases after it, so "one defect per request" was an
    artefact of where the scan stopped. A whole group request is already lost by
    the time anything here fires, so there is nothing to gain by stopping early
    and a rate to lose.

    :param where: identifies the unit being checked in error messages, e.g.
        ``"phrase 'we serve aerospace' category 'Aerospace'"``.
    """
    rules_by_id = catalog.rules_by_id()
    problems: list[str] = []

    seen: set[str] = set()
    for applied in applied_rules:
        rule = rules_by_id.get(applied.rule_id)
        if rule is None:
            # Everything below reads `rule`, and an id the catalog does not know
            # has no kind, vocabulary or reporting policy to check against.
            problems.append(
                f"{catalog.prompt_name}: {where}: reported unknown rule "
                f"{applied.rule_id!r}. Known rules: {sorted(rules_by_id)}"
            )
            continue
        if not rule.reportable:
            problems.append(
                f"{catalog.prompt_name}: {where}: reported {applied.rule_id!r}, "
                f"which is guidance folded into its parent rule and must not be "
                f"reported"
            )
        if applied.rule_id in seen:
            problems.append(
                f"{catalog.prompt_name}: {where}: reported {applied.rule_id!r} more "
                f"than once"
            )
        seen.add(applied.rule_id)

        allowed = catalog.valid_outcomes(applied.rule_id)
        if applied.outcome not in allowed:
            problems.append(
                f"{catalog.prompt_name}: {where}: rule {applied.rule_id!r} is a "
                f"{rule.kind} and cannot have outcome {applied.outcome!r}. "
                f"Allowed: {sorted(allowed)}"
            )

        # A guard is reported only to say it fired and a preference only to say
        # it was the branch taken — and the outcome check above ALREADY rejects
        # anything else, because
        # ONLY_REACHABLE_OUTCOME_BY_REPORT_WHEN pins those kinds' vocabulary to
        # the single value they can carry. Separate checks stood here until
        # 2026-08-11 and could not fire without the vocabulary check firing on
        # the same rule, so one defect was counted twice. That is a real cost
        # rather than noise: this function is exhaustive precisely so the problem
        # count measures how much of a response was wrong, and a preference
        # reported with the wrong outcome inflated that count by one every time.

        # An explanation is the whole of the justification now that no structured
        # evidence rides alongside it, so a blank one is a rule reported with no
        # reason at all. Its CONTENT is not checked — the prompt asks it to cite
        # and quote its sources, and a paraphrase that ignores that is for a
        # reader to weigh, not for this to reject.
        if not applied.explanation.strip():
            problems.append(
                f"{catalog.prompt_name}: {where}: rule {applied.rule_id!r} was "
                f"reported with an empty explanation"
            )

    missing = catalog.always_reported_rule_ids() - seen
    if missing:
        problems.append(
            f"{catalog.prompt_name}: {where}: did not report {sorted(missing)}, "
            f"which must be reported whichever outcome they reach"
        )

    # The matching ladder is one decision, so exactly one branch is reported.
    # Stages without a ladder (screening) declare no preference rules and skip this.
    preference_ids = {
        rule.id for rule in catalog.walk_rules() if rule.report_when == "when_chosen"
    }
    if preference_ids:
        chosen = seen & preference_ids
        if len(chosen) != 1:
            problems.append(
                f"{catalog.prompt_name}: {where}: expected exactly one chosen rule "
                f"from {sorted(preference_ids)}, got {sorted(chosen) or 'none'}"
            )

    problems.extend(_condition_chain_problems(catalog, applied_rules, where))
    return AppliedRuleReport(problems=problems)


def raise_for_violations(problems: list[str]) -> None:
    """Turn collected violations into the one error that fails the request.

    A single problem keeps its message verbatim so the common case reads exactly
    as it did when validation raised in place. Several are numbered under a count,
    because the count is the measurement: it says how much of the response was
    wrong, which one message from an aborted scan never could.
    """
    if not problems:
        return
    if len(problems) == 1:
        raise AppliedRuleValidationError(problems[0])

    numbered = "\n".join(
        f"  {index}. {problem}" for index, problem in enumerate(problems, start=1)
    )
    raise AppliedRuleValidationError(
        f"{len(problems)} rule-report violations:\n{numbered}"
    )


def validate_applied_rules(
    *,
    catalog: RuleCatalog,
    applied_rules: list[AppliedRule],
    where: str,
) -> None:
    """Raise ``AppliedRuleValidationError`` if ``applied_rules`` does not satisfy
    ``catalog``.

    One unit's worth. A parser walking many units should collect across all of
    them with ``check_applied_rules`` and call ``raise_for_violations`` once at the
    end, so the error covers the whole response rather than stopping at the first
    unit that failed.
    """
    report = check_applied_rules(
        catalog=catalog,
        applied_rules=applied_rules,
        where=where,
    )
    raise_for_violations(report.problems)
