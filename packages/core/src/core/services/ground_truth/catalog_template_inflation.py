"""Inflate ground-truth templates from the pinned catalog, never from the report.

The wire reports rules FLAT and every catalog child is (today) a never-reported
note — the tree lives only in the catalog. So the annotation template is built
by walking the catalog's sections and hydrating each rule slot from the flat
report (PH-1): reported rows carry their outcome and explanation; silence
materializes as ``reported=False`` so it is auditable (settled semantics #4).
Future reportable children flow through automatically, since the wire schema is
generated from the same catalog.

Silence is synthesized differently per ``report_when``, because it means
different things:

- ``on_violation`` silence IS a verdict — the guard did not fire — so it gets
  the implied outcome ``not_violated``.
- ``when_chosen`` silence means another branch won; there is nothing to imply,
  so the node is pure context (``outcome=None``).
- ``never`` (notes) are always context.
- ``always`` silence is impossible in a validated report; with a non-empty
  report it raises, and an entirely empty report is screening's no-candidate
  branch, where the full silent skeleton is exactly the auditable record.
"""

from __future__ import annotations

import logging
from typing import Optional

from core.models.extraction_results.llm_phrase_extraction_results import (
    LLMPhraseExtractionStats,
)
from core.models.extraction_schemas.applied_rule import AppliedRule
from core.models.extraction_schemas.grounding import (
    PhraseToTagAndRulesMap,
    TagToAppliedRulesMap,
    is_sentinel_grounding_label,
)
from core.models.extraction_schemas.iterative_tagging import (
    IterativeGroundingResult,
    IterativelyTaggedPhrase,
)
from core.models.extraction_schemas.screening import ScreeningVerdict
from core.models.ground_truth.rule_tree import AuditedRule, AuditedSection
from core.models.ground_truth.stage_blocks import (
    ChunkGT,
    ExtractedPhraseGT,
    InVocabGroundingGT,
    InVocabNodeGT,
    OovGroundingGT,
    RelationshipGT,
    ScreeningGT,
    ScreeningLLMCopy,
    TagGroundingGT,
)
from core.models.rule_catalog import (
    ONLY_REACHABLE_OUTCOME_BY_REPORT_WHEN,
    RuleCatalog,
    RuleNode,
)
from core.services.pipeline_nodes.multi_stage.llm_recursive_grounding_service import (
    get_phrase_trails,
)

logger = logging.getLogger(__name__)

# The implied outcome of a guard the report did not mention: it did not fire.
# Not part of any catalog's outcome_vocab — those pin what the WIRE may say,
# and the wire never says this; only the template synthesizer does.
UNREPORTED_GUARD_OUTCOME = "not_violated"


class InflationError(ValueError):
    """A report that cannot have come from this catalog, or a corrupted one."""


def _dedupe_reports(
    catalog: RuleCatalog, rows: list[AppliedRule]
) -> dict[str, AppliedRule]:
    known = catalog.rules_by_id()
    reports: dict[str, AppliedRule] = {}
    for row in rows:
        if row.rule_id not in known:
            raise InflationError(
                f"{catalog.prompt_name}: reported rule {row.rule_id!r} does not "
                f"exist at catalog_version {catalog.catalog_version!r}"
            )
        seen = reports.get(row.rule_id)
        if seen is not None and seen != row:
            raise InflationError(
                f"{catalog.prompt_name}: rule {row.rule_id!r} reported twice "
                f"with conflicting content — refusing to pick a winner"
            )
        reports[row.rule_id] = row
    return reports


def _inflate_rule(
    catalog: RuleCatalog,
    rule: RuleNode,
    reports: dict[str, AppliedRule],
    *,
    report_is_empty: bool,
) -> AuditedRule:
    sub_rules = [
        _inflate_rule(catalog, child, reports, report_is_empty=report_is_empty)
        for child in rule.children
    ]
    report = reports.get(rule.id)
    if report is not None:
        valid = catalog.valid_outcomes(rule.id)
        if report.outcome not in valid:
            raise InflationError(
                f"{catalog.prompt_name}: rule {rule.id!r} reported outcome "
                f"{report.outcome!r}, not one of {sorted(valid)}"
            )
        return AuditedRule(
            rule_id=rule.id,
            kind=rule.kind,
            outcome=report.outcome,
            explanation=report.explanation,
            reported=True,
            sub_rules=sub_rules,
        )

    if rule.report_when == "always" and not report_is_empty:
        raise InflationError(
            f"{catalog.prompt_name}: always-reported rule {rule.id!r} is missing "
            f"from a non-empty report — the wire schema cannot emit this"
        )
    implied = (
        UNREPORTED_GUARD_OUTCOME if rule.report_when == "on_violation" else None
    )
    return AuditedRule(
        rule_id=rule.id,
        kind=rule.kind,
        outcome=implied,
        explanation=None,
        reported=False,
        sub_rules=sub_rules,
    )


def inflate_applied_rules(
    catalog: RuleCatalog,
    rows: list[AppliedRule],
    *,
    synthesize_all_on_empty: bool = False,
) -> list[AuditedSection]:
    """``rows`` slotted into ``catalog``'s section tree, silence synthesized.

    Zero rows yields no sections unless ``synthesize_all_on_empty`` — the full
    silent skeleton is meaningful where an empty report is itself a verdict
    (screening's no-candidate branch), and noise where it is structural (a
    sentinel descent node).
    """
    if not rows and not synthesize_all_on_empty:
        return []
    reports = _dedupe_reports(catalog, rows)
    return [
        AuditedSection(
            section_id=section.section_id,
            combinator=section.combinator,
            applied_rules=[
                _inflate_rule(
                    catalog, rule, reports, report_is_empty=not reports
                )
                for rule in section.rules
            ],
        )
        for section in catalog.sections
    ]


def inflate_screening_llm_copy(
    catalog: RuleCatalog, verdict: ScreeningVerdict
) -> ScreeningLLMCopy:
    """The stored verdict as the audit template's immutable LLM copy.

    ``ScreeningLLMCopy`` re-derives ``passed`` from the inflated sections at
    construction, so this is also the integrity check: a stored verdict whose
    ``passed`` disagrees with its own rules cannot inflate.
    """
    return ScreeningLLMCopy(
        passed=verdict.passed,
        identified_entity=verdict.identified_entity,
        no_candidate_explanation=verdict.no_candidate_explanation,
        sections=inflate_applied_rules(
            catalog, verdict.applied_rules, synthesize_all_on_empty=True
        ),
    )


def split_sentinel_tag_rules(
    tag_rules: TagToAppliedRulesMap,
) -> tuple[TagToAppliedRulesMap, bool]:
    """The stored tag map with sentinel keys split out as a declination flag.

    "None of the above" / "Cannot categorize" is the grounding stage's escape
    hatch: the model examined the phrase and declined every label. The stats
    record that answer faithfully as a sentinel key with no rules — a process
    record, not a grounding link — so it must not inflate into a
    ``TagGroundingGT`` (the submission plane rejects sentinel tags outright).
    It becomes ``OovGroundingGT.llm_declined`` instead. A sentinel that
    unexpectedly arrives with rules is still a declination; the rows are
    dropped with a warning, since there is no catalog tree a non-label could
    hydrate.
    """
    real: TagToAppliedRulesMap = {}
    declined = False
    for tag, rows in tag_rules.items():
        if is_sentinel_grounding_label(tag):
            declined = True
            if rows:
                logger.warning(
                    f"sentinel {tag!r} arrived with {len(rows)} applied rules "
                    f"— recorded as a declination, rules dropped"
                )
            continue
        real[tag] = rows
    return real, declined


def inflate_tag_groundings(
    catalog: RuleCatalog, tag_rules: TagToAppliedRulesMap
) -> dict[str, TagGroundingGT]:
    """One ``TagGroundingGT`` per tag the LLM grounded a phrase to."""
    inflated: dict[str, TagGroundingGT] = {}
    for tag, rows in tag_rules.items():
        if not rows:
            raise InflationError(
                f"{catalog.prompt_name}: tag {tag!r} has no rules — a validated "
                f"grounding report always carries its always-reported rules"
            )
        inflated[tag] = TagGroundingGT(
            llm_result=inflate_applied_rules(catalog, rows), audits=[]
        )
    return inflated


def _partition_rows_by_catalog(
    rows: list[AppliedRule], catalogs: list[RuleCatalog]
) -> dict[str, list[AppliedRule]]:
    """Rows keyed by the prompt_name of the catalog that knows them.

    A descent node's ``direct`` rules can be initial-grounding rows while its
    ``iterative`` rules are recursive-grounding rows, so a node legitimately
    mixes catalogs. A row no catalog knows is corruption.
    """
    known_by_catalog = {c.prompt_name: set(c.rules_by_id()) for c in catalogs}
    partitions: dict[str, list[AppliedRule]] = {}
    for row in rows:
        for catalog in catalogs:
            if row.rule_id in known_by_catalog[catalog.prompt_name]:
                partitions.setdefault(catalog.prompt_name, []).append(row)
                break
        else:
            raise InflationError(
                f"rule {row.rule_id!r} is unknown to every catalog offered "
                f"({sorted(known_by_catalog)})"
            )
    return partitions


def _inflate_descent_node(
    node: IterativelyTaggedPhrase, catalogs: list[RuleCatalog]
) -> InVocabNodeGT:
    rows = [
        row
        for tag_map in (node.direct_og_tag_w_rules, node.iterative_og_tag_w_rules)
        for rule_list in tag_map.values()
        for row in rule_list
    ]
    catalogs_by_name = {c.prompt_name: c for c in catalogs}
    sections = [
        section
        for name, partition in _partition_rows_by_catalog(rows, catalogs).items()
        for section in inflate_applied_rules(catalogs_by_name[name], partition)
    ]
    return InVocabNodeGT(
        parent_group_id=node.parent_group_id,
        group_id=node.group_id,
        stop_reason=node.stop_reason,
        sections=sections,
        audits=[],
    )


def inflate_in_vocab_grounding(
    result: IterativeGroundingResult,
    *,
    recursive_catalog: RuleCatalog,
    initial_catalog: RuleCatalog,
) -> dict[str, InVocabGroundingGT]:
    """The group-major stored result, regrouped per phrase and inflated.

    Regrouping reuses ``get_phrase_trails`` — the same (parent, group) identity
    the pipeline itself uses. Both catalogs are offered to every node because
    provenance is mixed per node, not per stage (see
    ``_partition_rows_by_catalog``).
    """
    catalogs = [recursive_catalog, initial_catalog]
    return {
        trail.phrase: InVocabGroundingGT(
            levels={
                lvl: [_inflate_descent_node(node, catalogs) for node in nodes]
                for lvl, nodes in sorted(trail.lvl_by_lvl_itps.items())
            }
        )
        for trail in get_phrase_trails(result)
    }


def inflate_chunk(
    stats: LLMPhraseExtractionStats,
    *,
    screening_catalog: RuleCatalog,
    oov_catalog: RuleCatalog,
    recursive_catalog: Optional[RuleCatalog] = None,
    initial_catalog: Optional[RuleCatalog] = None,
) -> ChunkGT:
    """One chunk's stored stats as an audit template: chunk → round → phrase.

    Phrase membership and search_round come from the relationship stats — the
    stage every recorded phrase passes through. A phrase a grounding response
    introduced without ever being related (the known echo anomaly the trail
    dumps also warn about) cannot build an ``ExtractedPhraseGT`` and is warned
    and skipped, mirroring ``extraction_dump_util``.

    Keyword-family stats carry ``llm_phrase_freehand_grounding``,
    concept-family stats ``llm_phrase_initial_grounding`` +
    ``llm_phrase_recursive_grounding``; both land in the same slots here
    (``oov_grounding``, and ``in_vocab_grounding`` for concepts, which is when
    ``recursive_catalog``/``initial_catalog`` must be supplied).
    """
    phrase_rounds: dict[str, int] = {}
    relationships: dict[str, str] = {}
    for round_idx, phrase_map in sorted(stats.llm_phrase_relationship.items()):
        for phrase, description in phrase_map.items():
            if phrase not in phrase_rounds:
                phrase_rounds[phrase] = round_idx
                relationships[phrase] = description

    screenings: dict[str, ScreeningVerdict] = {
        phrase: verdict
        for _round, verdicts in sorted(stats.llm_phrase_screening.items())
        for phrase, verdict in verdicts.items()
    }

    oov_rounds: dict[int, PhraseToTagAndRulesMap] = getattr(
        stats, "llm_phrase_freehand_grounding", None
    ) or getattr(stats, "llm_phrase_initial_grounding", None) or {}
    oov_by_phrase: dict[str, TagToAppliedRulesMap] = {
        phrase: tag_map
        for _round, phrase_map in sorted(oov_rounds.items())
        for phrase, tag_map in phrase_map.items()
    }

    recursive_result: Optional[IterativeGroundingResult] = getattr(
        stats, "llm_phrase_recursive_grounding", None
    )
    in_vocab_by_phrase: dict[str, InVocabGroundingGT] = {}
    if recursive_result:
        if recursive_catalog is None or initial_catalog is None:
            raise InflationError(
                "stats carry recursive grounding, so recursive_catalog and "
                "initial_catalog are both required"
            )
        in_vocab_by_phrase = inflate_in_vocab_grounding(
            recursive_result,
            recursive_catalog=recursive_catalog,
            initial_catalog=initial_catalog,
        )

    for orphan in (set(screenings) | set(oov_by_phrase) | set(in_vocab_by_phrase)) - set(
        phrase_rounds
    ):
        logger.warning(
            f"phrase {orphan!r} appears in later stage output but was never "
            f"related — skipped in the GT template (visible in stats and dumps)"
        )

    extracted: dict[str, ExtractedPhraseGT] = {}
    for phrase, search_round in phrase_rounds.items():
        verdict = screenings.get(phrase)
        raw_tag_map = oov_by_phrase.get(phrase)
        oov_grounding: Optional[OovGroundingGT] = None
        if raw_tag_map:
            real_tags, llm_declined = split_sentinel_tag_rules(raw_tag_map)
            oov_grounding = OovGroundingGT(
                tags=inflate_tag_groundings(oov_catalog, real_tags),
                llm_declined=llm_declined,
            )
        extracted[phrase] = ExtractedPhraseGT(
            search_round=search_round,
            llm_relationship=RelationshipGT(
                llm_result=relationships[phrase], audits=[]
            ),
            llm_screening=(
                ScreeningGT(
                    llm_result=inflate_screening_llm_copy(
                        screening_catalog, verdict
                    ),
                    audits=[],
                )
                if verdict is not None
                else None
            ),
            oov_grounding=oov_grounding,
            in_vocab_grounding=in_vocab_by_phrase.get(phrase),
        )

    return ChunkGT(extracted_phrases=extracted, missed_phrases=[])
