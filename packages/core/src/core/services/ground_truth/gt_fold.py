"""The fold: what the document says is TRUE, given everything humans wrote.

Two readers live here. ``compute_document_truth`` answers "what is the
effective record" — latest derivation wins per surface (settled #10), the LLM
copy stands where nothing human exists but is marked UNREVIEWED, never counted
as agreement. ``rule_agreement_rollup`` answers "where does the LLM drift" —
per-rule agree/agree_but/disagree tallies plus outcome overrides, per document.
Per-document rollups keyed by rule id sum trivially, and the document's
identity already carries subject, field and catalog versions, so any slice
(per rule across subjects, per field, per catalog version) is an aggregation
the analysis layer chooses later.

``effective_passed`` is the combinator fold of settled #6: every evaluated
condition must hold, no guard may have fired, and at least one condition must
have been evaluated. Flipping one condition to failed forces fail; flipping it
back does not force pass while a guard stands — only the fold decides. On a
pure LLM copy this reproduces the stored ``passed`` cache.

These view models are compute, not storage (settled #5): nothing here is ever
persisted.
"""

from __future__ import annotations

from typing import Iterator, Optional

from pydantic import BaseModel

from core.models.ground_truth.audits import AuditVerdict, TextFieldAudit
from core.models.ground_truth.rule_tree import AuditedRule, AuditedSection
from core.models.ground_truth.stage_blocks import (
    CONDITION_HOLDS_OUTCOME,
    GUARD_FIRED_OUTCOME,
    ChunkGT,
    ExtractedPhraseGT,
    GroundingDerivation,
    HumanDescentPath,
    InVocabGroundingGT,
    InVocabNodeGT,
    MissedPhraseEntry,
    ScreeningGT,
    TagGroundingGT,
)


def _walk_rules(sections: list[AuditedSection]) -> Iterator[AuditedRule]:
    def _descend(rules: list[AuditedRule]) -> Iterator[AuditedRule]:
        for rule in rules:
            yield rule
            yield from _descend(rule.sub_rules)

    for section in sections:
        yield from _descend(section.applied_rules)


def effective_passed(sections: list[AuditedSection]) -> bool:
    """The combinator fold over outcomes, human-authored and reported alike."""
    saw_condition = False
    for rule in _walk_rules(sections):
        if rule.kind == "condition" and rule.outcome is not None:
            saw_condition = True
            if rule.outcome != CONDITION_HOLDS_OUTCOME:
                return False
        elif rule.kind == "guard" and rule.outcome == GUARD_FIRED_OUTCOME:
            return False
    return saw_condition


# --- effective views ---------------------------------------------------------


class EffectiveText(BaseModel):
    """A text field after its audits: ``agree`` keeps it, ``agree_but`` keeps
    it and carries the addendum, ``disagree`` replaces it."""

    text: str
    addendum: Optional[str] = None
    reviewed: bool
    verdict: Optional[AuditVerdict] = None


def effective_text(llm_text: str, audits: list[TextFieldAudit]) -> EffectiveText:
    if not audits:
        return EffectiveText(text=llm_text, reviewed=False)
    latest = audits[-1]
    if latest.type is AuditVerdict.DISAGREE:
        corrected = latest.corrected_text
        assert corrected is not None  # TextFieldAudit requires it for disagree
        return EffectiveText(text=corrected, reviewed=True, verdict=latest.type)
    return EffectiveText(
        text=llm_text,
        addendum=latest.corrected_text,
        reviewed=True,
        verdict=latest.type,
    )


class ScreeningTruth(BaseModel):
    identified_entity: Optional[str]
    passed: bool
    reviewed: bool


def effective_screening(gt: ScreeningGT) -> ScreeningTruth:
    if not gt.audits:
        return ScreeningTruth(
            identified_entity=gt.llm_result.identified_entity,
            passed=effective_passed(gt.llm_result.sections),
            reviewed=False,
        )
    latest = gt.audits[-1]
    return ScreeningTruth(
        identified_entity=latest.identified_entity,
        passed=effective_passed(latest.sections),
        reviewed=True,
    )


class TagTruth(BaseModel):
    held: bool
    reviewed: bool
    replaced_from: Optional[str] = None


def _effective_link(
    llm_tag: str,
    llm_sections: Optional[list[AuditedSection]],
    audits: list[GroundingDerivation],
) -> tuple[str, TagTruth]:
    if not audits:
        # llm_sections is present by construction: a link with neither an llm
        # result nor a derivation cannot exist (TagGroundingGT enforces it).
        return llm_tag, TagTruth(
            held=effective_passed(llm_sections or []), reviewed=False
        )
    latest = audits[-1]
    return latest.tag, TagTruth(
        held=effective_passed(latest.sections),
        reviewed=True,
        replaced_from=llm_tag if latest.tag != llm_tag else None,
    )


def effective_tag_groundings(
    tags: dict[str, TagGroundingGT],
) -> dict[str, TagTruth]:
    effective: dict[str, TagTruth] = {}
    for llm_tag, entry in tags.items():
        tag, truth = _effective_link(llm_tag, entry.llm_result, entry.audits)
        effective[tag] = truth
    return effective


class InVocabNodeTruth(BaseModel):
    parent_group_id: Optional[str]
    group_id: str
    stop_reason: Optional[str] = None
    held: bool
    reviewed: bool
    replaced_from: Optional[str] = None


_NodeId = tuple[int, Optional[str], str]  # (level, parent_group_id, group_id)


def _excluded_node_ids(
    levels: dict[int, list[InVocabNodeGT]],
    *,
    initial_roots: set[str],
    path_roots: list[_NodeId],
) -> set[_NodeId]:
    """Stored subtrees superseded by a reset or a divergence path.

    Chains follow the storage's own name-pair identity
    (parent_group_id == the parent's group_id).
    """
    frontier: list[_NodeId] = list(path_roots)
    for node in levels.get(1, []):
        if node.parent_group_id is None and node.group_id in initial_roots:
            frontier.append((1, node.parent_group_id, node.group_id))
    excluded = set(frontier)
    while frontier:
        next_frontier: list[_NodeId] = []
        for level, _parent, group in frontier:
            for node in levels.get(level + 1, []):
                if node.parent_group_id == group:
                    node_id = (level + 1, node.parent_group_id, node.group_id)
                    if node_id not in excluded:
                        excluded.add(node_id)
                        next_frontier.append(node_id)
        frontier = next_frontier
    return excluded


def effective_in_vocab(
    gt: InVocabGroundingGT,
    exclude_initial_roots: set[str] = frozenset(),  # type: ignore[assignment]
) -> dict[int, list[InVocabNodeTruth]]:
    """The descent's effective truth: stored nodes minus superseded subtrees,
    plus divergence-path nodes (verdict 6).

    ``exclude_initial_roots`` carries the reset precedence: initial tags the
    effective tag view REPLACED — their whole stored subtree is overridden
    wholesale. Divergence paths override the replaced child's subtree (latest
    path per divergence key wins across authors, settled #10) and contribute
    their hop nodes, ``replaced_from`` on the first hop.
    """
    latest_paths: dict[tuple, HumanDescentPath] = {}
    for path in gt.human_paths:
        key = (
            path.anchor_level,
            path.anchor_parent_group_id,
            path.anchor_group_id,
            path.replaces_group_id,
        )
        latest_paths[key] = path
    path_roots: list[_NodeId] = [
        (anchor_level + 1, anchor_group, replaces)
        for (
            anchor_level,
            _anchor_parent,
            anchor_group,
            replaces,
        ) in latest_paths
        if replaces is not None
    ]
    excluded = _excluded_node_ids(
        gt.levels,
        initial_roots=set(exclude_initial_roots),
        path_roots=path_roots,
    )

    levels: dict[int, list[InVocabNodeTruth]] = {}
    for lvl, nodes in gt.levels.items():
        for node in nodes:
            if (lvl, node.parent_group_id, node.group_id) in excluded:
                continue
            group_id, truth = _effective_link(
                node.group_id, node.sections, node.audits
            )
            levels.setdefault(lvl, []).append(
                InVocabNodeTruth(
                    parent_group_id=node.parent_group_id,
                    group_id=group_id,
                    stop_reason=node.stop_reason,
                    held=truth.held,
                    reviewed=truth.reviewed,
                    replaced_from=truth.replaced_from,
                )
            )
    for (anchor_level, _anchor_parent, anchor_group, replaces), path in (
        latest_paths.items()
    ):
        previous = anchor_group
        for index, hop in enumerate(path.hops):
            levels.setdefault(anchor_level + 1 + index, []).append(
                InVocabNodeTruth(
                    parent_group_id=previous,
                    group_id=hop.tag,
                    stop_reason=None,
                    held=effective_passed(hop.sections),
                    reviewed=True,
                    replaced_from=replaces if index == 0 else None,
                )
            )
            previous = hop.tag
    return levels


class PhraseTruth(BaseModel):
    relationship: EffectiveText
    screening: Optional[ScreeningTruth] = None
    tags: dict[str, TagTruth] = {}
    in_vocab: dict[int, list[InVocabNodeTruth]] = {}


class MissedTruth(BaseModel):
    """A human-asserted positive — reviewed by construction (PH-10: it exists
    only because a human derived it to satisfaction)."""

    phrase: str
    identified_entity: str
    tags: list[str]


def _missed_truth(entry: MissedPhraseEntry) -> MissedTruth:
    entity = entry.screening.identified_entity
    assert entity is not None  # PH-10: a missed phrase always identifies one
    return MissedTruth(
        phrase=entry.phrase,
        identified_entity=entity,
        tags=[g.tag for g in entry.groundings],
    )


class ChunkTruth(BaseModel):
    phrases: dict[str, PhraseTruth]
    missed: list[MissedTruth]


def compute_phrase_truth(phrase_gt: ExtractedPhraseGT) -> PhraseTruth:
    tags = (
        effective_tag_groundings(phrase_gt.oov_grounding.tags)
        if phrase_gt.oov_grounding is not None
        else {}
    )
    # Reset precedence (verdict 6a): a REPLACED initial tag supersedes the
    # whole stored descent under it — the phrase links directly to the new
    # tag; no per-node disagrees required.
    replaced_roots = {
        truth.replaced_from
        for truth in tags.values()
        if truth.replaced_from is not None
    }
    return PhraseTruth(
        relationship=effective_text(
            phrase_gt.llm_relationship.llm_result, phrase_gt.llm_relationship.audits
        ),
        screening=(
            effective_screening(phrase_gt.llm_screening)
            if phrase_gt.llm_screening is not None
            else None
        ),
        tags=tags,
        in_vocab=(
            effective_in_vocab(
                phrase_gt.in_vocab_grounding,
                exclude_initial_roots=replaced_roots,
            )
            if phrase_gt.in_vocab_grounding is not None
            else {}
        ),
    )


def compute_document_truth(chunks: dict[str, ChunkGT]) -> dict[str, ChunkTruth]:
    return {
        chunk_key: ChunkTruth(
            phrases={
                phrase: compute_phrase_truth(phrase_gt)
                for phrase, phrase_gt in chunk.extracted_phrases.items()
            },
            missed=[_missed_truth(entry) for entry in chunk.missed_phrases],
        )
        for chunk_key, chunk in chunks.items()
    }


# --- the drift diagnosis -----------------------------------------------------


class RuleAgreement(BaseModel):
    """One rule's tally across a document. ``overridden`` counts effective
    outcomes that differ from what the LLM reported for the same rule —
    outcome drift even where no explicit verdict was typed."""

    agree: int = 0
    agree_but: int = 0
    disagree: int = 0
    overridden: int = 0


def _tally_audits(
    rollup: dict[str, RuleAgreement], sections: list[AuditedSection]
) -> None:
    for rule in _walk_rules(sections):
        if not rule.audits:
            continue  # unreviewed nodes are excluded, never counted as agreement
        entry = rollup.setdefault(rule.rule_id, RuleAgreement())
        latest = rule.audits[-1].type
        if latest is AuditVerdict.AGREE:
            entry.agree += 1
        elif latest is AuditVerdict.AGREE_BUT:
            entry.agree_but += 1
        else:
            entry.disagree += 1


def _tally_overrides(
    rollup: dict[str, RuleAgreement],
    effective_sections: list[AuditedSection],
    llm_sections: Optional[list[AuditedSection]],
) -> None:
    if llm_sections is None:
        return
    llm_outcomes = {rule.rule_id: rule.outcome for rule in _walk_rules(llm_sections)}
    for rule in _walk_rules(effective_sections):
        if rule.rule_id in llm_outcomes and rule.outcome != llm_outcomes[rule.rule_id]:
            rollup.setdefault(rule.rule_id, RuleAgreement()).overridden += 1


def _tally_link(
    rollup: dict[str, RuleAgreement],
    llm_sections: Optional[list[AuditedSection]],
    audits: list[GroundingDerivation],
) -> None:
    for derivation in audits:
        _tally_audits(rollup, derivation.sections)
    if audits:
        _tally_overrides(rollup, audits[-1].sections, llm_sections)


def rule_agreement_rollup(chunks: dict[str, ChunkGT]) -> dict[str, RuleAgreement]:
    """Per-rule tallies for ONE document. Sum across documents and slice by the
    documents' identity fields (subject, field_type, catalog versions)."""
    rollup: dict[str, RuleAgreement] = {}
    for chunk in chunks.values():
        for phrase_gt in chunk.extracted_phrases.values():
            screening = phrase_gt.llm_screening
            if screening is not None:
                for derivation in screening.audits:
                    _tally_audits(rollup, derivation.sections)
                if screening.audits:
                    _tally_overrides(
                        rollup,
                        screening.audits[-1].sections,
                        screening.llm_result.sections,
                    )
            if phrase_gt.oov_grounding is not None:
                for entry in phrase_gt.oov_grounding.tags.values():
                    _tally_link(rollup, entry.llm_result, entry.audits)
            if phrase_gt.in_vocab_grounding is not None:
                for nodes in phrase_gt.in_vocab_grounding.levels.values():
                    for node in nodes:
                        _tally_link(rollup, node.sections, node.audits)
    return rollup
