from __future__ import annotations

from datetime import datetime
from typing import Iterator, Optional

from pydantic import BaseModel, ConfigDict, model_validator

from core.models.extraction_schemas.grounding import StopReason
from core.models.ground_truth.audits import EntityFieldAudit, TextFieldAudit
from core.models.rule_catalog import ONLY_REACHABLE_OUTCOME_BY_REPORT_WHEN
from core.models.ground_truth.rule_tree import AuditedRule, AuditedSection

# The outcome that lets a condition hold, mirrored from
# ``applied_rule_validation.passed_implied_by`` — the parse-time twin of the
# structural fold below. The two must agree; each names the other. Public
# because the effective fold (``gt_fold``) gates on the same two values.
CONDITION_HOLDS_OUTCOME = "satisfied"
GUARD_FIRED_OUTCOME = ONLY_REACHABLE_OUTCOME_BY_REPORT_WHEN["on_violation"]


def _walk_reported(sections: list[AuditedSection]) -> Iterator[AuditedRule]:
    def _descend(rules: list[AuditedRule]) -> Iterator[AuditedRule]:
        for rule in rules:
            if rule.reported:
                yield rule
            yield from _descend(rule.sub_rules)

    for section in sections:
        yield from _descend(section.applied_rules)


def _happy_path_problem(sections: list[AuditedSection]) -> Optional[str]:
    """Why ``sections`` is not a to-satisfaction derivation, or None if it is.

    The kinds that gate acceptance are the ones ``passed_implied_by`` reads:
    every condition must hold and no guard may have fired. Preferences,
    quality, and format rules never gate, and notes are context. At least one
    condition must be present — every catalog declares at least one, so a
    derivation with none has not applied the catalog at all.
    """

    def _rules(sections: list[AuditedSection]) -> Iterator[AuditedRule]:
        def _descend(rules: list[AuditedRule]) -> Iterator[AuditedRule]:
            for rule in rules:
                yield rule
                yield from _descend(rule.sub_rules)

        for section in sections:
            yield from _descend(section.applied_rules)

    saw_condition = False
    for rule in _rules(sections):
        if rule.kind == "condition":
            saw_condition = True
            if rule.outcome != CONDITION_HOLDS_OUTCOME:
                return (
                    f"condition {rule.rule_id!r} is "
                    f"{rule.outcome!r}, not {CONDITION_HOLDS_OUTCOME!r}"
                )
        elif rule.kind == "guard" and rule.outcome == GUARD_FIRED_OUTCOME:
            return f"guard {rule.rule_id!r} fired"
    if not saw_condition:
        return "no condition was applied"
    return None


class RelationshipGT(BaseModel):
    """The relationship stage's output for one phrase, audits attached.

    Relationship output is plain text (``LLMPhraseRelationshipResults`` maps
    phrase → description string), so this is the simplest block: the text and
    the text-field audits on it.
    """

    model_config = ConfigDict(extra="forbid")

    llm_result: str
    audits: list[TextFieldAudit] = []


class ScreeningLLMCopy(BaseModel):
    """The LLM's screening verdict for one phrase, inflated into the catalog tree.

    Hydrated from the stored ``ScreeningVerdict`` (flat rules) by catalog
    inflation (P2.1); this copy is immutable — human judgments live in
    ``HumanScreeningDerivation`` entries beside it, never here, so the audit
    slots inside these sections stay empty.

    ``passed`` is a validated cache. The fold here is the catalog-free half of
    ``passed_implied_by``: only ``reported=True`` rows count (a synthesized
    unreported guard is recorded silence, not a firing), a condition holds only
    on ``"satisfied"``, any reported guard rejects, and no reported rules at
    all rejects — the no-candidate branch.

    The branch shape mirrors ``ScreeningVerdict``: reported rules are empty
    exactly when no entity was identified, and ``no_candidate_explanation``
    exists only on that branch.
    """

    model_config = ConfigDict(extra="forbid")

    passed: bool
    identified_entity: Optional[str]
    no_candidate_explanation: Optional[str] = None
    sections: list[AuditedSection]

    @model_validator(mode="after")
    def check_branch_shape(self) -> "ScreeningLLMCopy":
        has_reported = any(True for _ in _walk_reported(self.sections))
        if self.identified_entity is None:
            if has_reported:
                raise ValueError(
                    "no identified_entity means the phrase offered no candidate, "
                    "so no rule can have been reported"
                )
            if self.no_candidate_explanation is None:
                raise ValueError(
                    "the no-candidate branch must say why (no_candidate_explanation)"
                )
        else:
            if not has_reported:
                raise ValueError(
                    "a judged entity always carries reported rules — every "
                    "catalog declares at least one always-reported condition"
                )
            if self.no_candidate_explanation is not None:
                raise ValueError(
                    "no_candidate_explanation exists only on the no-candidate branch"
                )
        return self

    @model_validator(mode="after")
    def check_passed_is_cache_of_fold(self) -> "ScreeningLLMCopy":
        computed = self._fold_reported()
        if self.passed is not computed:
            raise ValueError(
                f"stored passed={self.passed} but the reported rules fold to "
                f"{computed}; passed is a cache of the fold, never independent"
            )
        return self

    def _fold_reported(self) -> bool:
        folded_any = False
        for rule in _walk_reported(self.sections):
            folded_any = True
            if rule.kind == "condition" and rule.outcome != CONDITION_HOLDS_OUTCOME:
                return False
            if rule.kind == "guard":
                return False
        return folded_any


class HumanScreeningDerivation(BaseModel):
    """One annotator's full screening derivation for one phrase.

    ``identified_entity`` is the LLM's entity when the annotator kept it, or
    their replacement — the derivation itself carries the asserted value, so
    ``audits`` are ``EntityFieldAudit`` entries (keep/replace verdicts with
    the rationale note, no corrected_text duplicate). ``sections`` start as a
    copy of the LLM's when the entity is kept and fresh from the catalog when
    it changed — that rule needs the original to check, so it is the
    submission service's contract (P2.3), not this model's.
    """

    model_config = ConfigDict(extra="forbid")

    identified_entity: Optional[str]
    audits: list[EntityFieldAudit] = []
    sections: list[AuditedSection]


class ScreeningGT(BaseModel):
    """Screening for one phrase: the immutable LLM copy plus one derivation per
    annotator, in submission order (same-author pop is P2.3's)."""

    model_config = ConfigDict(extra="forbid")

    llm_result: ScreeningLLMCopy
    audits: list[HumanScreeningDerivation] = []


class GroundingDerivation(BaseModel):
    """One annotator's derivation for one grounding link.

    The tag plays the role ``identified_entity`` plays in screening (the
    design's own likening): keep it and audit it, or replace it and re-derive
    the sections fresh from the catalog — enforced at submission (P2.3).
    ``audits`` are ``EntityFieldAudit`` entries: the asserted tag lives on the
    derivation itself, the audit carries the verdict and the rationale note.
    """

    model_config = ConfigDict(extra="forbid")

    tag: str
    audits: list[EntityFieldAudit] = []
    sections: list[AuditedSection]


class TagGroundingGT(BaseModel):
    """Ground truth for one (phrase, tag) grounding link.

    ``llm_result=None`` marks a human-asserted tag — one the LLM never
    produced — and such an entry must carry at least one derivation, or it
    asserts nothing.
    """

    model_config = ConfigDict(extra="forbid")

    llm_result: Optional[list[AuditedSection]]
    audits: list[GroundingDerivation] = []

    @model_validator(mode="after")
    def check_human_asserted_tag_has_a_derivation(self) -> "TagGroundingGT":
        if self.llm_result is None and not self.audits:
            raise ValueError(
                "a human-asserted tag (llm_result=None) with no derivation "
                "asserts nothing"
            )
        return self


class OovGroundingGT(BaseModel):
    """Freehand grounding (keywords) or initial grounding (concepts) for one
    phrase: the stored side is ``{phrase → {tag → rules}}``, so per phrase this
    is the tag map. A phrase can legitimately ground to several tags.

    ``llm_declined=True`` records the stage's escape hatch: the model examined
    the phrase and answered with the sentinel label ("None of the above" /
    "Cannot categorize") instead of grounding it. The sentinel never becomes a
    tag entry — reserved non-labels never reach ground truth — but the
    declination is part of the run's record, and this document is its surviving
    copy. Corrections are unchanged: a human-asserted tag entry stands beside
    the flag, and absence of the flag means the stage simply never produced a
    grounding answer for the phrase.
    """

    model_config = ConfigDict(extra="forbid")

    tags: dict[str, TagGroundingGT]
    llm_declined: bool = False


class InVocabNodeGT(BaseModel):
    """One node of a phrase's recursive-grounding descent, audits attached.

    Identity mirrors ``IterativelyTaggedPhrase``: (parent_group_id, group_id),
    with ``parent_group_id=None`` for tags applied at initial grounding and
    ``stop_reason`` recording why a node was never descended. The stored side
    splits a node's rules into direct vs iterative og-tag maps (alt-label
    indirection included); this document deliberately keeps one container —
    the audit is on the link, not on which traversal produced it.
    """

    model_config = ConfigDict(extra="forbid")

    parent_group_id: Optional[str]
    group_id: str
    stop_reason: Optional[StopReason] = None
    sections: list[AuditedSection]
    audits: list[GroundingDerivation] = []


class HumanDescentPath(BaseModel):
    """One annotator's divergence from the stored descent (P3 verdict 6b).

    Anchored at the last stored node the annotator still agrees with;
    ``replaces_group_id`` names the disputed child whose subtree this path
    supersedes — sibling branches stand. It is None only for a
    continuation-from-stop: the stored descent ended at the anchor and the
    annotator descends further (validated at submission — an anchor with
    stored children requires naming the replaced one; adding a sibling
    branch beside kept ones is not recordable in v1). Hops are human
    derivations with
    fresh recursive-grounding sections, chained parent→child down the pinned
    ontology (validated at submission, where the ontology is at hand), and
    every hop must hold on the happy path — a hop that fails its own rules
    asserts nothing (the PH-10 doctrine). ``stopped`` is the explicit
    terminal: the API accepts the sentinel label as the stop signal, the
    model stores the flag — no magic tag in storage.

    Path-level provenance mirrors ``MissedPhraseEntry``: one author per path,
    ``at`` with no default on purpose.
    """

    model_config = ConfigDict(extra="forbid")

    author_email: str
    at: datetime
    source: str
    anchor_level: int
    anchor_parent_group_id: Optional[str]
    anchor_group_id: str
    replaces_group_id: Optional[str]
    hops: list[GroundingDerivation]
    stopped: bool

    @model_validator(mode="after")
    def check_path_shape(self) -> "HumanDescentPath":
        if not self.hops:
            raise ValueError("a divergence path with no hops asserts nothing")
        if self.stopped is not True:
            raise ValueError(
                "a divergence path must end with an explicit stop — an "
                "open-ended descent claim is not recordable"
            )
        for hop in self.hops:
            if problem := _happy_path_problem(hop.sections):
                raise ValueError(
                    f"a divergence hop must ground to satisfaction — "
                    f"hop {hop.tag!r}: {problem}"
                )
        return self


class InVocabGroundingGT(BaseModel):
    """A phrase's recursive descent, level by level — the per-phrase mirror of
    ``IterativeGroundingResult``. Lists, not sets: submission order is kept and
    the wire is JSON.

    ``human_paths`` holds annotators' divergences (P3 verdict 6b) beside the
    stored tree — the stored nodes stay immutable LLM copies, corrections to
    the trajectory live here."""

    model_config = ConfigDict(extra="forbid")

    levels: dict[int, list[InVocabNodeGT]]
    human_paths: list[HumanDescentPath] = []


class ExtractedPhraseGT(BaseModel):
    """Everything this instrument records about one extracted phrase.

    ``search_round`` follows the stats convention: 0 is brute-force survivors,
    1 the first LLM search, N>=2 recursive round N-1. Stage blocks are optional
    because which stages exist depends on the field type (keyword fields have
    no recursive descent) and on the phrase's own path (a screened-out phrase
    was never grounded); which combination is REQUIRED is app knowledge, bound
    at the Document layer — core stays field-type-agnostic.
    """

    model_config = ConfigDict(extra="forbid")

    search_round: int
    llm_relationship: RelationshipGT
    llm_screening: Optional[ScreeningGT] = None
    oov_grounding: Optional[OovGroundingGT] = None
    in_vocab_grounding: Optional[InVocabGroundingGT] = None


class MissedPhraseEntry(BaseModel):
    """A phrase the human asserts belongs in the final extracted results —
    a full positive extraction claim, not merely "the search missed this".

    The claim must follow the happy path: the (required) screening derivation
    identifies an entity and applies the catalog to satisfaction, and every
    grounding derivation does the same. A phrase the search missed but that
    would fail screening cannot be recorded here — it changes nothing
    downstream, so there is nothing to assert. This is deliberately stricter
    than ``HumanScreeningDerivation`` on an extracted phrase, where a human may
    legitimately conclude "fail" against an LLM pass.

    Human derivations only: there is no llm slot anywhere in it. Substring
    presence in the chunk text is validated at submission (P2.3), where the
    chunk text is at hand. Assertion provenance mirrors the audit-entry
    convention, including ``at`` having no default on purpose.
    """

    model_config = ConfigDict(extra="forbid")

    phrase: str
    author_email: str
    at: datetime
    source: str
    relationship_text: str
    screening: HumanScreeningDerivation
    groundings: list[GroundingDerivation] = []

    @model_validator(mode="after")
    def check_happy_path(self) -> "MissedPhraseEntry":
        if self.screening.identified_entity is None:
            raise ValueError(
                "a missed phrase asserts an extraction, so its screening "
                "derivation must identify an entity"
            )
        if problem := _happy_path_problem(self.screening.sections):
            raise ValueError(
                f"a missed phrase must screen to satisfaction: {problem}"
            )
        for grounding in self.groundings:
            if problem := _happy_path_problem(grounding.sections):
                raise ValueError(
                    f"a missed phrase must ground to satisfaction — "
                    f"tag {grounding.tag!r}: {problem}"
                )
        return self


class ChunkGT(BaseModel):
    """One chunk's ground truth: what was extracted there, and what was missed.

    Keyed in the document by the same ``"start:end"`` chunk keys as
    ``chunked_extraction_stats`` — the document mirrors storage exactly
    (settled semantics #12). Lives in core so the fold (P2.4, a core service)
    can traverse a whole document without importing the app.
    """

    model_config = ConfigDict(extra="forbid")

    extracted_phrases: dict[str, ExtractedPhraseGT]
    missed_phrases: list[MissedPhraseEntry] = []
