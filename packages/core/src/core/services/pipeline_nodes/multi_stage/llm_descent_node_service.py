"""The Step 2 descent stage's request and parse layer (2026-09-22).

A descent request carries an accepted parent's ACCEPTED records (record
blocks in the user context, subject-keyed) and, in the system text, the
descent prompt with its two per-field placeholders filled: the parent's
name, and the parent's children as the dash-line outline with definitions —
or an empty list for the leaf step. Its answer is the record-major structural
wire: per record the children matched (with the quote that supplies the
narrowing feature), sibling proposals, or the declination that lets the
parent stand. The parse holds options to the parent's children: a vocabulary
label named that is NOT a child is a false child, recorded and dropped (the
parent stands, user decision 2026-09-22); anything else off the list is a
proposal.
"""

from __future__ import annotations

import logging
import traceback
from datetime import datetime
from typing import Any, Mapping, Optional, Sequence

from llm_providers.db_models.gpt_batch_request import GPTBatchRequest
from llm_providers.field_types import BatchRequestIDType
from llm_providers.models.file_objects.prompt import Prompt
from llm_providers.models.llm_model import LLM_Model
from llm_providers.models.open_ai.gpt_model_params import GPTModelParams
from llm_providers.services.gpt_batch_request.gpt_batch_request_service import (
    create_base_gpt_batch_request,
)
from llm_providers.services.gpt_batch_request.gpt_batch_request_writes import (
    record_response_parse_error_capped,
)

from core.models.extraction_schemas.catalog_wire_schema import response_format_for
from core.models.extraction_schemas.grounding import (
    RecordGroundingEntry,
    RecordGroundingResults,
)
from core.models.extraction_schemas.synthesis import GroupRecords
from core.models.field_types import ConceptFieldType
from core.models.rule_catalog import STAGE_DESCENT, RuleCatalog
from core.models.skos_concept import Concept
from core.services.phrase_blocks_contract import (
    hold_response_to_sent_record_ids,
    render_record_blocks,
    sent_record_ids_from_user_message,
    sent_records_from_user_message,
)
from core.services.pipeline_nodes.multi_stage.llm_grounding_node_service import (
    parse_record_grounding_structural_result,
    subject_keyed_payloads,
)
from core.services.rule_catalog_registry import get_rule_catalog
from core.utils.rdf_to_graph_util import render_concept_outline

logger = logging.getLogger(__name__)

DESCENT_LABEL = "descent"
NO_CHILDREN_LINE = "(the vocabulary holds nothing narrower)"


def descent_catalog_for(field_name: str) -> RuleCatalog:
    return get_rule_catalog(STAGE_DESCENT, field_name)


def descent_request_payload(
    group_records: GroupRecords, record_ids: Sequence[str], children: Sequence[Concept]
) -> dict[str, Any]:
    """What a descent request is built from and what its ``|ud=`` digest is
    over: the parent's accepted records (subject-keyed) and the children
    offered, by name."""
    missing = sorted(rid for rid in record_ids if rid not in group_records)
    if missing:
        raise ValueError(f"descent_request_payload: no group record for id(s) {missing}")
    records = subject_keyed_payloads({rid: group_records[rid].model_dump() for rid in sorted(set(record_ids))})
    return {"records": records, "children": sorted(c.name for c in children)}


def render_descent_prompt(
    prompt: Prompt, field_type: ConceptFieldType, parent: Concept, children: Sequence[Concept]
) -> str:
    """The system text: the descent prompt with the parent's name and the
    children's dash-line outline (definitions on their own lines) in place of
    the field's two placeholders; the leaf step lists nothing."""
    parent_stem, child_stem = field_type.recursive_grounding_placeholders
    outline = render_concept_outline(list(children), with_definitions=True) if children else NO_CHILDREN_LINE
    return prompt.text.replace(parent_stem, parent.name).replace(child_stem, outline)


def create_deferred_descent_gpt_request(
    *,
    deferred_at: datetime,
    subject_unique_id: str,
    field_type: ConceptFieldType,
    request_id: str,
    prompt: Prompt,
    parent: Concept,
    children: Sequence[Concept],
    payload: Mapping[str, Any],
    gpt_model: LLM_Model,
    eager: bool,
    model_params: GPTModelParams,
) -> GPTBatchRequest:
    return create_base_gpt_batch_request(
        deferred_at=deferred_at,
        subject_unique_id=subject_unique_id,
        custom_id=request_id,
        context=render_record_blocks(dict(payload["records"])),
        prompt_text=render_descent_prompt(prompt, field_type, parent, children),
        gpt_model=gpt_model,
        model_params=model_params.with_response_format(response_format_for(descent_catalog_for(field_type.name))),
        batch_id="Eager" if eager else None,
    )


class DescentAnswer:
    """One descent request's parsed answer: per record the children matched
    (canonical names) with their rules, the proposals, the declinations, and
    the false children."""

    def __init__(self) -> None:
        self.reached: dict[str, RecordGroundingResults] = {}  # child → {record: entry(tags={child: rules})}
        self.proposals: dict[str, RecordGroundingResults] = {}  # proposal → {record: entry}
        self.declined: dict[str, str] = {}  # record → reason
        self.false_children: dict[str, list[str]] = {}  # record → labels
        self.answered: set[str] = set()


async def parse_descent_request(
    *,
    subject_unique_id: str,
    field_name: str,
    req_id: BatchRequestIDType,
    completed_request_map: dict[BatchRequestIDType, GPTBatchRequest],
    children: Sequence[Concept],
    vocabulary_names: Mapping[str, str],
    timestamp: datetime,
) -> DescentAnswer:
    """Parse one descent (or leaf-step) request. Options are held to the
    parent's children by name or other name; a proposal that is a vocabulary
    label elsewhere is a FALSE CHILD (dropped, recorded); a record the answer
    left out is a declination without a reason (the parent stands)."""
    req_obj = completed_request_map.get(req_id)
    if not req_obj:
        raise ValueError(f"{DESCENT_LABEL}: Missing GPTBatchRequest for request ID {req_id} in {subject_unique_id}:{field_name}")
    elif not req_obj.response:
        raise ValueError(f"{DESCENT_LABEL}: GPTBatchRequest for request ID {req_id} has no response_blob in {subject_unique_id}:{field_name}")
    child_labels: list[str] = [label for c in children for label in [c.name, *c.altLabels]]
    child_name_of = {label.casefold(): c.name for c in children for label in [c.name, *c.altLabels]}
    try:
        user_message = req_obj.request.body.user_message()
        sent = sent_records_from_user_message(user_message)
        parsed = parse_record_grounding_structural_result(
            req_obj.response.result,
            catalog=descent_catalog_for(field_name),
            allowed_labels=child_labels,
            sent_records=sent if isinstance(sent, dict) else None,
        )
        held = hold_response_to_sent_record_ids(
            user_message=user_message, response_by_record_id=parsed,
            where=f"{subject_unique_id}:{field_name} {DESCENT_LABEL} {req_id}", on_missing="drop",
        )
        sent_ids = sent_record_ids_from_user_message(user_message) or []
    except Exception as e:
        await record_response_parse_error_capped(
            gpt_batch_request=req_obj, error_message=str(e), timestamp=timestamp, traceback_str=traceback.format_exc(),
        )
        logger.error(f"{DESCENT_LABEL}: Error parsing results for subject {subject_unique_id} from GPT response: {e}")
        raise

    answer = DescentAnswer()
    answer.answered = set(held)
    for rid, entry in held.items():
        if not entry.tags:
            answer.declined[rid] = entry.explanation or ""
            continue
        for label, rules in entry.tags.items():
            child = child_name_of.get(label.casefold())
            if child is not None:
                answer.reached.setdefault(child, {})[rid] = RecordGroundingEntry(tags={child: rules})
                continue
            elsewhere = vocabulary_names.get(label.casefold())
            if elsewhere is not None:
                answer.false_children.setdefault(rid, []).append(elsewhere)
                logger.warning(f"{DESCENT_LABEL}: record {rid} named {elsewhere!r}, a vocabulary label that is not a child here; recorded as a false child")
                continue
            answer.proposals.setdefault(label, {})[rid] = RecordGroundingEntry(tags={label: rules})
    for rid in sent_ids:
        if rid not in held:
            logger.warning(f"{DESCENT_LABEL}: record {rid} unanswered in {req_id}; the parent stands for it")
    return answer


# --- the vocabulary view, the chunk's inputs, and the trail reader (2026-09-22) ---
#
# Shared by the descent node (which issues requests and reads answers during
# the walk) and the reconcile node (which reads the finished trail and decides
# what ships), so the two can never disagree about what a wave contained.

from core.models.extraction_schemas.descent import (  # noqa: E402
    PROPOSAL_WAVE,
    DescentRequest,
    DescentTrail,
    WaveTrail,
)
from core.models.extraction_schemas.screening import RecordScreeningResults  # noqa: E402
from core.services.pipeline_nodes.multi_stage.llm_relationship_screening_node_service import (  # noqa: E402
    UnitRequestPayload,
    build_unit_request_payload,
    get_unit_screening_result,
    unit_screening_catalog_for,
)
from core.services.pipeline_nodes.multi_stage.stage_derivations import (  # noqa: E402
    pack_units,
    wave_group,
)
from core.utils.rdf_to_graph_util import get_match_label_to_concept_map  # noqa: E402


class Vocabulary:
    """One field's vocabulary as the descent needs it: names and other names,
    parents and children, depths, definitions."""

    def __init__(self, known_concepts: set[Concept]) -> None:
        self.known_concepts = known_concepts
        self.match_label_to_concept_map = get_match_label_to_concept_map(known_concepts)
        self.concept_by_name: dict[str, Concept] = {c.name: c for c in known_concepts}
        self.children_of: dict[str, list[Concept]] = {}
        for c in known_concepts:
            if c.ancestors:
                self.children_of.setdefault(c.ancestors[-1], []).append(c)
        for kids in self.children_of.values():
            kids.sort(key=lambda c: c.name)
        self.max_depth = max((c.level for c in known_concepts), default=1)
        # casefolded name or other name → vocabulary name
        self.vocabulary_names: dict[str, str] = {
            label.casefold(): c.name for c in known_concepts for label in [c.name, *c.altLabels]
        }

    def fold(self, label: str) -> str:
        return self.vocabulary_names.get(label.casefold(), label)

    def name_of(self, label: str) -> Optional[str]:
        return self.vocabulary_names.get(label.casefold())

    def ancestors_of(self, label: str) -> list[str]:
        c = self.concept_by_name.get(label)
        return list(c.ancestors) if c else []

    def depth_of(self, name: str) -> int:
        return self.concept_by_name[name].level

    def meaning_of(self, label: str) -> Optional[str]:
        c = self.concept_by_name.get(label)
        return (c.definition or None) if c else None

    def allowed_labels(self) -> list[str]:
        return list(self.match_label_to_concept_map.keys())

    def is_leaf(self, name: str) -> bool:
        return not self.children_of.get(name)


class ChunkInputs:
    """What one chunk's waves are computed from: its group records, the
    direct vocabulary matches by depth (label → records), and the proposals
    with their sources."""

    def __init__(self) -> None:
        self.group_records: GroupRecords = {}
        self.direct_by_depth: dict[int, dict[str, set[str]]] = {}
        self.proposals: dict[str, set[str]] = {}
        self.proposal_sources: dict[str, set[str]] = {}

    def add_proposal(self, label: str, record_id: str, source: str) -> None:
        key = next((k for k in self.proposals if k.casefold() == label.casefold()), label)
        self.proposals.setdefault(key, set()).add(record_id)
        self.proposal_sources.setdefault(key, set()).add(source)

    def direct_at(self, depth: int) -> dict[str, list[str]]:
        return {label: sorted(ids) for label, ids in self.direct_by_depth.get(depth, {}).items()}

    def proposal_units(self) -> dict[str, list[str]]:
        return {label: sorted(ids) for label, ids in sorted(self.proposals.items(), key=lambda kv: kv[0].casefold())}


def chunk_inputs_from(
    vocab: Vocabulary,
    group_records: GroupRecords,
    sources: Sequence[tuple[str, RecordGroundingResults]],
) -> ChunkInputs:
    """The inputs from the grounding call's and the proposal pass's per-record
    maps: a tag that is a vocabulary label (by name or other name) is a direct
    match at its depth; anything else is a proposal."""
    inputs = ChunkInputs()
    inputs.group_records = group_records
    for source, results in sources:
        for rid, entry in results.items():
            for label in entry.tags:
                name = vocab.name_of(label)
                if name is None:
                    inputs.add_proposal(label, rid, source)
                else:
                    inputs.direct_by_depth.setdefault(vocab.depth_of(name), {}).setdefault(name, set()).add(rid)
    return inputs


def failed_by_record(verdicts: RecordScreeningResults, into: dict[str, set[str]]) -> None:
    for rid, by_candidate in verdicts.items():
        for label, verdict in by_candidate.items():
            if not verdict.passed:
                into.setdefault(rid, set()).add(label)


def accepted_by_label(verdicts: RecordScreeningResults) -> dict[str, list[str]]:
    out: dict[str, set[str]] = {}
    for rid, by_candidate in verdicts.items():
        for label, verdict in by_candidate.items():
            if verdict.passed:
                out.setdefault(label, set()).add(rid)
    return {label: sorted(ids) for label, ids in out.items()}


def wave_payloads(vocab: Vocabulary, inputs: ChunkInputs, units: Mapping[str, Sequence[str]], cap: int) -> list[UnitRequestPayload]:
    if not units:
        return []
    return [build_unit_request_payload(inputs.group_records, group, meaning_of=vocab.meaning_of) for group in pack_units(units, cap)]


async def reached_and_answers_from(
    *,
    vocab: Vocabulary,
    subject_unique_id: str,
    field_name: str,
    reqs: Sequence[DescentRequest],
    completed: dict[BatchRequestIDType, GPTBatchRequest],
    timestamp: datetime,
) -> tuple[dict[str, set[str]], dict[str, DescentAnswer]]:
    """Parse a wave's descent answers: (child → records reached, parent → answer)."""
    reached: dict[str, set[str]] = {}
    answers: dict[str, DescentAnswer] = {}
    for r in reqs:
        answer = await parse_descent_request(
            subject_unique_id=subject_unique_id, field_name=field_name, req_id=r.req_id,
            completed_request_map=completed, children=([] if r.leaf else vocab.children_of.get(r.parent, [])),
            vocabulary_names=vocab.vocabulary_names, timestamp=timestamp,
        )
        answers[r.parent] = answer
        for child, by_record in answer.reached.items():
            reached.setdefault(child, set()).update(by_record)
    return reached, answers


def _declined_entry(reason: str) -> RecordGroundingEntry:
    return RecordGroundingEntry(tags={}, explanation=reason or "the subject fixes nothing narrower")


async def read_descent_trail(
    *,
    vocab: Vocabulary,
    subject_unique_id: str,
    field_name: str,
    inputs: ChunkInputs,
    screens: Mapping[int, Sequence[BatchRequestIDType]],
    descents: Mapping[int, Sequence[DescentRequest]],
    completed: dict[BatchRequestIDType, GPTBatchRequest],
    timestamp: datetime,
) -> DescentTrail:
    """The chunk's whole descent, re-read from the completed answers, LEVEL BY
    LEVEL: for each depth wave its units, the pairs removed under a failed
    ancestor, its verdicts, every accepted parent's descent answer (children
    reached, sibling proposals, declinations), the leaf answers and the false
    children; then the proposal wave. This is what the reconcile step decides
    from and what the dump shows."""
    trail = DescentTrail(max_depth=vocab.max_depth)
    failed: dict[str, set[str]] = {}
    reached: dict[str, set[str]] = {}
    for wave in range(1, vocab.max_depth + 1):
        if wave not in screens:
            break
        units, removed = wave_group(inputs.direct_at(wave), {k: sorted(v) for k, v in reached.items()}, failed, vocab.ancestors_of)
        wt = WaveTrail(units=units, removed_under_failed_ancestor=removed)
        trail.waves[wave] = wt
        if screens[wave]:
            wt.screening = await get_unit_screening_result(
                subject_unique_id=subject_unique_id, field_name=field_name,
                catalog=unit_screening_catalog_for(field_name), req_ids=list(screens[wave]),
                completed_request_map=completed, timestamp=timestamp,
            )
        failed_by_record(wt.screening, failed)
        if wave not in descents:
            break
        reached, answers = await reached_and_answers_from(
            vocab=vocab, subject_unique_id=subject_unique_id, field_name=field_name,
            reqs=descents[wave], completed=completed, timestamp=timestamp,
        )
        for r in descents[wave]:
            answer = answers[r.parent]
            per_record: RecordGroundingResults = {}
            for child, by_record in answer.reached.items():
                for rid, entry in by_record.items():
                    per_record.setdefault(rid, entry).tags.update(entry.tags)
            for label, by_record in answer.proposals.items():
                for rid, entry in by_record.items():
                    per_record.setdefault(rid, entry).tags.update(entry.tags)
                    inputs.add_proposal(label, rid, f"{'leaf' if r.leaf else 'descent'}:{r.parent}")
            for rid, reason in answer.declined.items():
                per_record.setdefault(rid, _declined_entry(reason))
            (wt.leaf if r.leaf else wt.descent)[r.parent] = per_record
            if answer.false_children:
                wt.false_children[r.parent] = {rid: sorted(v) for rid, v in answer.false_children.items()}
    trail.proposal_units = inputs.proposal_units()
    trail.proposal_sources = {label: sorted(s) for label, s in inputs.proposal_sources.items()}
    if screens.get(PROPOSAL_WAVE):
        trail.proposal_screening = await get_unit_screening_result(
            subject_unique_id=subject_unique_id, field_name=field_name,
            catalog=unit_screening_catalog_for(field_name), req_ids=list(screens[PROPOSAL_WAVE]),
            completed_request_map=completed, timestamp=timestamp,
        )
    return trail


# --- what the reconcile step decides from the trail (2026-09-22) -------------

from core.db_models.vocabulary_candidates import CandidateEvidence, VocabularyCandidate  # noqa: E402
from core.models.extraction_schemas.applied_rule import AppliedRule  # noqa: E402
from core.services.pipeline_nodes.multi_stage.stage_derivations import deepest_accepted_labels  # noqa: E402

_QUOTE_OUTCOMES = ("satisfied", "unverified")


def quote_of(rules: Sequence[AppliedRule]) -> str:
    """The quote a grounding tag carries: the explanation of its evidence
    condition (outcome ``satisfied``, or ``unverified`` when the quote was
    kept without a verbatim match)."""
    for rule in rules:
        if rule.outcome in _QUOTE_OUTCOMES and rule.explanation:
            return rule.explanation
    return ""


def split_grounding_results(vocab: Vocabulary, results: RecordGroundingResults) -> tuple[RecordGroundingResults, RecordGroundingResults]:
    """One per-record map into two: the vocabulary matches (a declined record
    keeps its explanation here) and the proposals."""
    in_vocab: RecordGroundingResults = {}
    proposals: RecordGroundingResults = {}
    for rid, entry in results.items():
        vocab_tags = {label: rules for label, rules in entry.tags.items() if vocab.name_of(label) is not None}
        proposal_tags = {label: rules for label, rules in entry.tags.items() if vocab.name_of(label) is None}
        in_vocab[rid] = RecordGroundingEntry(
            tags=vocab_tags, explanation=entry.explanation if not vocab_tags and not proposal_tags else None,
            dropped_options=list(entry.dropped_options), dropped_quotes=list(entry.dropped_quotes),
        )
        if proposal_tags:
            proposals[rid] = RecordGroundingEntry(tags=proposal_tags)
    return in_vocab, proposals


def accepted_labels_by_record(trail: DescentTrail) -> dict[str, list[str]]:
    """record → every vocabulary label a depth wave accepted on it."""
    out: dict[str, set[str]] = {}
    for wt in trail.waves.values():
        for rid, by_candidate in wt.screening.items():
            for label, verdict in by_candidate.items():
                if verdict.passed:
                    out.setdefault(rid, set()).add(label)
    return {rid: sorted(labels) for rid, labels in out.items()}


def shipped_labels_by_record(vocab: Vocabulary, trail: DescentTrail) -> dict[str, list[str]]:
    """The reconcile rule over the trail: per record the deepest accepted
    labels (``deepest_accepted_labels``) — what ships in vocabulary."""
    return deepest_accepted_labels(accepted_labels_by_record(trail), vocab.ancestors_of)


def accepted_proposals(trail: DescentTrail) -> set[str]:
    """The proposals the proposal wave accepted on at least one record — what
    ships out of vocabulary."""
    return {label for by_candidate in trail.proposal_screening.values() for label, verdict in by_candidate.items() if verdict.passed}


def merged_screening(trail: DescentTrail) -> RecordScreeningResults:
    """Every verdict of every wave (depth waves, then the proposal wave) in
    one record → candidate → verdict map; a later wave's verdict on the same
    pair (never expected) would win."""
    merged: RecordScreeningResults = {}
    for wave in sorted(trail.waves):
        for rid, by_candidate in trail.waves[wave].screening.items():
            merged.setdefault(rid, {}).update(by_candidate)
    for rid, by_candidate in trail.proposal_screening.items():
        merged.setdefault(rid, {}).update(by_candidate)
    return merged


def leaf_step_view(trail: DescentTrail) -> dict[str, RecordGroundingResults]:
    out: dict[str, RecordGroundingResults] = {}
    for wt in trail.waves.values():
        for leaf, per_record in wt.leaf.items():
            out.setdefault(leaf, {}).update(per_record)
    return out


def _parent_of_source(source: str) -> Optional[str]:
    for prefix in ("descent:", "leaf:"):
        if source.startswith(prefix):
            return source[len(prefix):]
    return None


def proposal_quotes(trail: DescentTrail, grounding_proposals: Sequence[RecordGroundingResults]) -> dict[str, dict[str, str]]:
    """proposal label → record → the quote it was proposed with, from every
    stage that proposed it (the grounding maps and the descent answers)."""
    quotes: dict[str, dict[str, str]] = {}
    maps: list[RecordGroundingResults] = list(grounding_proposals)
    for wt in trail.waves.values():
        maps.extend(wt.descent.values())
        maps.extend(wt.leaf.values())
    by_fold = {label.casefold(): label for label in trail.proposal_units}
    for results in maps:
        for rid, entry in results.items():
            for label, rules in entry.tags.items():
                key = by_fold.get(label.casefold())
                if key is None:
                    continue
                q = quote_of(rules)
                if q and not quotes.setdefault(key, {}).get(rid):
                    quotes[key][rid] = q
    return quotes


def vocabulary_candidates_from(
    *,
    trail: DescentTrail,
    group_records: GroupRecords,
    grounding_proposals: Sequence[RecordGroundingResults],
    subject_unique_id: str,
    field_name: str,
    run_timestamp: datetime,
    ontology_version_id: str,
    created_at: datetime,
) -> list[VocabularyCandidate]:
    """One document per proposal of the chunk, with its records, quotes,
    sources and the proposal wave's verdict. ``parent_label`` is the first
    descent or leaf parent it was proposed under (alphabetically), or "" for
    a proposal the grounding call or the proposal pass made; every source is
    kept on ``sources``."""
    quotes = proposal_quotes(trail, grounding_proposals)
    out: list[VocabularyCandidate] = []
    for label, record_ids in trail.proposal_units.items():
        sources = trail.proposal_sources.get(label, [])
        parents = sorted({p for p in (_parent_of_source(s) for s in sources) if p is not None})
        verdicts = [trail.proposal_screening[rid][label] for rid in record_ids if label in trail.proposal_screening.get(rid, {})]
        accepted: Optional[bool] = any(v.passed for v in verdicts) if verdicts else None
        failed_rule = next((v.failed_rule for v in verdicts if not v.passed and v.failed_rule), None) if accepted is False else None
        out.append(VocabularyCandidate(
            subject_unique_id=subject_unique_id, field_name=field_name, run_timestamp=run_timestamp,
            ontology_version_id=ontology_version_id, parent_label=parents[0] if parents else "", label=label,
            records=[
                CandidateEvidence(record_id=rid, focal_form=group_records[rid].focal_form if rid in group_records else "", quote=quotes.get(label, {}).get(rid, ""))
                for rid in record_ids
            ],
            sources=list(sources), accepted=accepted, failed_rule=failed_rule, created_at=created_at,
        ))
    return out
