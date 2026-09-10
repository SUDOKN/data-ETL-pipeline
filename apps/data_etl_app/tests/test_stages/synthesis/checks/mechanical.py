"""Mechanical checks for one (subject, field) of one run.

Three postures, kept strictly apart (README §method):

- INVARIANTS  — structural facts that must always hold; a failure fails the
  pytest suite.
- METRICS     — tracked numbers with no pass/fail of their own; trends are
  judged against the reproducibility noise floor, never per run.
- ENUMERATORS — string scans whose ONLY job is to nominate records for agent
  judgment. Regex over LLM prose has mismeasured in both directions (10x
  under, 69x over); an enumerator is never a verdict.

Wire port 2026-09-05: evidence is ``snippets: [str]`` (no per-snippet
location); the code-derived fold locations are a WATCH metric
(``location_coverage``) and judge pointers; designation preservation is a
proxy metric + enumerator (checks/designations.py).
"""

from __future__ import annotations

import re
import statistics
from typing import Any, Optional

from . import designations, lints, loading, pull

# Enumerator patterns: recall-biased on purpose; precision is the judge's job.
_DOC_LISTING_RE = re.compile(
    r"download|literature|resource|catalog|brochure|datasheet|manual|\.pdf|\.xls",
    re.IGNORECASE,
)
_NEGATION_RE = re.compile(
    r"\b(?:do not|does not|don't|no longer|never|not\s+(?:a|an|the|available|offered)|"
    r"cannot|except|excluding)\b",
    re.IGNORECASE,
)
# Known polysemous short forms across the corpus (Lead the metal vs "We Lead
# the Industry"; Trim electrician vs automotive trim; JET/SHARP/ACER brands…).
POLYSEME_FORMS = {
    "lead",
    "ground",
    "trim",
    "steel",
    "jet",
    "sharp",
    "acer",
    "anchor",
    "able",
    "drawing",
    "rolling",
}


def _clean_requests(dump: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        r
        for r in loading.synthesis_requests(dump)
        if r.get("note") not in ("synthetic_response", "usage_unavailable")
    ]


def run_invariants(
    subject: str,
    field_name: str,
    dump: dict[str, Any],
    records: list[loading.SynthRecord],
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []

    def add(check_id: str, ok: Optional[bool], detail: str) -> None:
        status = "skip" if ok is None else ("pass" if ok else "FAIL")
        results.append({"id": check_id, "status": status, "detail": detail})

    # INV-1: delivery — the stage has never once failed mechanically; keep the
    # tripwire that would notice the first time. Retries are delivery, not
    # failure (the under-answer re-ask is part of the stage), so they are
    # reported in the detail and not counted against it.
    block_present = loading.has_synthesis_block(dump)
    if block_present:
        summaries = [
            (chunk.get("synthesis") or {}).get("summary") or {}
            for chunk in (dump.get("chunks") or {}).values()
            if chunk.get("synthesis")
        ]
        not_synth = [g for s in summaries for g in (s.get("not_synthesized") or [])]
        unknown = [u for s in summaries for u in (s.get("unknown_answer_ids") or [])]
        sent = sum(s.get("records") or 0 for s in summaries)
        done = sum(s.get("synthesized") or 0 for s in summaries)
        retried = sum(len(s.get("retried") or []) for s in summaries)
        add(
            "INV-1-delivery",
            done == sent and not not_synth and not unknown,
            f"synthesized {done}/{sent}, retried={retried}, "
            f"not_synthesized={not_synth}, unknown_answer_ids={unknown}",
        )
    else:
        undelivered = [
            r.group_id for r in records if r.status == "not_synthesized"
        ]
        add(
            "INV-1-delivery",
            not undelivered,
            f"(pre-fix dump, row statuses only) not_synthesized={undelivered}",
        )

    # INV-2: the pv witness — every live request's pv= (group AND retry
    # requests) must match the header.
    header_pv = loading.synthesis_pv(dump)
    request_pvs = {
        loading.pv_of_custom_id(r.get("custom_id") or "")
        for r in _clean_requests(dump)
    } - {None}
    if header_pv is None or not request_pvs:
        add("INV-2-pv-witness", None, "no synthesis requests or header pv to compare")
    else:
        add(
            "INV-2-pv-witness",
            request_pvs == {header_pv},
            f"header pv={header_pv}, request pvs={sorted(p for p in request_pvs if p)}",
        )

    # INV-4: an empty group must never carry a synthesis.
    violated = [
        r.group_id
        for r in records
        if r.status == "no_mentions" and r.synthesis is not None
    ]
    add("INV-4-empty-groups", not violated, f"no_mentions with synthesis: {violated}")

    # INV-5: the 2026-08-26 instrument fix — full-run dumps carry the synthesis
    # block. Informational (skip) on runs predating the fix.
    add(
        "INV-5-synthesis-block",
        True if block_present else None,
        "synthesis block present"
        if block_present
        else "absent (run predates the 2026-08-26 dump fix, or fix regressed "
        "— compare run date)",
    )
    return results


def contract_products_invariant(
    products_records: list[loading.SynthRecord],
    contract_records: list[loading.SynthRecord],
) -> dict[str, Any]:
    """INV-3, subject-level: contract_products' syntheses are a byte-copy of
    products' (one shared LLM call). A divergence means the sharing broke."""
    def view(records: list[loading.SynthRecord]) -> dict[tuple[str, str], tuple]:
        return {
            (r.chunk_bounds, r.group_id): (r.focal_form, r.synthesis)
            for r in records
        }

    p, c = view(products_records), view(contract_records)
    diverged = sorted(
        key for key in (p.keys() | c.keys()) if p.get(key) != c.get(key)
    )
    return {
        "id": "INV-3-contract-byte-copy",
        "status": "pass" if not diverged else "FAIL",
        "detail": f"{len(diverged)} diverging (chunk, group) pairs"
        + (f": {diverged[:5]}" if diverged else ""),
    }


def compute_metrics(
    subject: str,
    field_name: str,
    dump: dict[str, Any],
    records: list[loading.SynthRecord],
    evidence_index: Optional[dict[str, Any]],
    subject_name: Optional[str],
    designation_report: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    synthesized = [r for r in records if r.synthesis]
    chars = [len(r.synthesis or "") for r in synthesized]

    def snippets_of(r: loading.SynthRecord) -> Optional[int]:
        if r.snippets is not None:
            return r.snippets
        if evidence_index is not None:
            ev = pull.evidence_for(evidence_index, r)
            if ev is not None:
                return len(ev.get("snippets") or [])
        return None

    snippet_counts = [e for e in (snippets_of(r) for r in synthesized) if e is not None]

    # Lints, recomputed with the pinned current implementations.
    entity_shaped = [
        r for r in synthesized if r.focal_form and lints.is_entity_shaped(r.focal_form)
    ]
    absent = [
        r.group_id
        for r in entity_shaped
        if r.synthesis
        and r.focal_form
        and lints.focal_form_absent(r.synthesis, r.focal_form, r.forms)
    ]
    clusters: Optional[list[dict[str, Any]]] = None
    if evidence_index is not None:
        triples = []
        for r in synthesized:
            ev = pull.evidence_for(evidence_index, r)
            triples.append(
                (r.group_id, r.synthesis, (ev or {}).get("request_custom_id"))
            )
        clusters = lints.identical_synthesis_clusters(triples)

    own_name_rate = None
    if subject_name:
        hits = sum(
            1
            for r in synthesized
            if r.synthesis and lints.count_own_name_hits(r.synthesis, subject_name)
        )
        own_name_rate = round(hits / len(synthesized), 4) if synthesized else None

    requests = _clean_requests(dump)
    retry_requests = [r for r in requests if pull.retry_index(r.get("custom_id") or "")]
    latencies = sorted(
        r["client_latency_ms"] for r in requests if r.get("client_latency_ms")
    )

    def pct(values: list, q: float):
        return values[min(len(values) - 1, int(q * len(values)))] if values else None

    twins = sorted(
        {
            g
            for g in {r.group_id for r in records}
            if sum(1 for r in records if r.group_id == g) > 1
        }
    )

    if designation_report is None:
        designation_report = designations.report(
            synthesized, evidence_index, pull.evidence_for
        )

    return {
        "records": len(records),
        "synthesized": len(synthesized),
        "retried_records": sum(1 for r in synthesized if r.retried),
        "twin_groups": len(twins),
        "snippets_known_for": len(snippet_counts),
        "single_snippet_share": (
            round(sum(1 for e in snippet_counts if e == 1) / len(snippet_counts), 4)
            if snippet_counts
            else None
        ),
        "thin_le2_share": (
            round(sum(1 for e in snippet_counts if e <= 2) / len(snippet_counts), 4)
            if snippet_counts
            else None
        ),
        "max_snippets_in_a_record": max(snippet_counts) if snippet_counts else None,
        "synthesis_chars": sum(chars),
        "chars_per_record_median": statistics.median(chars) if chars else None,
        "focal_form_absent": {
            "entity_shaped_records": len(entity_shaped),
            "flagged": len(absent),
            "flagged_group_ids": absent,
        },
        "identical_synthesis": (
            {
                "clusters": len(clusters),
                "records_in_clusters": sum(len(c["group_ids"]) for c in clusters),
            }
            if clusters is not None
            else None
        ),
        "own_name_record_rate": own_name_rate,  # identification counter, NOT a defect
        # WATCH: code-located mentions on the fold; the model never sees them.
        "location_coverage": loading.fold_location_summary(dump),
        # WATCH (proxy): the statics' designation rule, token-level.
        "designation_preservation": designation_report.get("summary"),
        "requests": len(requests),
        "retry_requests": len(retry_requests),
        "input_tokens": sum(r.get("input_tokens") or 0 for r in requests),
        "output_tokens": sum(r.get("output_tokens") or 0 for r in requests),
        "client_latency_ms_p50": pct(latencies, 0.50),
        "client_latency_ms_p90": pct(latencies, 0.90),
    }


def enumerate_candidates(
    records: list[loading.SynthRecord],
    evidence_index: Optional[dict[str, Any]],
    third_party_roster: list[str],
    locations: Optional[dict[tuple[str, str], list[str]]] = None,
    designation_report: Optional[dict[str, Any]] = None,
) -> dict[str, list[str]]:
    """Candidate group_ids (with chunk bounds, ``bounds:group``) per judgment
    family. Evidence-based families need the snapshot; without it only the
    record-level families fill in. ``locations`` (code-derived, from the fold
    block) join the roster/document scans as provenance text."""
    def tag(r: loading.SynthRecord) -> str:
        return f"{r.chunk_bounds}:{r.group_id}"

    roster_re = (
        re.compile(
            "|".join(re.escape(name) for name in third_party_roster), re.IGNORECASE
        )
        if third_party_roster
        else None
    )
    out: dict[str, list[str]] = {
        "twins": [],
        "retried": [],
        "thin_single_snippet": [],
        "third_party_evidence": [],
        "document_listing": [],
        "negation": [],
        "polyseme": [],
        "designation_dropped": [],
        "collapse_phrase": [],
    }
    per_record = (designation_report or {}).get("per_record") or {}
    seen_group_ids = [r.group_id for r in records]
    for r in records:
        if not r.synthesis:
            continue
        if seen_group_ids.count(r.group_id) > 1:
            out["twins"].append(tag(r))
        if r.retried:
            out["retried"].append(tag(r))
        if any(f.lower() in POLYSEME_FORMS for f in (r.forms or [r.focal_form or ""])):
            out["polyseme"].append(tag(r))
        negated = bool(r.synthesis and _NEGATION_RE.search(r.synthesis))
        ev = (
            pull.evidence_for(evidence_index, r)
            if evidence_index is not None
            else None
        )
        if ev is not None:
            snippets = ev.get("snippets") or []
            located = (locations or {}).get((r.chunk_bounds, r.group_id)) or []
            evidence_text = " ".join([*located, *snippets])
            if len(snippets) == 1:
                out["thin_single_snippet"].append(tag(r))
            if roster_re and roster_re.search(evidence_text):
                out["third_party_evidence"].append(tag(r))
            if _DOC_LISTING_RE.search(evidence_text):
                out["document_listing"].append(tag(r))
            negated = negated or bool(_NEGATION_RE.search(evidence_text))
        if negated:
            out["negation"].append(tag(r))
        detail = per_record.get(r.pair_key)
        if detail:
            if detail.get("dropped"):
                out["designation_dropped"].append(tag(r))
            if detail.get("collapse_phrases"):
                out["collapse_phrase"].append(tag(r))
    return out
