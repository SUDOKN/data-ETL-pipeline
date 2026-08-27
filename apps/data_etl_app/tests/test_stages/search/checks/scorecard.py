"""Assemble, persist, and trend the per-(subject, field) scorecards.

Comparability discipline (the B4 lesson: re-chunking reshuffles ~half the
phrase set): a delta is only computed against the most recent prior row whose
fingerprint hash matches; anything else is INCOMPARABLE, stated, never
silently compared."""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent))
from loading import FieldRun  # noqa: E402
from paths import METRICS_JSONL, fingerprint_hash, run_dir, subject_slug  # noqa: E402


def build_fingerprint(fr: FieldRun) -> dict[str, Any]:
    search_meta = (fr.metadata or {}).get("llm_phrase_search") or {}
    model = (search_meta.get("llm_model") or {}).get("name")
    windows = sorted(
        {(w.chunk_bounds, w.sub_bounds) for w in fr.windows if w.round_index is None}
    )
    pv = None
    for w in fr.windows:
        if w.round_index is None and w.prompt_version:
            pv = w.prompt_version
            break
    payload = {
        "subject": fr.subject,
        "field": fr.field,
        "model": model,
        "search_prompt_version": pv,
        "scraped_text_version": (fr.scraped_text or {}).get("s3_version_id"),
        "windows": windows,
    }
    return {
        **payload,
        "hash": fingerprint_hash(json.dumps(payload, sort_keys=True, default=str)),
    }


def previous_comparable_row(
    fp_hash: str, subject: str, field_name: str, before_run: str
) -> Optional[dict[str, Any]]:
    if not METRICS_JSONL.is_file():
        return None
    best: Optional[dict[str, Any]] = None
    for line in METRICS_JSONL.read_text().splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        if (
            row.get("subject") == subject
            and row.get("field") == field_name
            and row.get("run_id") != before_run
            and row.get("fingerprint_hash") == fp_hash
        ):
            if best is None or row.get("run_id", "") > best.get("run_id", ""):
                best = row
    return best


_TRENDED = (
    ("verbatim", "not_in_window_rate"),
    ("lengths", "six_plus_rate"),
    ("consistency", "consistency"),
    ("expectations", "confirmed_recall"),
    ("sweep", "floor_recall"),
)


def write_scorecard(
    fr: FieldRun, metrics: dict[str, Any], skipped: list[str]
) -> dict[str, Any]:
    fp = build_fingerprint(fr)
    prior = previous_comparable_row(fp["hash"], fr.subject, fr.field, fr.run_id)
    deltas: dict[str, Any] = {}
    if prior:
        for section, key in _TRENDED:
            new = (metrics.get(section) or {}).get(key)
            old = (prior.get("metrics", {}).get(section) or {}).get(key)
            if isinstance(new, (int, float)) and isinstance(old, (int, float)):
                deltas[f"{section}.{key}"] = round(new - old, 4)
    # A judged census costs many agent-hours; a later mechanical re-run must
    # never silently discard it. Carry any existing `judged` block forward and
    # stamp it with the fingerprint it was produced under, so a judged block
    # that predates a bounds/prompt change is visibly stale rather than quietly
    # wrong.
    existing_path = run_dir(fr.run_id) / f"scorecard_{subject_slug(fr.subject)}_{fr.field}.json"
    carried_judged = None
    if existing_path.is_file():
        try:
            previous = json.loads(existing_path.read_text())
        except ValueError:
            previous = {}
        carried_judged = (previous.get("metrics") or {}).get("judged")
        if carried_judged is not None:
            judged_fp = carried_judged.get("fingerprint_hash")
            if judged_fp is None:
                carried_judged["fingerprint_hash"] = fp["hash"]
            elif judged_fp != fp["hash"]:
                carried_judged["stale"] = (
                    "judged under fingerprint "
                    f"{judged_fp}, mechanical pass is {fp['hash']} — re-judge"
                )
    if carried_judged is not None:
        metrics = {**metrics, "judged": carried_judged}

    scorecard = {
        "run_id": fr.run_id,
        "subject": fr.subject,
        "field": fr.field,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "fingerprint": fp,
        "fingerprint_hash": fp["hash"],
        "comparable_baseline_run": prior.get("run_id") if prior else None,
        "deltas_vs_baseline": deltas or None,
        "skipped_missing_input": skipped,
        "metrics": metrics,
    }
    out_dir = run_dir(fr.run_id)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"scorecard_{subject_slug(fr.subject)}_{fr.field}.json"
    out_path.write_text(json.dumps(scorecard, indent=2, ensure_ascii=False) + "\n")
    return scorecard


def append_history(scorecard: dict[str, Any]) -> None:
    """One compact longitudinal row per (run, subject, field) — the offender
    lists and examples stay in the scorecard file, not the trend line."""
    metrics = scorecard["metrics"]
    compact: dict[str, Any] = {}
    for section, payload in metrics.items():
        if not isinstance(payload, dict):
            continue
        compact[section] = {
            k: v
            for k, v in payload.items()
            if isinstance(v, (int, float, str, bool)) or v is None
        }
    row = {
        "run_id": scorecard["run_id"],
        "subject": scorecard["subject"],
        "field": scorecard["field"],
        "generated_at": scorecard["generated_at"],
        "fingerprint_hash": scorecard["fingerprint_hash"],
        "comparable_baseline_run": scorecard["comparable_baseline_run"],
        "verdict": metrics.get("verdict"),
        "metrics": compact,
    }
    METRICS_JSONL.parent.mkdir(parents=True, exist_ok=True)
    with METRICS_JSONL.open("a") as handle:
        handle.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")


def write_summary(run_id: str, scorecards: list[dict[str, Any]]) -> Path:
    lines = [
        f"# Search-stage eval — mechanical pass, run {run_id}",
        "",
        f"Generated {datetime.now(timezone.utc).isoformat()}. Judged metrics",
        "(precision census, recall census, wrong-actor, agreement) are produced",
        "by the RUNBOOK's agent protocol and merged into these scorecards by the",
        "assistant — this file alone is NOT the full evaluation.",
        "",
        "| subject | field | verdict | live/replayed | recall (confirmed) | not-in-window | consistency | Δ vs baseline |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for sc in scorecards:
        m = sc["metrics"]
        cost = m.get("cost", {})
        exp = m.get("expectations") or {}
        verb = m.get("verbatim") or {}
        cons = m.get("consistency") or {}
        verdict = (m.get("verdict") or {}).get("status", "?")
        reds = ", ".join((m.get("verdict") or {}).get("reds", []))
        deltas = sc.get("deltas_vs_baseline")
        delta_text = (
            "; ".join(f"{k} {v:+}" for k, v in deltas.items())
            if deltas
            else ("none comparable" if sc.get("comparable_baseline_run") is None else "no change")
        )
        # An unverified expectation set has no confirmed entries, so recall is
        # null — which must never read as "no misses". Say "not gated".
        recall_value = exp.get("confirmed_recall")
        if recall_value is None:
            candidates = (exp.get("candidates") or {})
            pending = sum(v for v in candidates.values() if isinstance(v, int))
            recall_text = f"not gated ({pending} candidates)" if exp else "—"
        else:
            recall_text = str(recall_value)
        lines.append(
            f"| {sc['subject']} | {sc['field']} | {verdict}{(' — ' + reds) if reds else ''} "
            f"| {cost.get('live', '?')}/{cost.get('replayed', '?')} "
            f"| {recall_text} "
            f"| {verb.get('not_in_window_rate', '—')} "
            f"| {cons.get('consistency', '—')} "
            f"| {delta_text} |"
        )
    out = run_dir(run_id) / "SUMMARY.md"
    out.parent.mkdir(parents=True, exist_ok=True)
    # SUMMARY.md is REGENERATED on every mechanical pass, so the assistant's
    # narrative (verified findings, judged census, caveats) must never live
    # here — it goes in FINDINGS.md, which this file only points at.
    findings = out.parent / "FINDINGS.md"
    if findings.exists():
        lines += ["", "See **FINDINGS.md** in this directory for the written analysis "
                      "of this run (verified findings, judged census status, caveats)."]
    out.write_text("\n".join(lines) + "\n")
    return out
