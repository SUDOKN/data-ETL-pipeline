"""Scorecard and scoreboard writers.

Per run: history/runs/<run_id>/<subject>__<field>.json (the scorecard),
history/runs/<run_id>/REPORT.md (assembled by run_eval), and one appended row per
metric in history/metrics_scoreboard.csv — the one-file trend view.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

from . import loading

HISTORY_ROOT = loading.EVAL_ROOT / "history"
SCOREBOARD = HISTORY_ROOT / "metrics_scoreboard.csv"
SCOREBOARD_FIELDS = ["run_id", "subject", "field", "metric", "value", "denominator", "method"]


def history_dir(run_id: str) -> Path:
    path = HISTORY_ROOT / "runs" / run_id
    path.mkdir(parents=True, exist_ok=True)
    return path


def write_scorecard(run_id: str, subject: str, field_name: str, card: dict[str, Any]) -> Path:
    path = history_dir(run_id) / f"{subject}__{field_name}.json"
    path.write_text(json.dumps(card, indent=2, ensure_ascii=False), encoding="utf-8")
    return path


def _flatten_metrics(card: dict[str, Any]) -> list[tuple[str, Any, Any]]:
    """(metric, value, denominator) rows worth trending."""
    rows: list[tuple[str, Any, Any]] = []
    metrics = card.get("metrics") or {}
    for name in (
        "records",
        "synthesized",
        "retried_records",
        "twin_groups",
        # Named single_entry_share on rows before the 2026-09-05 wire port
        # (same measure: records whose evidence is one passage).
        "single_snippet_share",
        "thin_le2_share",
        "max_snippets_in_a_record",
        "synthesis_chars",
        "chars_per_record_median",
        "own_name_record_rate",
        "requests",
        "retry_requests",
        "input_tokens",
        "output_tokens",
        "client_latency_ms_p50",
        "client_latency_ms_p90",
    ):
        if metrics.get(name) is not None:
            rows.append((name, metrics[name], None))
    location = metrics.get("location_coverage") or {}
    if location.get("mentions"):
        rows.append(("location_coverage", location.get("located"), location.get("mentions")))
    designation = metrics.get("designation_preservation") or {}
    if designation.get("tokens"):
        rows.append(
            (
                "designation_tokens_preserved",
                designation.get("tokens_preserved"),
                designation.get("tokens"),
            )
        )
        rows.append(
            (
                "designation_records_preserved",
                designation.get("records_fully_preserved"),
                designation.get("records_with_designations"),
            )
        )
    ffa = metrics.get("focal_form_absent") or {}
    if ffa:
        rows.append(
            ("focal_form_absent", ffa.get("flagged"), ffa.get("entity_shaped_records"))
        )
    ident = metrics.get("identical_synthesis") or {}
    if ident:
        rows.append(("identical_synthesis_clusters", ident.get("clusters"), None))
    for dim, slot in (card.get("judged_rates") or {}).items():
        rows.append((f"judged_{dim}_fail", slot.get("fail"), slot.get("judged")))
    coverage = card.get("judgment_coverage") or {}
    if coverage:
        rows.append(
            ("judged_coverage", coverage.get("judged"), coverage.get("judgeable"))
        )
    invariant_fails = sum(
        1 for inv in (card.get("invariants") or []) if inv.get("status") == "FAIL"
    )
    rows.append(("invariant_failures", invariant_fails, len(card.get("invariants") or [])))
    return rows


def append_scoreboard(run_id: str, subject: str, field_name: str, card: dict[str, Any]) -> int:
    """Append this card's trend rows; idempotent per (run, subject, field) —
    existing rows for that key are dropped and rewritten."""
    HISTORY_ROOT.mkdir(parents=True, exist_ok=True)
    existing: list[dict[str, Any]] = []
    if SCOREBOARD.is_file():
        with SCOREBOARD.open(newline="", encoding="utf-8") as handle:
            existing = [
                row
                for row in csv.DictReader(handle)
                if not (
                    row["run_id"] == run_id
                    and row["subject"] == subject
                    and row["field"] == field_name
                )
            ]
    method = card.get("provenance", {}).get("method_stamp", "")
    new_rows = [
        {
            "run_id": run_id,
            "subject": subject,
            "field": field_name,
            "metric": metric,
            "value": value,
            "denominator": "" if denominator is None else denominator,
            "method": method,
        }
        for metric, value, denominator in _flatten_metrics(card)
    ]
    with SCOREBOARD.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=SCOREBOARD_FIELDS)
        writer.writeheader()
        for row in sorted(
            existing + new_rows,
            key=lambda r: (r["run_id"], r["subject"], r["field"], r["metric"]),
        ):
            writer.writerow(row)
    return len(new_rows)
