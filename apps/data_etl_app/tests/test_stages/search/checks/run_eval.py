"""The mechanical half of the search-stage evaluation.

Usage:
  .venv/bin/python run_eval.py --run 20260825T194457 [--pull] [--fields products ...]

--pull fetches the run's request docs from Mongo first (needs .env Mongo URI);
without it, an existing raw file is used if present, and metrics that need
wire text are skipped EXPLICITLY (listed under skipped_missing_input).

This runs only the M-battery. The judged half (J1–J5) is the RUNBOOK's agent
protocol; the assistant merges those numbers into the same scorecards."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent))
import mechanical  # noqa: E402
from expectations import load_expectations  # noqa: E402
from loading import load_run  # noqa: E402
from paths import CONFIG_DIR, FIELDS_CONFIG_DIR, run_dir  # noqa: E402
from scorecard import append_history, write_scorecard, write_summary  # noqa: E402


def load_config() -> dict:
    common = yaml.safe_load((CONFIG_DIR / "common.yaml").read_text()) or {}
    fields = {}
    for path in FIELDS_CONFIG_DIR.glob("*.yaml"):
        fields[path.stem] = yaml.safe_load(path.read_text()) or {}
    return {"common": common, "fields": fields}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run", required=True)
    parser.add_argument("--pull", action="store_true", help="pull request docs from Mongo first")
    parser.add_argument("--fields", nargs="*", default=None)
    args = parser.parse_args()

    raw_file = run_dir(args.run) / "raw" / "search_requests.json"
    if args.pull:
        from pull import pull  # local import: offline callers never need pymongo

        raw_file = pull(args.run)
    config = load_config()
    thresholds = config["common"].get("thresholds", {})
    prices = config["common"].get("prices", {})

    field_runs = load_run(args.run, pulled_file=raw_file if raw_file.is_file() else None)
    if args.fields:
        field_runs = [fr for fr in field_runs if fr.field in set(args.fields)]
    if not field_runs:
        print("no search-field dumps found for this run", file=sys.stderr)
        raise SystemExit(1)

    # Cross-field overlap needs every field of a subject at once (the
    # field-axis defect is only visible by comparing a field's vocabulary
    # against its siblings'), so build the per-subject index first.
    by_subject: dict[str, dict[str, list]] = {}
    for fr in field_runs:
        by_subject.setdefault(fr.subject, {})[fr.field] = [
            w for w in fr.windows if w.round_index is None
        ]

    scorecards = []
    for fr in field_runs:
        field_cfg = config["fields"].get(fr.field, {})
        skipped: list[str] = []
        metrics: dict = {}

        have_phrases = any(w.phrases is not None for w in fr.windows)
        have_wire = any(w.domain is not None for w in fr.windows)
        if not have_phrases:
            skipped.append("all_content_metrics (no phrases: pre-2026-08-26 dump and no pull)")
        if have_phrases and not have_wire:
            skipped.append("verbatim/consistency/sweep/expectations (no wire text: run with --pull)")

        metrics["window_health"] = mechanical.window_metrics(fr.windows)
        metrics["degeneration"] = mechanical.degeneration_metrics(fr.windows)
        metrics["cost"] = mechanical.cost_metrics(fr, prices)
        if have_phrases:
            metrics["lengths"] = mechanical.length_metrics(fr.windows)
            metrics["duplicates"] = mechanical.duplicate_metrics(fr.windows)
        if have_phrases and have_wire:
            metrics["verbatim"] = mechanical.verbatim_metrics(fr.windows)
            metrics["consistency"] = mechanical.consistency_metrics(fr.windows)
            metrics["sweep"] = mechanical.sweep_metrics(
                fr.windows, field_cfg.get("sweeps") or []
            )
            metrics["merged_designations"] = mechanical.merged_designation_metrics(
                fr.windows, field_cfg.get("sweeps") or []
            )
            metrics["expectations"] = mechanical.expectation_metrics(
                fr.windows, load_expectations(fr.subject, fr.field)
            )
        if have_phrases:
            metrics["cross_field_overlap"] = mechanical.cross_field_overlap(
                by_subject.get(fr.subject, {}), fr.field
            )
        metrics["verdict"] = mechanical.verdict(metrics, thresholds)

        sc = write_scorecard(fr, metrics, skipped)
        append_history(sc)
        scorecards.append(sc)
        v = metrics["verdict"]
        print(
            f"{fr.subject:28} {fr.field:26} {v['status']:4} "
            f"{('; '.join(v['reds']) if v['reds'] else '')}"
        )

    summary = write_summary(args.run, scorecards)
    print(f"\nwrote {len(scorecards)} scorecards; summary: {summary}")


if __name__ == "__main__":
    main()
