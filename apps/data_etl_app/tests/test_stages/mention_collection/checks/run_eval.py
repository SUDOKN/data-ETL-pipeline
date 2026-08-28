"""CLI for the mention-stage evaluation.

    python checks/run_eval.py --run <run_id>       # Phase A: deterministic pass
    python checks/run_eval.py --run <run_id> --finalize   # Phase C: ingest verdicts

Phase A reads the dumps, runs every deterministic check, writes one scorecard
per (subject, field), appends the cross-run trend, and emits **work orders** —
the exhaustive list of items still needing a reader, with everything a judge
needs inlined. Phase C ingests the verdict files those judges wrote and rescores.

Nothing here dispatches an LLM request. The judging is done by agents following
RUNBOOK.md; this module only prepares their work and folds in their answers.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

if __package__ in (None, ""):  # pragma: no cover - script invocation
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import goldens  # type: ignore[import-not-found]
    import ledger  # type: ignore[import-not-found]
    import loading  # type: ignore[import-not-found]
    import mechanical  # type: ignore[import-not-found]
    import paths  # type: ignore[import-not-found]
else:
    from . import goldens, ledger, loading, mechanical, paths


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _config_digest(run: loading.FieldRun) -> str:
    """What makes two runs comparable at all.

    Chunk geometry, the normalizer and the snippet radius all change which
    mentions exist; the prompt version changes what the locations say. A trend
    line that mixes two digests is comparing different questions.
    """
    stage = run.metadata.get(paths.STAGE_REQUEST_TOKEN) or {}
    return paths.digest(
        {
            "chunk_strat": run.metadata.get("chunk_strat"),
            "aggregation_fold": run.metadata.get("aggregation_fold"),
            "pv": stage.get("prompt_version_id"),
            "max_mentions_per_request": stage.get("max_mentions_per_request"),
            "snippet_radius": stage.get("snippet_radius"),
        }
    )


def build_work_orders(
    run: loading.FieldRun, taxonomy_version: str
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Every judgeable item of one field, split into cached and pending.

    Exhaustive by construction: every distinct snippet, every group, every
    nesting pair. Sampling is not an option the harness offers.
    """
    stage = run.metadata.get(paths.STAGE_REQUEST_TOKEN) or {}
    pv = stage.get("prompt_version_id")
    nominations = mechanical.nominate_for_judgment(run)
    repeated = set(nominations["repeated_snippet"])
    restating = set(nominations["restatement"])

    counts: dict[str, int] = {}
    for mention in run.mentions():
        counts[mention.mention_id] = counts.get(mention.mention_id, 0) + 1

    items: list[tuple[str, dict[str, Any]]] = []

    for mention_id, mention in run.distinct_snippets().items():
        annotations = []
        if mention_id in repeated:
            annotations.append("repeated_line")
        if mention_id in restating:
            annotations.append("restatement")
        if not mention.described:
            annotations.append("not_described")
        key = ledger.content_key(
            item=ledger.ITEM_SNIPPET,
            identity=mention_id,
            judged_content=mention.snippet + "\x1e" + mention.location,
            pv=pv,
            taxonomy_version=taxonomy_version,
        )
        items.append(
            (
                key,
                {
                    "item": ledger.ITEM_SNIPPET,
                    "mention_id": mention_id,
                    "group_id": mention.group_id,
                    "group_key": mention.group_key,
                    "form": mention.form,
                    "page": mention.page,
                    "occurrences": counts.get(mention_id, 0),
                    "snippet": mention.snippet,
                    "location": mention.location,
                    "location_source": mention.location_source,
                    "annotations": annotations,
                    "dimensions": ["S1", "S2", "S3", "S4", "S5"],
                },
            )
        )

    for group in run.groups:
        key = ledger.content_key(
            item=ledger.ITEM_GROUP,
            identity=group.group_id,
            judged_content=json.dumps(
                [sorted(group.forms), group.status, group.key], sort_keys=True
            ),
            pv=pv,
            taxonomy_version=taxonomy_version,
        )
        items.append(
            (
                key,
                {
                    "item": ledger.ITEM_GROUP,
                    "group_id": group.group_id,
                    "group_key": group.key,
                    "forms": list(group.forms),
                    "status": group.status,
                    "collapsed_into": group.collapsed_into,
                    "mention_count": group.mention_count,
                    "distinct_snippets": group.distinct_snippets,
                    "chunk_bounds": group.chunk_bounds,
                    "dimensions": ["G1", "G2", "G3", "G4"],
                },
            )
        )

    for inner, outer in run.nested_mentions():
        identity = f"{inner.group_id}:{inner.span[0]}:{inner.span[1]}:{outer.group_id}"
        key = ledger.content_key(
            item=ledger.ITEM_INHERITANCE,
            identity=identity,
            judged_content=inner.snippet + "\x1e" + inner.form + "\x1e" + outer.form,
            pv=pv,
            taxonomy_version=taxonomy_version,
        )
        items.append(
            (
                key,
                {
                    "item": ledger.ITEM_INHERITANCE,
                    "inner_form": inner.form,
                    "inner_group_id": inner.group_id,
                    "inner_group_key": inner.group_key,
                    "outer_form": outer.form,
                    "outer_group_id": outer.group_id,
                    "snippet": inner.snippet,
                    "location": inner.location,
                    "dimensions": ["I1"],
                },
            )
        )

    return ledger.split_cached_pending(items, ledger.load_ledger(run.subject, run.field))


def prepare(run_id: str, dumps_root: Optional[Path] = None) -> dict[str, Any]:
    runs = loading.load_run(run_id, dumps_root)
    reports = mechanical.evaluate_run(runs)
    taxonomy_version = paths.taxonomy_version()

    out_dir = paths.run_dir(run_id)
    (out_dir / "pending").mkdir(parents=True, exist_ok=True)
    paths.HISTORY_ROOT.mkdir(parents=True, exist_ok=True)

    summary: dict[str, Any] = {
        "run_id": run_id,
        "generated_at": _now(),
        "taxonomy_version": taxonomy_version,
        "fields": {},
        "totals": {"pending": 0, "cached": 0, "judgeable": 0},
    }
    trend_rows: list[dict[str, Any]] = []

    for run in runs:
        report = reports[run.key]
        record: dict[str, Any] = {
            **report.as_dict(),
            "run_id": run_id,
            "config_digest": _config_digest(run),
        }

        # The golden corpus is run-independent, so it grades any run the moment
        # a subject is labelled. Only `confirmed` labels gate; a must-not-occur
        # violation is exact (the form is absent from the text, so collecting it
        # is a boundary defect), while an over-count is checked leniently
        # because the pipeline reads a trimmed, capped copy of the snapshot.
        if run.has_fold:
            field_labels = goldens.load_subject_goldens(run.subject).get(run.field, [])
            if field_labels:
                findings, golden_metrics = goldens.check_occurrences(run, field_labels)
                record["golden"] = {
                    **golden_metrics,
                    "findings": [f.as_dict() for f in findings],
                }
                report.metrics.update(golden_metrics)
                if findings:
                    report.findings.append(
                        mechanical.Finding(
                            "golden.occurrences",
                            mechanical.GATE_TRIPWIRE,
                            False,
                            "collected occurrences contradict the golden corpus",
                            len(findings),
                            [f"{f.form}: expected {f.expected}, got {f.actual}"
                             for f in findings],
                        )
                    )
                    record["status"] = report.status
                    record["reds"] = report.reds

        if run.has_fold and run.judged:
            cached, pending = build_work_orders(run, taxonomy_version)
            record["judgment_coverage"] = {
                "judgeable": len(cached) + len(pending),
                "judged": len(cached),
                "pending": len(pending),
            }
            if pending:
                (out_dir / "pending" / f"{run.key}.json").write_text(
                    json.dumps(
                        {
                            "run_id": run_id,
                            "subject": run.subject,
                            "field": run.field,
                            "taxonomy_version": taxonomy_version,
                            "items": pending,
                        },
                        indent=2,
                        ensure_ascii=False,
                    ),
                    encoding="utf-8",
                )
            record["judged_rates"] = ledger.judged_rates(
                ledger.load_ledger(run.subject, run.field),
                [row["content_key"] for row in cached],
            )
            summary["totals"]["pending"] += len(pending)
            summary["totals"]["cached"] += len(cached)
            summary["totals"]["judgeable"] += len(cached) + len(pending)

        (out_dir / f"{run.key}.json").write_text(
            json.dumps(record, indent=2, ensure_ascii=False), encoding="utf-8"
        )
        summary["fields"][run.key] = {
            "status": report.status,
            "reds": report.reds,
            "pending": record.get("judgment_coverage", {}).get("pending"),
        }
        trend_rows.append(
            {
                "run_id": run_id,
                "subject": run.subject,
                "field": run.field,
                "generated_at": summary["generated_at"],
                "config_digest": record["config_digest"],
                "taxonomy_version": taxonomy_version,
                "status": report.status,
                "reds": report.reds,
                "metrics": report.metrics,
                "judged_rates": record.get("judged_rates"),
            }
        )

    with paths.METRICS_FILE.open("a", encoding="utf-8") as handle:
        for row in trend_rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")

    (out_dir / "SUMMARY.md").write_text(_summary_markdown(summary, reports), encoding="utf-8")
    return summary


def _summary_markdown(
    summary: dict[str, Any], reports: dict[str, mechanical.FieldReport]
) -> str:
    lines = [
        f"# Mention-stage eval — run `{summary['run_id']}`",
        "",
        f"Generated {summary['generated_at']} · taxonomy `{summary['taxonomy_version']}`",
        "",
        "REGENERATED BY EVERY MECHANICAL PASS — write analysis in FINDINGS.md, not here.",
        "",
        "| field | status | groups | mentions | snippets | nested | >50 entries | pending | reds |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    for key in sorted(reports):
        report = reports[key]
        m = report.metrics
        pending = summary["fields"].get(key, {}).get("pending")
        lines.append(
            f"| {key} | {report.status} | {m.get('groups', '-')} | "
            f"{m.get('mentions', '-')} | {m.get('distinct_snippets_global', '-')} | "
            f"{m.get('nested_occurrences', '-')} | {m.get('groups_over_50_entries', '-')} | "
            f"{pending if pending is not None else '-'} | "
            f"{', '.join(report.reds) or '—'} |"
        )
    totals = summary["totals"]
    lines += [
        "",
        f"**Judgment coverage:** {totals['cached']} judged / {totals['judgeable']} "
        f"judgeable, {totals['pending']} pending.",
        "",
        "`BLIND` = the dump carries no fold block, so this instrument cannot see "
        "the stage (every full-run dump before 2026-08-27). Not a failure.",
    ]
    return "\n".join(lines) + "\n"


def finalize(run_id: str, dumps_root: Optional[Path] = None) -> dict[str, Any]:
    """Ingest `history/runs/<run>/verdicts/*.jsonl`, then re-score."""
    verdict_dir = paths.run_dir(run_id) / "verdicts"
    ingested = malformed = skipped = 0
    if verdict_dir.is_dir():
        for path in sorted(verdict_dir.glob("*.jsonl")):
            stem = path.stem.split("__part")[0]
            slug, _, field = stem.partition("__")
            rows: list[dict[str, Any]] = []
            for line in path.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError:
                    malformed += 1
            written, was_skipped = ledger.append_verdicts(
                paths.subject_of_slug(slug), field, rows
            )
            ingested += written
            skipped += was_skipped
    result = prepare(run_id, dumps_root)
    result["verdicts_ingested"] = ingested
    result["verdict_lines_malformed"] = malformed
    result["verdicts_already_present"] = skipped
    return result


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", dest="run_id", default=None,
                        help="run id (default: newest dump directory on disk)")
    parser.add_argument("--finalize", action="store_true",
                        help="ingest this run's verdict files, then re-score")
    parser.add_argument("--dumps-root", type=Path, default=None)
    args = parser.parse_args(argv)

    run_id = args.run_id or loading.latest_run(args.dumps_root)
    if not run_id:
        parser.error("no run id given and no dumps on disk")

    result = finalize(run_id, args.dumps_root) if args.finalize else prepare(
        run_id, args.dumps_root
    )
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
