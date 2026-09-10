"""Synthesis-stage evaluation CLI — the mechanical half of the protocol.

  checks/run_eval.py --run <run_id> [--pull]      mechanical pass + work orders
  checks/run_eval.py --run <run_id> --finalize    ingest verdicts + report

``--pull`` snapshots the run's synthesis wire evidence out of Mongo first
(needs the repo .env Mongo URI); without it a previous snapshot is reused.
The agent-judgment half between the two steps is driven by RUNBOOK.md.

Written for `python checks/run_eval.py` from the stage folder, matching the
sibling stage evals.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Optional

import yaml

if __package__ in (None, ""):  # invoked as a script, sibling-style
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from checks import designations, ledger, lints, loading, mechanical, pull, scorecard  # type: ignore
else:
    from . import designations, ledger, lints, loading, mechanical, pull, scorecard

EXPECTATIONS_ROOT = loading.EVAL_ROOT / "expectations"
TAXONOMY_FILE = loading.EVAL_ROOT / "TAXONOMY.md"

EVIDENCE_NOTE = (
    "evidence = the record's snippets exactly as the model received them "
    "(verbatim passages, no per-snippet location). The model ALSO read the "
    "whole chunk text (chunk_texts[<chunk_bounds>], a local file) with the "
    "instruction that it places the snippets but never adds claims — a claim "
    "traceable only to the chunk text is a containment breach (J1). "
    "locations = the code-derived heading / table header each snippet sits "
    "under (pointers into the chunk text; the model never saw these lines). "
    "designations_dropped = designation-shaped tokens present in the snippets "
    "but not found in the synthesis (a mechanical nomination for J2's "
    "designation clause — verify by reading)."
)


def load_expectations(subject: str, field_name: str) -> dict[str, Any]:
    """Subject identity + this field's probes, from expectations/<subject>/."""
    out: dict[str, Any] = {"probes": []}
    subject_file = EXPECTATIONS_ROOT / subject / "subject.yaml"
    if subject_file.is_file():
        out.update(yaml.safe_load(subject_file.read_text(encoding="utf-8")) or {})
    field_file = EXPECTATIONS_ROOT / subject / f"{field_name}.yaml"
    if field_file.is_file():
        field_doc = yaml.safe_load(field_file.read_text(encoding="utf-8")) or {}
        out["probes"] = field_doc.get("probes") or []
    return out


def taxonomy_version() -> str:
    """Hash of the judgment contract. Stamped into every verdict: editing
    TAXONOMY.md invalidates cached verdicts, which is the intent."""
    if not TAXONOMY_FILE.is_file():
        return "no-taxonomy"
    return hashlib.sha256(TAXONOMY_FILE.read_bytes()).hexdigest()[:12]


def _active_probes(expectations: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        p
        for p in expectations.get("probes") or []
        if p.get("status") in ("active", "predicted")
    ]


def prepare(run_id: str) -> dict[str, Any]:
    files = loading.dump_files(run_id)
    if not files:
        raise SystemExit(f"run {run_id}: no synthesis-field dumps found")
    evidence_index = pull.load_evidence_index(run_id)
    snapshot_names = pull.load_subject_names(run_id)
    records_by: dict[tuple[str, str], list[loading.SynthRecord]] = {}
    dumps: dict[tuple[str, str], dict[str, Any]] = {}
    for (subject, field_name), path in files.items():
        dump = loading.load_dump(path)
        dumps[(subject, field_name)] = dump
        records_by[(subject, field_name)] = list(
            loading.iter_records(subject, field_name, dump)
        )

    tax_version = taxonomy_version()
    summary: dict[str, Any] = {"run_id": run_id, "taxonomy_version": tax_version, "fields": {}}
    pending_dir = scorecard.history_dir(run_id) / "pending"
    pending_dir.mkdir(exist_ok=True)

    for (subject, field_name), records in sorted(records_by.items()):
        dump = dumps[(subject, field_name)]
        expectations = load_expectations(subject, field_name)
        subject_name = (
            expectations.get("subject_name")
            or snapshot_names.get(subject)
            or loading.subject_display_name(subject, run_id)
        )
        card: dict[str, Any] = {
            "run_id": run_id,
            "subject": subject,
            "field": field_name,
            "provenance": {
                "pv": loading.synthesis_pv(dump),
                "lint_versions": lints.lint_versions(),
                "taxonomy_version": tax_version,
                "evidence_snapshot": evidence_index is not None,
                "subject_name": subject_name,
                "subject_name_source": (
                    "expectations"
                    if expectations.get("subject_name")
                    else "wire" if subject in snapshot_names else "fallback"
                ),
                "method_stamp": "|".join(
                    [tax_version, *sorted(lints.lint_versions().values())]
                ),
            },
        }

        if field_name == loading.SHARED_DUPLICATE:
            # Byte-copy of products through synthesis: verify, don't re-score.
            card["invariants"] = [
                mechanical.contract_products_invariant(
                    records_by.get((subject, "products"), []), records
                )
            ]
            card["note"] = (
                "synthesis shared with products (one LLM call); metrics and "
                "judgments live on the products scorecard"
            )
        else:
            locations = loading.fold_locations(dump)
            designation_report = designations.report(
                records, evidence_index, pull.evidence_for
            )
            card["invariants"] = mechanical.run_invariants(
                subject, field_name, dump, records
            )
            card["metrics"] = mechanical.compute_metrics(
                subject,
                field_name,
                dump,
                records,
                evidence_index,
                subject_name,
                designation_report=designation_report,
            )
            card["candidates"] = mechanical.enumerate_candidates(
                records,
                evidence_index,
                expectations.get("third_party_roster") or [],
                locations=locations,
                designation_report=designation_report,
            )
            book = ledger.load_ledger(subject, field_name)
            pv = loading.synthesis_pv(dump)
            cached, pending = ledger.split_cached_pending(
                records, evidence_index, pv, tax_version, book
            )
            card["judgment_coverage"] = {
                "judgeable": len(cached) + len(pending),
                "judged": len(cached),
                "pending": len(pending),
            }
            card["judged_rates"] = ledger.judged_rates(cached, book)

            if pending:
                chunk_texts: dict[str, Optional[str]] = {}
                for r, _key in pending:
                    if r.chunk_bounds in chunk_texts or evidence_index is None:
                        continue
                    found = pull.evidence_for(evidence_index, r) or {}
                    path = pull.chunk_text_path(run_id, found.get("chunk_text"))
                    chunk_texts[r.chunk_bounds] = str(path) if path else None
                order = {
                    "run_id": run_id,
                    "subject": subject,
                    "subject_name": subject_name,
                    "field": field_name,
                    "pv": pv,
                    "taxonomy_version": tax_version,
                    "evidence_note": EVIDENCE_NOTE,
                    "chunk_texts": chunk_texts,
                    "probes": _active_probes(expectations),
                    "records": [
                        {
                            "content_key": key,
                            "chunk_bounds": r.chunk_bounds,
                            "group_id": r.group_id,
                            "focal_form": r.focal_form,
                            "forms": r.forms,
                            "retried": bool(r.retried),
                            "synthesis": r.synthesis,
                            **_evidence_fields(evidence_index, r),
                            "locations": locations.get((r.chunk_bounds, r.group_id)) or [],
                            "designations_dropped": (
                                (designation_report["per_record"].get(r.pair_key) or {})
                                .get("dropped")
                                or []
                            ),
                        }
                        for r, key in pending
                    ],
                }
                (pending_dir / f"{subject}__{field_name}.json").write_text(
                    # default=str: expectation YAML dates parse as datetime.date
                    json.dumps(order, indent=1, ensure_ascii=False, default=str),
                    encoding="utf-8",
                )

        scorecard.write_scorecard(run_id, subject, field_name, card)
        scorecard.append_scoreboard(run_id, subject, field_name, card)
        summary["fields"][f"{subject}/{field_name}"] = {
            "invariant_failures": sum(
                1 for i in card["invariants"] if i["status"] == "FAIL"
            ),
            **(
                {
                    "records": card["metrics"]["records"],
                    "pending_judgments": card["judgment_coverage"]["pending"],
                }
                if "metrics" in card
                else {"shared": True}
            ),
        }
    return summary


def _evidence_fields(
    evidence_index: Optional[dict[str, Any]], record: loading.SynthRecord
) -> dict[str, Any]:
    if evidence_index is None:
        return {"evidence": None, "evidence_sha256": None, "request_custom_id": None}
    found = pull.evidence_for(evidence_index, record) or {}
    return {
        "evidence": found.get("snippets"),
        "evidence_sha256": found.get("evidence_sha256"),
        # Which wire request carried it — the judge's co-packed context for J3
        # (the retry request when the record was retried).
        "request_custom_id": found.get("request_custom_id"),
    }


def finalize(run_id: str) -> dict[str, Any]:
    verdict_dir = scorecard.history_dir(run_id) / "verdicts"
    ingested = 0
    malformed = 0
    if verdict_dir.is_dir():
        by_target: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
        for path in sorted(verdict_dir.glob("*.jsonl")):
            for line in path.read_text(encoding="utf-8").splitlines():
                if not line.strip():
                    continue
                try:
                    row = json.loads(line)
                    by_target[(row["subject"], row["field"])].append(row)
                except Exception:
                    malformed += 1  # a killed agent can leave a torn last line
        for (subject, field_name), rows in by_target.items():
            ingested += ledger.append_verdicts(subject, field_name, rows)

    summary = prepare(run_id)  # absorb the fresh ledger state (idempotent)
    summary["verdicts_ingested"] = ingested
    summary["verdict_lines_malformed"] = malformed
    _write_report(run_id)
    return summary


def _write_report(run_id: str) -> Path:
    run_history = scorecard.history_dir(run_id)
    lines = [
        f"# Synthesis evaluation — run {run_id}",
        "",
        "Mechanical results below; judged rates cover the cached-verdict",
        "population (each scorecard's `judgment_coverage` carries the split).",
        "Trend view: `../../metrics_scoreboard.csv`. Method: `../../../README.md`,",
        "`../../../TAXONOMY.md`, `../../../RUNBOOK.md`.",
        "",
    ]
    for path in sorted(run_history.glob("*__*.json")):
        card = json.loads(path.read_text(encoding="utf-8"))
        fails = [i for i in card["invariants"] if i["status"] == "FAIL"]
        if "metrics" not in card:
            if fails:  # a contract_products byte-copy divergence is news
                lines.append(f"## {card['subject']} / {card['field']}")
                lines.append(
                    f"- invariants: {len(fails)} FAILED — "
                    + "; ".join(f"{f['id']}: {f['detail']}" for f in fails)
                )
                lines.append("")
            continue
        metrics = card["metrics"]
        coverage = card.get("judgment_coverage") or {}
        lines.append(f"## {card['subject']} / {card['field']}")
        lines.append(
            f"- invariants: {'ALL PASS' if not fails else f'{len(fails)} FAILED'}"
            + (f" — {[f['id'] for f in fails]}" if fails else "")
        )
        lines.append(
            f"- records {metrics['records']}, synthesized {metrics['synthesized']}, "
            f"retried {metrics.get('retried_records')}, "
            f"single-snippet share {metrics.get('single_snippet_share')}, "
            f"median chars {metrics.get('chars_per_record_median')}"
        )
        absent = metrics.get("focal_form_absent") or {}
        identical = metrics.get("identical_synthesis") or {}
        lines.append(
            f"- focal-form-absent {absent.get('flagged')}/"
            f"{absent.get('entity_shaped_records')} entity-shaped; "
            f"identical-synthesis clusters "
            f"{identical.get('clusters') if identical else 'n/a'}"
        )
        loc = metrics.get("location_coverage") or {}
        lines.append(
            f"- location coverage (watch) {loc.get('located')}/{loc.get('mentions')} "
            f"mentions located = {loc.get('coverage')}"
        )
        des = metrics.get("designation_preservation") or {}
        if des:
            lines.append(
                f"- designation preservation (proxy) tokens "
                f"{des.get('tokens_preserved')}/{des.get('tokens')} = {des.get('token_rate')}; "
                f"records fully preserved {des.get('records_fully_preserved')}/"
                f"{des.get('records_with_designations')} = {des.get('record_rate')}; "
                f"collapse-phrase records {des.get('records_with_collapse_phrase')}"
            )
        lines.append(
            f"- judged {coverage.get('judged', 0)}/{coverage.get('judgeable', 0)}"
            f" (pending {coverage.get('pending', 0)})"
        )
        for dimension, slot in (card.get("judged_rates") or {}).items():
            lines.append(
                f"  - {dimension}: {slot['fail']} fail / {slot['judged']} judged"
                f" (rate {slot['fail_rate']})"
            )
        lines.append("")
    report = run_history / "REPORT.md"
    report.write_text("\n".join(lines), encoding="utf-8")
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", help="run id (default: newest dump dir on disk)")
    parser.add_argument(
        "--pull",
        action="store_true",
        help="snapshot the run's synthesis wire evidence from Mongo first",
    )
    parser.add_argument(
        "--finalize",
        action="store_true",
        help="ingest history/runs/<run>/verdicts/*.jsonl, then re-score and report",
    )
    args = parser.parse_args()
    run_id = args.run or loading.latest_run()
    if not run_id:
        raise SystemExit("no runs on disk and --run not given")
    if args.pull:
        print(json.dumps(pull.build_snapshot(run_id), indent=2))
    result = finalize(run_id) if args.finalize else prepare(run_id)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
