"""Mechanical readout of the grounding-shape tryout outputs (no judgment).

Per target and arm, over the N repeats: whether every response parsed under
its schema; coverage of the sent records (arm b: every sent id in at least one
supporting list or in ``unmatched``, no unknown ids, no id both supported and
unmatched); membership (arm b: matched options that are vocabulary labels
after decoration strip and casing repair; proposals that are labels); per-record
label maps and their STABILITY across repeats (the grounding floor in each
shape, as the mean pairwise Jaccard over (record, label) pairs and the share of
records whose label set is identical in every repeat); citation completeness
(arm b: the share of supporting record ids that the evidence explanation
quotes); and the label-set overlap between the arms' modes. What needs reading
— multi-record inference, proposal quality, evidence fidelity — goes to judges.

Usage (from the repo root):
    .venv/bin/python apps/data_etl_app/tests/test_stages/grounding/tryout/check_tryout.py [--targets tag ...]
"""

from __future__ import annotations

import argparse
import itertools
import json
import pathlib
import re
import sys
from collections import Counter, defaultdict
from typing import Any, Optional

HERE = pathlib.Path(__file__).resolve().parent
OUT = HERE / "out"
sys.path.insert(0, str(HERE))
from run_grounding_tryout import FIELD_PREFIX, load_concepts  # noqa: E402

DECORATION_RE = re.compile(r"\s*\(also:.*\)\s*$")


def _labels(field: str) -> dict[str, str]:
    """casefold label -> canonical spelling, names and altLabels alike."""
    out: dict[str, str] = {}
    for concept in load_concepts(field):
        for label in [concept.name, *concept.altLabels]:
            out[label.casefold()] = label
    return out


def _canonical(option: str, labels: dict[str, str]) -> Optional[str]:
    stripped = DECORATION_RE.sub("", option).strip()
    return labels.get(stripped.casefold())


def parse_arm_a(iv_text: str, oov_text: str, labels: dict[str, str]) -> dict[str, Any]:
    """Today's shape: record-keyed entries; the OOV pass minted labels."""
    per_record: dict[str, set[str]] = defaultdict(set)
    proposals: set[str] = set()
    drops = 0
    for text, minted in ((iv_text, False), (oov_text, True)):
        doc = json.loads(text)
        for entry in doc.get("groundings") or []:
            rid = entry["record_id"]
            per_record.setdefault(rid, set())
            for unit in entry.get("options") or entry.get("candidates") or []:
                label = unit.get("option") or unit.get("candidate")
                if minted:
                    canon = _canonical(label, labels)
                    if canon:
                        per_record[rid].add(canon)
                    else:
                        per_record[rid].add(f"OOV:{label}")
                        proposals.add(label)
                else:
                    canon = _canonical(label, labels)
                    if canon is None:
                        drops += 1
                    else:
                        per_record[rid].add(canon)
    return {"per_record": {k: sorted(v) for k, v in per_record.items()}, "proposals": sorted(proposals), "drops": drops, "answered": len(per_record)}


def parse_arm_b(text: str, sent: list[str], labels: dict[str, str]) -> dict[str, Any]:
    doc = json.loads(text)
    per_record: dict[str, set[str]] = {rid: set() for rid in sent}
    unknown: set[str] = set()
    supported: set[str] = set()
    unmatched: set[str] = set()
    drops: list[str] = []
    folded_proposals: list[str] = []
    proposals: list[str] = []
    citation_total = citation_hit = 0
    options_seen: Counter[str] = Counter()
    for entry in doc.get("matched") or []:
        canon = _canonical(entry["option"], labels)
        options_seen[(canon or entry["option"]).casefold()] += 1
        explanation = (entry.get("IGR-E1") or {}).get("explanation", "")
        for rid in entry.get("supporting_records") or []:
            if rid not in per_record:
                unknown.add(rid)
                continue
            supported.add(rid)
            citation_total += 1
            citation_hit += int(rid in explanation)
            if canon is None:
                drops.append(entry["option"])
            else:
                per_record[rid].add(canon)
    for entry in doc.get("proposed") or []:
        canon = _canonical(entry["label"], labels)
        for rid in entry.get("supporting_records") or []:
            if rid not in per_record:
                unknown.add(rid)
                continue
            supported.add(rid)
            if canon:
                folded_proposals.append(entry["label"])
                per_record[rid].add(canon)
            else:
                proposals.append(entry["label"])
                per_record[rid].add(f"OOV:{entry['label']}")
    for entry in doc.get("unmatched") or []:
        rid = entry["record_id"]
        if rid not in per_record:
            unknown.add(rid)
            continue
        unmatched.add(rid)
    covered = supported | unmatched
    missing = [rid for rid in sent if rid not in covered]
    contradictions = sorted(supported & unmatched)
    return {
        "per_record": {k: sorted(v) for k, v in per_record.items()},
        "proposals": sorted(set(proposals)),
        "proposals_folded_into_vocabulary": sorted(set(folded_proposals)),
        "drops": drops,
        "coverage": {"missing": missing, "unknown": sorted(unknown), "contradictions": contradictions},
        "duplicate_options": sorted(k for k, n in options_seen.items() if n > 1),
        "citation": {"total": citation_total, "hit": citation_hit},
        "matched_entries": len(doc.get("matched") or []),
        "unmatched_entries": len(unmatched),
    }


def _pairs(per_record: dict[str, list[str]]) -> set[tuple[str, str]]:
    return {(rid, label) for rid, labels in per_record.items() for label in labels}


def stability(runs: list[dict[str, Any]], sent: list[str]) -> dict[str, Any]:
    if len(runs) < 2:
        return {"pairs": 0}
    jaccards: list[float] = []
    for a, b in itertools.combinations(runs, 2):
        pa, pb = _pairs(a["per_record"]), _pairs(b["per_record"])
        jaccards.append(len(pa & pb) / len(pa | pb) if (pa | pb) else 1.0)
    identical_records = sum(
        1 for rid in sent if len({tuple(r["per_record"].get(rid, [])) for r in runs}) == 1
    )
    return {"pairs": len(jaccards), "mean_jaccard": round(sum(jaccards) / len(jaccards), 3), "records_identical_in_every_repeat": identical_records, "records": len(sent)}


def mode_labels(runs: list[dict[str, Any]]) -> dict[str, list[str]]:
    """Per record, the labels present in a majority of repeats."""
    counts: dict[str, Counter[str]] = defaultdict(Counter)
    for r in runs:
        for rid, labels in r["per_record"].items():
            for label in labels:
                counts[rid][label] += 1
    need = len(runs) / 2
    return {rid: sorted(l for l, n in c.items() if n > need) for rid, c in counts.items()}


def check_target(tag: str) -> dict[str, Any]:
    d = OUT / tag
    meta = json.loads((d / "request_meta.json").read_text())
    field, sent = meta["field"], meta["record_ids"]
    labels = _labels(field)
    report: dict[str, Any] = {"tag": tag, "field": field, "records": len(sent), "arms": {}}
    runs_a: list[dict[str, Any]] = []
    runs_b: list[dict[str, Any]] = []
    parse_failures: Counter[str] = Counter()
    for k in itertools.count():
        iv, oov = d / f"a_iv_{k}.json", d / f"a_oov_{k}.json"
        if not (iv.exists() and oov.exists()):
            break
        try:
            runs_a.append(parse_arm_a(iv.read_text(), oov.read_text(), labels))
        except Exception as exc:  # a parse failure is a finding, not a crash
            parse_failures["a"] += 1
            print(f"{tag} a#{k}: parse failure {exc}", file=sys.stderr)
    for k in itertools.count():
        b = d / f"b_{k}.json"
        if not b.exists():
            break
        try:
            runs_b.append(parse_arm_b(b.read_text(), sent, labels))
        except Exception as exc:
            parse_failures["b"] += 1
            print(f"{tag} b#{k}: parse failure {exc}", file=sys.stderr)
    if runs_a:
        report["arms"]["a"] = {
            "repeats": len(runs_a),
            "parse_failures": parse_failures["a"],
            "labels_per_repeat": [sum(len(v) for v in r["per_record"].values()) for r in runs_a],
            "proposals_per_repeat": [len(r["proposals"]) for r in runs_a],
            "drops_per_repeat": [r["drops"] for r in runs_a],
            "stability": stability(runs_a, sent),
        }
    if runs_b:
        cov = [r["coverage"] for r in runs_b]
        cit_total = sum(r["citation"]["total"] for r in runs_b)
        cit_hit = sum(r["citation"]["hit"] for r in runs_b)
        report["arms"]["b"] = {
            "repeats": len(runs_b),
            "parse_failures": parse_failures["b"],
            "labels_per_repeat": [sum(len(v) for v in r["per_record"].values()) for r in runs_b],
            "matched_entries_per_repeat": [r["matched_entries"] for r in runs_b],
            "unmatched_per_repeat": [r["unmatched_entries"] for r in runs_b],
            "proposals_per_repeat": [len(r["proposals"]) for r in runs_b],
            "proposals_folded_per_repeat": [len(r["proposals_folded_into_vocabulary"]) for r in runs_b],
            "drops_per_repeat": [len(r["drops"]) for r in runs_b],
            "coverage_violations": {
                "repeats_with_missing": sum(1 for c in cov if c["missing"]),
                "missing_ids_total": sum(len(c["missing"]) for c in cov),
                "unknown_ids_total": sum(len(c["unknown"]) for c in cov),
                "contradictions_total": sum(len(c["contradictions"]) for c in cov),
            },
            "duplicate_option_entries": sum(len(r["duplicate_options"]) for r in runs_b),
            "citation_completeness": round(cit_hit / cit_total, 3) if cit_total else None,
            "stability": stability(runs_b, sent),
        }
    if runs_a and runs_b:
        ma, mb = mode_labels(runs_a), mode_labels(runs_b)
        pa, pb = _pairs(ma), _pairs(mb)
        report["mode_overlap"] = {
            "a_only": sorted(f"{r}:{l}" for r, l in pa - pb),
            "b_only": sorted(f"{r}:{l}" for r, l in pb - pa),
            "both": len(pa & pb),
        }
    return report


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--targets", nargs="*", default=[])
    args = ap.parse_args()
    tags = args.targets or sorted(p.name for p in OUT.iterdir() if p.is_dir())
    reports = [check_target(t) for t in tags]
    (OUT / "CHECK.json").write_text(json.dumps(reports, indent=1))
    lines = ["# Grounding-shape tryout — mechanical check", ""]
    for r in reports:
        lines.append(f"## {r['tag']} ({r['field']}, {r['records']} records)")
        for arm, a in r["arms"].items():
            lines.append(f"- arm {arm}: {json.dumps(a)}")
        if "mode_overlap" in r:
            lines.append(f"- mode overlap: both {r['mode_overlap']['both']}, a-only {len(r['mode_overlap']['a_only'])}, b-only {len(r['mode_overlap']['b_only'])}")
        lines.append("")
    (OUT / "CHECK.md").write_text("\n".join(lines) + "\n")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
