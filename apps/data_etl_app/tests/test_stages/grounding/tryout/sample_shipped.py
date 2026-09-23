"""The shipped-tag census of a Step 2 run (2026-09-22): every (record, label)
pair the reconcile step shipped — the concept fields' in-vocabulary labels
(with the record's descent path) and the keyword fields' accepted candidates —
one row each, for Sonnet judges coding the harness TAXONOMY.

    python sample_shipped.py <run>            # out/judge_shipped/{sample.jsonl,packets/*.jsonl}
    python sample_shipped.py <run> --readout  # merge out/judge_shipped/verdicts/*.jsonl, print the tables
"""

from __future__ import annotations

import argparse
import collections
import json
import pathlib

HERE = pathlib.Path(__file__).resolve().parent
ROOT = HERE.parents[5]
DUMPS = ROOT / "packages" / "logs" / "extraction_dumps"
OUT = HERE / "out" / "judge_shipped"
CONCEPT_FIELDS = ("industries", "material_caps", "process_caps", "conformity_attestations")
KEYWORD_FIELDS = ("products", "contract_products", "equipments")
PACKET_ROWS = 110
CLEAN = ("D", "N")


def _path_of(row: dict, label: str) -> list[dict]:
    """The record's descent nodes, wave by wave, up to and including the
    shipped label — what the judge reads for the hop codes."""
    path = []
    for wave, nodes in sorted((row.get("descent_levels") or {}).items(), key=lambda kv: int(kv[0])):
        for n in nodes:
            v = n.get("verdict")
            entry = {"wave": int(wave), "label": n["label"], "verdict": ("accepted" if v["passed"] else f"rejected {v.get('failed_rule')}") if v else ("removed under failed ancestor" if n.get("removed_under_failed_ancestor") else None)}
            if n.get("descent"):
                entry["descent"] = n["descent"]
            if n.get("leaf_step"):
                entry["leaf_step"] = n["leaf_step"]
            if n.get("false_children"):
                entry["false_children"] = n["false_children"]
            path.append(entry)
    return path


def enumerate_rows(run: str) -> list[dict]:
    rows: list[dict] = []
    for f in sorted((DUMPS / run).glob("*.json")):
        doc = json.load(open(f))
        field, subject = doc["field_type"], doc["subject_unique_id"]
        for chunk_bounds, chunk in doc["chunks"].items():
            for row in chunk.get("rows", []):
                rec = row.get("record") or {}
                base = {"subject": subject, "field": field, "chunk": chunk_bounds, "record": row["group_id"],
                        "focal_form": rec.get("focal_form") or row.get("focal_form"), "synthesis": rec.get("synthesis")}
                if field in CONCEPT_FIELDS:
                    for label in (row.get("shipped") or {}).get("in_vocab", []):
                        accepting = None
                        for nodes in (row.get("descent_levels") or {}).values():
                            for n in nodes:
                                if n["label"] == label and n.get("verdict", {}) and n["verdict"].get("passed"):
                                    accepting = n["verdict"]
                        grounding_tags = list(((row.get("grounding") or {}).get("tags") or {}).keys())
                        rows.append({**base, "kind": "concept", "label": label,
                                     "grounding_labels": grounding_tags,
                                     "path": _path_of(row, label),
                                     "evidence": (accepting or {}).get("evidence"), "screening_quote": (accepting or {}).get("quote")})
                elif field in KEYWORD_FIELDS:
                    for cand, v in (row.get("screening") or {}).items():
                        if v.get("passed"):
                            rules = ((row.get("freehand_grounding") or {}).get("tags") or {}).get(cand, [])
                            rows.append({**base, "kind": "keyword", "label": cand,
                                         "grounding_explanation": next((r.get("explanation") for r in rules if r.get("explanation")), None),
                                         "evidence": v.get("evidence"), "screening_quote": v.get("quote")})
    rows.sort(key=lambda r: (r["field"], r["subject"], r["record"], r["label"].casefold()))
    for i, r in enumerate(rows):
        r["item_id"] = f"s_{i:04d}"
    return rows


def write_packets(rows: list[dict]) -> None:
    (OUT / "packets").mkdir(parents=True, exist_ok=True)
    (OUT / "verdicts").mkdir(parents=True, exist_ok=True)
    with open(OUT / "sample.jsonl", "w") as fh:
        for r in rows:
            fh.write(json.dumps(r, ensure_ascii=False) + "\n")
    by_field = collections.defaultdict(list)
    for r in rows:
        by_field[r["field"]].append(r)
    for field, frows in by_field.items():
        for k in range(0, len(frows), PACKET_ROWS):
            name = f"packet_{field}_{k // PACKET_ROWS}"
            with open(OUT / "packets" / f"{name}.jsonl", "w") as fh:
                for r in frows[k:k + PACKET_ROWS]:
                    fh.write(json.dumps(r, ensure_ascii=False) + "\n")
            print(f"{name}: {len(frows[k:k + PACKET_ROWS])} rows")
    print(f"{len(rows)} rows → {OUT}")


def readout(rows: list[dict]) -> None:
    V: dict[str, dict] = {}
    for f in sorted((OUT / "verdicts").glob("*.jsonl")):
        for line in open(f):
            if line.strip():
                v = json.loads(line)
                V[v["item_id"]] = v
    by_id = {r["item_id"]: r for r in rows}
    judged = [(by_id[i], v) for i, v in V.items() if i in by_id]
    print(f"judged {len(judged)} of {len(rows)} rows\n")
    codes = ["D", "N", "X", "V", "B", "P", "F", "U"]
    print("## tag codes by field (clean = D + N)")
    print("| field | tags | clean | defective | " + " | ".join(codes) + " |")
    print("|---|---|---|---|" + "---|" * len(codes))
    groups = collections.defaultdict(collections.Counter)
    for r, v in judged:
        groups[r["field"]][v.get("code", "U")] += 1
        groups["ALL"][v.get("code", "U")] += 1
    for g, c in sorted(groups.items(), key=lambda kv: (kv[0] != "ALL", kv[0])):
        n = sum(c.values()); clean = c["D"] + c["N"]
        print(f"| {g} | {n} | {clean} ({100*clean/n:.1f}%) | {n-clean} ({100*(n-clean)/n:.1f}%) | " + " | ".join(str(c[k]) for k in codes) + " |")
    print("\n## industries sub-kinds:", dict(collections.Counter(v.get("sub_kind") for r, v in judged if r["field"] == "industries" and v.get("code") == "X")))
    print("\n## descent hops (concept rows shipped below wave 1)")
    hops = collections.Counter(v.get("hop") for r, v in judged if r["kind"] == "concept" and v.get("hop") not in (None, "none"))
    n = sum(hops.values())
    print({k: f"{c} ({100*c/max(1,n):.0f}%)" for k, c in hops.items()}, "| hop defect share:", f"{100*(hops['H-BRIDGE']+hops['H-FORCED']+hops['H-STOP'])/max(1,n):.1f}%")
    print("\n## by evidence label (named / inferred) — clean share")
    ev = collections.defaultdict(collections.Counter)
    for r, v in judged:
        ev[r.get("evidence")][ "clean" if v.get("code") in CLEAN else "defective"] += 1
    for e, c in ev.items():
        n = sum(c.values()); print(f"  {e}: {c['clean']}/{n} clean ({100*c['clean']/n:.0f}%)")
    print("\n## unsettled rows")
    for r, v in judged:
        if v.get("unsettled") or v.get("code") == "U":
            print(f"  {r['item_id']} {r['field']} {r['label']!r} — {v.get('note','')[:150]}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("run")
    ap.add_argument("--readout", action="store_true")
    a = ap.parse_args()
    rows = enumerate_rows(a.run)
    readout(rows) if a.readout else write_packets(rows)
