"""The proposal-wave readout (2026-09-22): every proposal the first complete
Step 2 run made, one row per (proposal, record), for Sonnet judges.

    python sample_proposals.py <run>            # write out/judge_proposals/{sample.jsonl,packets/*.jsonl}
    python sample_proposals.py <run> --readout  # merge out/judge_proposals/verdicts/*.jsonl and print the tables

Each row carries the record as the model saw it (name, subject/focal form,
synthesis), the proposal, where it came from (grounding call, proposal pass,
descent under a parent, leaf step under a parent), the grounding quote, the
proposal wave's verdict (accepted with evidence + quote, or the rule it failed)
and mechanical hints: vocabulary labels that share words with the proposal
(a proposal that is a vocabulary label in disguise is a grounding miss no
guard can catch).
"""

from __future__ import annotations

import argparse
import collections
import json
import pathlib
import re
import sys

HERE = pathlib.Path(__file__).resolve().parent
ROOT = HERE.parents[5]
DUMPS = ROOT / "packages" / "logs" / "extraction_dumps"
OUT = HERE / "out" / "judge_proposals"
CONCEPT_FIELDS = ("industries", "material_caps", "process_caps", "conformity_attestations")
PACKET_ROWS = 110

sys.path.insert(0, str(HERE))
from run_grounding_tryout import load_concepts  # noqa: E402

_STOP = {"the", "of", "and", "for", "in", "a", "an", "to", "with", "on"}


def _tokens(label: str) -> set[str]:
    out = set()
    for t in re.findall(r"[a-z0-9]+", label.casefold()):
        if t in _STOP:
            continue
        out.add(t[:-1] if len(t) > 4 and t.endswith("s") else t)
    return out


def vocabulary_hints(field: str) -> "callable":
    concepts = load_concepts(field)
    labels: list[tuple[str, str, set[str]]] = []
    for c in concepts:
        for name in [c.name, *c.altLabels]:
            labels.append((name, c.name, _tokens(name)))

    def hints(label: str) -> list[str]:
        toks = _tokens(label)
        if not toks:
            return []
        scored = []
        for name, canonical, ltoks in labels:
            if not ltoks:
                continue
            inter = len(toks & ltoks)
            if inter == 0:
                continue
            j = inter / len(toks | ltoks)
            if j >= 0.5 or toks <= ltoks or ltoks <= toks:
                scored.append((j, name if name == canonical else f"{name} (= {canonical})"))
        return [n for _, n in sorted(scored, reverse=True)[:3]]

    return hints


def enumerate_rows(run: str) -> list[dict]:
    rows: list[dict] = []
    hint_fns = {}
    for f in sorted((DUMPS / run).glob("*.json")):
        doc = json.load(open(f))
        field = doc["field_type"]
        if field not in CONCEPT_FIELDS:
            continue
        if field not in hint_fns:
            hint_fns[field] = vocabulary_hints(field)
        subject = doc["subject_unique_id"]
        for chunk_bounds, chunk in doc["chunks"].items():
            trail = chunk.get("descent_trail")
            if not trail:
                continue
            by_id = {r["group_id"]: r for r in chunk["rows"]}
            for label, v in trail["proposal_wave"].items():
                for rid in v["records"]:
                    row = by_id.get(rid) or {}
                    rec = row.get("record") or {}
                    verdict = (row.get("proposal_wave") or {}).get(label) or {}
                    # the grounding-side quote for this proposal on this record
                    quote = ""
                    for key in ("proposals", "proposal_pass"):
                        block = row.get(key)
                        if isinstance(block, dict):
                            for rule in block.get("tags", {}).get(label, []):
                                if rule.get("outcome") in ("satisfied", "unverified") and rule.get("explanation"):
                                    quote = rule["explanation"]
                    if not quote:
                        for nodes in (row.get("descent_levels") or {}).values():
                            for n in nodes:
                                for k in ("descent", "leaf_step"):
                                    q = ((n.get(k) or {}).get("proposed") or {}).get(label)
                                    if q:
                                        quote = q
                    rows.append({
                        "item_id": "",
                        "subject": subject, "field": field, "chunk": chunk_bounds,
                        "record": rid, "focal_form": rec.get("focal_form") or row.get("focal_form"),
                        "synthesis": rec.get("synthesis"),
                        "label": label, "sources": v["sources"],
                        "grounding_quote": quote,
                        "accepted": rid in v["accepted"],
                        "evidence": verdict.get("evidence"),
                        "failed_rule": verdict.get("failed_rule"),
                        "screening_quote": verdict.get("quote"),
                        "vocabulary_hints": hint_fns[field](label),
                    })
    rows.sort(key=lambda r: (r["field"], r["subject"], r["label"].casefold(), r["record"]))
    for i, r in enumerate(rows):
        r["item_id"] = f"p_{i:04d}"
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
    names = []
    for field, frows in by_field.items():
        for k in range(0, len(frows), PACKET_ROWS):
            name = f"packet_{field}_{k // PACKET_ROWS}"
            with open(OUT / "packets" / f"{name}.jsonl", "w") as fh:
                for r in frows[k:k + PACKET_ROWS]:
                    fh.write(json.dumps(r, ensure_ascii=False) + "\n")
            names.append((name, len(frows[k:k + PACKET_ROWS])))
    for name, n in names:
        print(f"{name}: {n} rows")
    print(f"{len(rows)} rows, {len(names)} packets → {OUT}")


def readout(rows: list[dict]) -> None:
    verdicts: dict[str, dict] = {}
    for f in sorted((OUT / "verdicts").glob("*.jsonl")):
        for line in open(f):
            line = line.strip()
            if line:
                v = json.loads(line)
                verdicts[v["item_id"]] = v
    by_id = {r["item_id"]: r for r in rows}
    judged = [(by_id[i], v) for i, v in verdicts.items() if i in by_id]
    print(f"judged {len(judged)} of {len(rows)} rows\n")

    def table(title, keyfn):
        print(f"## {title}")
        groups = collections.defaultdict(collections.Counter)
        for r, v in judged:
            groups[keyfn(r)][v.get("code", "?")] += 1
        codes = sorted({c for g in groups.values() for c in g})
        print("| group | rows | " + " | ".join(codes) + " |")
        print("|---|---|" + "---|" * len(codes))
        for g, c in sorted(groups.items()):
            n = sum(c.values())
            print(f"| {g} | {n} | " + " | ".join(f"{c[k]} ({100*c[k]/n:.0f}%)" for k in codes) + " |")
        print()

    table("code by field", lambda r: r["field"])
    table("code by source", lambda r: ",".join(sorted({s.split(':')[0] for s in r["sources"]})))
    table("code by the proposal wave's verdict", lambda r: "accepted" if r["accepted"] else f"rejected {r['failed_rule']}")
    print("## screening verdict right? (screen_ok)")
    ok = collections.Counter((("accepted" if r["accepted"] else "rejected"), v.get("screen_ok")) for r, v in judged)
    for k, n in sorted(ok.items()):
        print(f"  {k[0]:9s} screen_ok={k[1]}: {n}")
    print("\n## vocabulary labels in disguise (code L), by named label")
    lab = collections.Counter((r["field"], v.get("vocab_label") or "?") for r, v in judged if v.get("code") == "L")
    for (field, label), n in lab.most_common(25):
        print(f"  {field:24s} {label!r}: {n}")
    print("\n## unsettled rows (code U or flagged)")
    for r, v in judged:
        if v.get("code") == "U" or v.get("unsettled"):
            print(f"  {r['item_id']} {r['field']} {r['label']!r} — {v.get('note', '')[:160]}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("run")
    ap.add_argument("--readout", action="store_true")
    args = ap.parse_args()
    rows = enumerate_rows(args.run)
    if args.readout:
        readout(rows)
    else:
        write_packets(rows)
