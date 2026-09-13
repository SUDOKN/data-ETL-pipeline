"""Phase 2 sample for the answerability baseline (design: SYNTHESIS_FIELD_REQUIREMENTS_2026-09-11.md §7).

Joins run 20260911T003500's judged work orders to their verdicts and writes, per field,
`sample_<field>.jsonl` = every failing record + random passing records to reach TARGET,
passes stratified by the judged population. Verdict dimensions are NOT written into the
sample (the agents grade axes independently); they go to `verdict_index.jsonl` for the readout join.
contract_products is skipped (byte-copy of products through synthesis).
"""
import collections, glob, json, pathlib, random

H = pathlib.Path(__file__).resolve().parents[1] / "history/runs/20260911T003500"  # the harness root
OUT = pathlib.Path(__file__).resolve().parent
TARGET = 150
DIMS = ["J1", "J2", "J3", "J4", "J5", "J6", "J7"]

orders = {}
for p in glob.glob(str(H / "pending/judged/*__*.json")):
    o = json.load(open(p))
    if o["field"] == "contract_products":
        continue
    for r in o["records"]:
        orders[r["content_key"]] = (o["subject"], o["field"], o["subject_name"], r)

rows = []
for p in glob.glob(str(H / "verdicts/*.jsonl")):
    for line in open(p):
        line = line.strip()
        if not line:
            continue
        v = json.loads(line)
        ok = v["content_key"]
        if ok not in orders:
            continue
        subj, field, sname, r = orders[ok]
        fails = {d: v["checks"][d] for d in DIMS if v["checks"].get(d, {}).get("verdict") == "fail"}
        rows.append(dict(
            content_key=ok, subject=subj, subject_name=sname, field=field, group_id=r["group_id"],
            chunk_bounds=r["chunk_bounds"], focal_form=r["focal_form"], evidence=r["evidence"],
            locations=r.get("locations") or [], synthesis=r["synthesis"],
            population=r.get("population"), any_fail=bool(fails),
            fails={d: {"severity": c.get("severity"), "note": c.get("note")} for d, c in fails.items()},
        ))
print("joined", len(rows))

by_field = collections.defaultdict(list)
for r in rows:
    by_field[r["field"]].append(r)

rnd = random.Random(20260911)
summary = []
with open(OUT / "verdict_index.jsonl", "w") as idx:
    for field, rs in sorted(by_field.items()):
        fails = [r for r in rs if r["any_fail"]]
        passes = [r for r in rs if not r["any_fail"]]
        want = max(TARGET - len(fails), 0)
        by_pop = collections.defaultdict(list)
        for r in passes:
            by_pop[r["population"]].append(r)
        chosen = []
        total_pass = len(passes)
        for pop, pool in sorted(by_pop.items(), key=lambda kv: kv[0] or ""):
            rnd.shuffle(pool)
            k = round(want * len(pool) / total_pass) if total_pass else 0
            chosen.extend(pool[:k])
        # top up / trim to exactly `want` (rounding)
        leftover = [r for r in passes if r not in chosen]
        rnd.shuffle(leftover)
        while len(chosen) < want and leftover:
            chosen.append(leftover.pop())
        chosen = chosen[:want]
        sample = fails + chosen
        rnd.shuffle(sample)
        with open(OUT / f"sample_{field}.jsonl", "w") as f:
            for r in sample:
                agent_row = {k: r[k] for k in ("content_key", "subject", "subject_name", "field", "focal_form", "evidence", "locations", "synthesis")}
                f.write(json.dumps(agent_row, ensure_ascii=False) + "\n")
                idx.write(json.dumps({k: r[k] for k in ("content_key", "field", "population", "any_fail", "fails")}, ensure_ascii=False) + "\n")
        pops = collections.Counter(r["population"] for r in sample)
        summary.append(f"{field}: judged {len(rs)}, fails {len(fails)}, sample {len(sample)} (passes {len(chosen)}; populations {dict(pops)})")
(OUT / "SAMPLE_SUMMARY.txt").write_text("\n".join(summary) + "\n")
print("\n".join(summary))
