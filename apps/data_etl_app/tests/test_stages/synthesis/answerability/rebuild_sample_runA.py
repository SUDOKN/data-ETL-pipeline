"""Rebuild runA_<run>/sample_<field>.jsonl from FULL work orders when the run's pending/*.json have since
been thinned to unjudged records by a later `run_eval.py --pull` (2026-09-12: the re-pull that folded
alecmfg's two re-run fields into run A did exactly that, and sample_runA.py then matched only 35 rows).
Same row shape and row order as sample_runA.py; the new run's records are read from every order dir given.
Usage: rebuild_sample_runA.py <run_id> <baseline_run> <order_dir> [<order_dir> ...]"""
import glob, json, pathlib, sys

RUN, BASE, *ORDER_DIRS = sys.argv[1:]
D = pathlib.Path(__file__).resolve().parent
RUNS = D.parent / "history/runs"  # the harness root
OUT = D / f"runA_{RUN}"
OUT.mkdir(exist_ok=True)

def record_keys(paths) -> dict:
    out = {}
    for p in paths:
        o = json.load(open(p))
        if o["field"] == "contract_products":
            continue
        for r in o["records"]:
            out[r["content_key"]] = ((o["subject"], o["field"], r["chunk_bounds"], r["group_id"]), o["subject_name"], r)
    return out

base = record_keys(glob.glob(str(RUNS / BASE / "pending/judged/*__*.json")))
new_by_key = {}
for d in ORDER_DIRS:
    for _, (k, sname, r) in record_keys(glob.glob(str(pathlib.Path(d) / "*__*.json"))).items():
        new_by_key.setdefault(k, (sname, r))

missing, summary = [], []
for path in sorted(D.glob("sample_*.jsonl")):
    field = path.stem[len("sample_"):]
    n = 0
    with open(OUT / path.name, "w") as f:
        for line in open(path):
            b = json.loads(line)
            hit = base.get(b["content_key"])
            if hit is None:
                missing.append(f"{field}\t{b['content_key']}\t{b['subject']}\tnot-in-baseline-orders"); continue
            key = hit[0]
            got = new_by_key.get(key)
            if got is None:
                missing.append(f"{field}\t{b['content_key']}\t{b['subject']}\t{'|'.join(key)}"); continue
            sname, r = got
            row = {"content_key": r["content_key"], "baseline_content_key": b["content_key"], "record_key": "|".join(key),
                   "subject": key[0], "subject_name": sname, "field": key[1], "focal_form": r["focal_form"],
                   "evidence": r["evidence"], "locations": r.get("locations") or [], "synthesis": r["synthesis"],
                   # the four labels the model decided before the paragraph (2026-09-13; None on earlier runs)
                   "labels": r.get("labels")}
            f.write(json.dumps(row, ensure_ascii=False) + "\n"); n += 1
    summary.append(f"{field}: {n} rows")
(OUT / f"MISSING_{RUN}.txt").write_text("\n".join(missing) + "\n")
print("\n".join(summary)); print("missing", len(missing))
