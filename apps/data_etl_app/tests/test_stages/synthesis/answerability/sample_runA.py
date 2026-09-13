"""Stage-2 (answerability) sample for a NEW run: the SAME records as the baseline stage-2
sample (sample_<field>.jsonl, drawn from run 20260911T003500), carrying the new run's paragraphs,
so axis marks pair record-for-record. content_key is content-hashed (it changes with the
paragraph), so records are matched on the RECORD key (subject, field, chunk_bounds, group_id)
via the baseline run's judged orders. Records absent from the new run are listed in
MISSING_<run>.txt. Usage: sample_runA.py <run_id> [<baseline_run>] → runA_<run>/sample_<field>.jsonl"""
import glob, json, pathlib, sys

RUN = sys.argv[1]
BASE = sys.argv[2] if len(sys.argv) > 2 else "20260911T003500"
D = pathlib.Path(__file__).resolve().parent
RUNS = D.parent / "history/runs"  # the harness root
OUT = D / f"runA_{RUN}"
OUT.mkdir(exist_ok=True)

def record_keys(run: str, sub: str) -> dict:
    out = {}
    for p in glob.glob(str(RUNS / run / sub / "*__*.json")):
        o = json.load(open(p))
        if o["field"] == "contract_products":
            continue
        for r in o["records"]:
            out[r["content_key"]] = ((o["subject"], o["field"], r["chunk_bounds"], r["group_id"]), o["subject_name"], r)
    return out

base = record_keys(BASE, "pending/judged")
new_by_key = {k: (sname, r) for _, (k, sname, r) in record_keys(RUN, "pending").items()}

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
                   "evidence": r["evidence"], "locations": r.get("locations") or [], "synthesis": r["synthesis"]}
            f.write(json.dumps(row, ensure_ascii=False) + "\n"); n += 1
    summary.append(f"{field}: {n} rows")
(OUT / f"MISSING_{RUN}.txt").write_text("\n".join(missing) + "\n")
print("\n".join(summary)); print("missing", len(missing))
