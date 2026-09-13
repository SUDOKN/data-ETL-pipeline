"""Label-truth readout for a run on the LABEL wire (2026-09-13, design doc §33/§35).

  .venv/bin/python label_truth_readout.py <run_id>

Reads runA_<run>/results_<field>.jsonl (the stage-2 graders' rows, BRIEF.md + BRIEF_LABELS.md) and prints,
per field and overall: the paragraph's axis C (party) and D (capacity) marks (carried / absent / contradicted /
na) beside the LABELS' C and D marks (carried / contradicted / na), the share of rows where the labels and the
paragraph disagree (`labels.consistent == false`), and the confusion between the label's capacity value and
the grader's verdict on it (which capacity values are most often wrong). This is the "label truth" reading of
the experiment's decision rule: the labels must be at least as true as the paragraph on C and D."""
import collections, json, pathlib, sys

RUN = sys.argv[1]
D = pathlib.Path(__file__).resolve().parent
NEW = D / f"runA_{RUN}"
MARKS = ("carried", "absent", "contradicted", "na")


def load(path: pathlib.Path) -> list[dict]:
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except ValueError:
            continue
    return rows


def sample_labels(field: str) -> dict[str, dict]:
    out = {}
    p = NEW / f"sample_{field}.jsonl"
    if p.is_file():
        for r in load(p):
            out[r["content_key"]] = r.get("labels") or {}
    return out


totals: dict[str, collections.Counter] = collections.defaultdict(collections.Counter)
cap_wrong: collections.Counter = collections.Counter()
cap_seen: collections.Counter = collections.Counter()
doer_wrong: collections.Counter = collections.Counter()
doer_seen: collections.Counter = collections.Counter()
for path in sorted(NEW.glob("results_*.jsonl")):
    field = path.stem[len("results_"):]
    rows = load(path)
    labels_by_key = sample_labels(field)
    c = collections.Counter()
    for r in rows:
        axes = r.get("axes") or {}
        lab = r.get("labels") or {}
        for ax in ("C", "D"):
            c[f"para_{ax}_{(axes.get(ax) or {}).get('mark')}"] += 1
            c[f"label_{ax}_{(lab.get(ax) or {}).get('mark')}"] += 1
        if "consistent" in lab:
            c["consistent_true" if lab["consistent"] else "consistent_false"] += 1
        sl = labels_by_key.get(str(r.get("content_key") or "")) or {}
        if sl:
            cap_seen[sl.get("capacity")] += 1; doer_seen[sl.get("doer")] += 1
            if (lab.get("D") or {}).get("mark") == "contradicted":
                cap_wrong[sl.get("capacity")] += 1
            if (lab.get("C") or {}).get("mark") == "contradicted":
                doer_wrong[sl.get("doer")] += 1
    n = len(rows)
    totals["ALL"].update(c); totals["ALL"]["rows"] += n
    def pct(k: str) -> str:
        return f"{c[k]}/{n} = {100 * c[k] / n:.1f}%" if n else "0"
    print(f"\n== {field}: {n} graded rows")
    print(f"   paragraph C contradicted {pct('para_C_contradicted')}, absent {pct('para_C_absent')} | label C contradicted {pct('label_C_contradicted')}, na {pct('label_C_na')}")
    print(f"   paragraph D contradicted {pct('para_D_contradicted')}, absent {pct('para_D_absent')} | label D contradicted {pct('label_D_contradicted')}, na {pct('label_D_na')}")
    print(f"   labels disagree with paragraph: {pct('consistent_false')}")
n = totals["ALL"]["rows"]
if n:
    t = totals["ALL"]
    print(f"\n== ALL: {n} graded rows")
    print(f"   paragraph C contradicted {t['para_C_contradicted']} ({100*t['para_C_contradicted']/n:.1f}%), absent {t['para_C_absent']} | label C contradicted {t['label_C_contradicted']} ({100*t['label_C_contradicted']/n:.1f}%)")
    print(f"   paragraph D contradicted {t['para_D_contradicted']} ({100*t['para_D_contradicted']/n:.1f}%), absent {t['para_D_absent']} | label D contradicted {t['label_D_contradicted']} ({100*t['label_D_contradicted']/n:.1f}%)")
    print(f"   labels disagree with paragraph: {t['consistent_false']} ({100*t['consistent_false']/n:.1f}%)")
    print("   capacity label → share graded contradicted:")
    for v, seen in cap_seen.most_common():
        print(f"      {cap_wrong[v]:4d}/{seen:4d} = {100*cap_wrong[v]/seen:5.1f}%  {v}")
    print("   doer label → share graded contradicted:")
    for v, seen in doer_seen.most_common():
        print(f"      {doer_wrong[v]:4d}/{seen:4d} = {100*doer_wrong[v]/seen:5.1f}%  {v}")
