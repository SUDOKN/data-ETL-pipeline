"""Tabulate the answerability results: per field × axis, marks split by the run's own
pass/fail stratum (joined from verdict_index.jsonl). Prints markdown; writes READOUT.md."""
import collections, json, pathlib

D = pathlib.Path(__file__).resolve().parent
AXES = list("ABCDEFGH")
MARKS = ["carried", "absent", "contradicted", "na"]

index = {}
for line in open(D / "verdict_index.jsonl"):
    r = json.loads(line)
    index[r["content_key"]] = r

out = []
overall = collections.defaultdict(lambda: collections.Counter())
for path in sorted(D.glob("results_*.jsonl")):
    field = path.stem[len("results_"):]
    rows, bad, dup = [], 0, 0
    seen = set()
    for line in open(path):
        line = line.strip()
        if not line:
            continue
        try:
            r = json.loads(line)
        except Exception:
            bad += 1
            continue
        if r["content_key"] in seen:
            dup += 1
            continue
        seen.add(r["content_key"])
        rows.append(r)
    sample_keys = {json.loads(l)["content_key"] for l in open(D / f"sample_{field}.jsonl")}
    missing = sample_keys - seen
    extra = seen - sample_keys
    out.append(f"\n## {field}: {len(rows)} rows (malformed {bad}, duplicate {dup}, missing {len(missing)}, extra {len(extra)})")
    out.append("| axis | stratum | n | carried | absent | contradicted | na | absent+contradicted % |")
    out.append("|---|---|---:|---:|---:|---:|---:|---:|")
    for axis in AXES + ["field_item"]:
        for stratum in ("fail", "pass", "all"):
            c = collections.Counter()
            for r in rows:
                v = index.get(r["content_key"], {})
                s = "fail" if v.get("any_fail") else "pass"
                if stratum != "all" and s != stratum:
                    continue
                m = (r["axes"].get(axis) if axis != "field_item" else r.get("field_item")) or {}
                c[m.get("mark", "?")] += 1
            n = sum(c.values())
            bad_share = (c["absent"] + c["contradicted"]) / n * 100 if n else 0
            out.append(f"| {axis} | {stratum} | {n} | {c['carried']} | {c['absent']} | {c['contradicted']} | {c['na']} | {bad_share:.1f} |")
            if stratum == "all":
                overall[axis].update(c)
    worst = collections.Counter(r.get("worst", "?") for r in rows)
    out.append(f"worst axis distribution: {dict(worst.most_common())}")
    # top contradicted/absent quotes per axis for the design (up to 3 each)
    for axis in AXES + ["field_item"]:
        ex = []
        for r in rows:
            m = (r["axes"].get(axis) if axis != "field_item" else r.get("field_item")) or {}
            if m.get("mark") in ("absent", "contradicted"):
                ex.append(f"  - {m['mark']} `{r['content_key']}`: {m.get('note') or ''} | «{(m.get('quote') or '')[:160]}»")
        if ex:
            out.append(f"{axis} examples ({len(ex)}):")
            out.extend(ex[:4])

out.append("\n## all fields, all strata")
out.append("| axis | carried | absent | contradicted | na | absent+contradicted % |")
out.append("|---|---:|---:|---:|---:|---:|")
for axis, c in overall.items():
    n = sum(c.values())
    out.append(f"| {axis} | {c['carried']} | {c['absent']} | {c['contradicted']} | {c['na']} | {((c['absent']+c['contradicted'])/n*100 if n else 0):.1f} |")
text = "\n".join(out)
(D / "READOUT.md").write_text(text + "\n")
print(text)
