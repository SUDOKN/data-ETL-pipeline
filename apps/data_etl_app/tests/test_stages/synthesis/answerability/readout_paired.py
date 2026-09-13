"""Paired stage-2 readout: baseline results_<field>.jsonl (run 20260911T003500) vs a new run's
runA_<run>/results_<field>.jsonl, matched on the baseline content key each new row carries.
Per field × axis: absent+contradicted % on the baseline vs the new run over the PAIRED records,
split by the baseline J-rubric stratum (pass/fail), plus per-record transitions (fixed / broken).
Usage: readout_paired.py <run_id>  → writes READOUT_PAIRED_<run>.md"""
import collections, json, pathlib, sys

RUN = sys.argv[1]
D = pathlib.Path(__file__).resolve().parent
NEW = D / f"runA_{RUN}"
AXES = list("ABCDEFGH") + ["field_item"]
BAD = ("absent", "contradicted")

index = {json.loads(l)["content_key"]: json.loads(l) for l in open(D / "verdict_index.jsonl")}

def load(path, keymap=None):
    """rows keyed by the BASELINE content key; `keymap` translates a new run's content_key
    (the agents echo only that) via the run's sample file, which carries both keys."""
    rows = {}
    for line in open(path):
        line = line.strip()
        if not line:
            continue
        try:
            r = json.loads(line)
        except Exception:
            continue
        k = r.get("baseline_content_key") or (keymap or {}).get(r["content_key"]) or r["content_key"]
        rows[k] = r
    return rows

def sample_keymap(path):
    m = {}
    if path.exists():
        for line in open(path):
            r = json.loads(line)
            m[r["content_key"]] = r.get("baseline_content_key", r["content_key"])
    return m

def mark(r, axis):
    m = (r["axes"].get(axis) if axis != "field_item" else r.get("field_item")) or {}
    return m.get("mark", "?")

out = [f"# Stage-2 paired readout — baseline 20260911T003500 vs {RUN}\n"]
tot = collections.defaultdict(lambda: collections.Counter())
for bpath in sorted(D.glob("results_*.jsonl")):
    field = bpath.stem[len("results_"):]
    npath = NEW / bpath.name
    if not npath.exists():
        out.append(f"\n## {field}: new results not present yet"); continue
    base, new = load(bpath), load(npath, sample_keymap(NEW / f"sample_{field}.jsonl"))
    keys = [k for k in base if k in new]
    out.append(f"\n## {field}: {len(keys)} paired records (baseline {len(base)}, new {len(new)})")
    clean_b = sum(all(mark(base[k], a) not in BAD for a in AXES) for k in keys)
    clean_n = sum(all(mark(new[k], a) not in BAD for a in AXES) for k in keys)
    out.append(f"fully clean: baseline {clean_b} → new {clean_n}")
    out.append("| axis | stratum | n | base bad % | new bad % | fixed | broken | still bad |")
    out.append("|---|---|---:|---:|---:|---:|---:|---:|")
    for a in AXES:
        for stratum in ("pass", "fail", "all"):
            ks = [k for k in keys if stratum == "all" or (("fail" if index.get(k, {}).get("any_fail") else "pass") == stratum)]
            if not ks:
                continue
            bb = sum(mark(base[k], a) in BAD for k in ks); nb = sum(mark(new[k], a) in BAD for k in ks)
            fixed = sum(mark(base[k], a) in BAD and mark(new[k], a) not in BAD for k in ks)
            broken = sum(mark(base[k], a) not in BAD and mark(new[k], a) in BAD for k in ks)
            still = sum(mark(base[k], a) in BAD and mark(new[k], a) in BAD for k in ks)
            out.append(f"| {a} | {stratum} | {len(ks)} | {bb/len(ks)*100:.1f} | {nb/len(ks)*100:.1f} | {fixed} | {broken} | {still} |")
            if stratum == "all":
                tot[a].update({"n": len(ks), "bb": bb, "nb": nb, "fixed": fixed, "broken": broken})
    # broken examples (regressions) per axis, up to 3
    for a in AXES:
        ex = [k for k in keys if mark(base[k], a) not in BAD and mark(new[k], a) in BAD]
        if ex:
            out.append(f"{a} broken examples ({len(ex)}):")
            for k in ex[:3]:
                m = (new[k]["axes"].get(a) if a != "field_item" else new[k].get("field_item")) or {}
                out.append(f"  - `{k}` {m.get('mark')}: {m.get('note') or ''} | «{(m.get('quote') or '')[:150]}»")
out.append("\n## all fields (paired, all strata)")
out.append("| axis | n | base bad % | new bad % | fixed | broken |")
out.append("|---|---:|---:|---:|---:|---:|")
for a in AXES:
    c = tot[a]
    if c["n"]:
        out.append(f"| {a} | {c['n']} | {c['bb']/c['n']*100:.1f} | {c['nb']/c['n']*100:.1f} | {c['fixed']} | {c['broken']} |")
text = "\n".join(out)
(D / f"READOUT_PAIRED_{RUN}.md").write_text(text + "\n")
print(text)
