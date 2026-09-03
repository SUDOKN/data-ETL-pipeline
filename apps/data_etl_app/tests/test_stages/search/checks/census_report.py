"""Merge-time analysis over the full census (CENSUS_NOTES merge checklist).

Emits, for one run:
  A. per-field pooled rollup (from judgments/*.jsonl)
  B. per-field pooled confirmed recall (from scorecard JSONs)
  C. (unit, window) ranking by miss count vs forms returned  - the
     "which windows do I show a prompt engineer" list
  D. intra-run inconsistency, mechanical: a form the run itself returned
     somewhere, whose text occurs in >=2 windows of the same field run,
     converted per window it occurs in. Needs no judges.
  E. gate blind spots: units whose verdict is OK with 0 confirmed entries
     to gate on, beside their census in-field rate
  F. equipments metrology split (misses matching instrument vocabulary)
  G. anchor-mfg process_caps miss entities (equipment-noun refiling check)
  H. per-subject pipe-table miss share vs that subject's base rate

Usage: .venv/bin/python checks/census_report.py --run 20260901T013332
"""
from __future__ import annotations
import argparse, json, re, sys
from collections import Counter, defaultdict
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from paths import run_dir  # noqa: E402
from loading import load_run  # noqa: E402
from merge_judgments import ROLLUP, normalize_window  # noqa: E402

WS = re.compile(r"\s+")
def collapse(t: str) -> str: return WS.sub(" ", t)

METROLOGY = re.compile(
    r"\bcmm\b|coordinate.measuring|\bcomparators?\b|interferometer|"
    r"\bga[u]?ges?\b|gage maker|deltronic|virtek|tensile test|hardness test|"
    r"shadow ?graph|bor[eo]scopes?|micro-? ?vu|blue.light scan|profilometer|"
    r"micrometers?\b|calipers?\b|height gauge|surface plate|optical comparator|"
    r"\bprobing\b|zeiss|hexagon|renishaw|mitutoyo|\bfaro\b|romer|starrett|"
    r"spectromet|inspection equipment|measuring (?:equipment|instrument|machine)|"
    r"vision system|video measur|\bscales?\b|balance\b|durometer|rockwell|"
    r"brinell|charpy|izod|\bmicroscopes?\b", re.I)

EQUIP_NOUN = re.compile(
    r"robot|vision system|\bplc\b|\bdcs\b|assembly cell|tester|scanner|"
    r"machine\b|press\b|welder|feeder|conveyor", re.I)


def read_jsonl(path: Path) -> list[dict]:
    out = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line:
            out.append(json.loads(line))
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run", required=True)
    ap.add_argument("--top", type=int, default=25)
    args = ap.parse_args()
    rd = run_dir(args.run)
    jdir = rd / "judgments"

    # ---------- load judgments ----------
    units: dict[tuple[str, str], list[dict]] = {}
    for f in sorted(jdir.glob("*.jsonl")):
        if ".judge2" in f.name or "__part" in f.name:
            continue
        subject, field = f.stem.split("__", 1)
        units[(subject, field)] = read_jsonl(f)

    # ---------- A: per-field rollup ----------
    print("=" * 78)
    print("A. PER-FIELD POOLED ROLLUP (census codes -> shared rollup)")
    print("=" * 78)
    agg: dict[str, Counter] = defaultdict(Counter)
    actor_flag: Counter = Counter()
    for (subj, field), recs in units.items():
        for r in recs:
            if r.get("type") == "miss":
                agg[field]["misses"] += 1
                continue
            cat = ROLLUP.get(str(r.get("code", "")).strip().upper(), "unknown")
            agg[field][cat] += 1
            agg[field]["judged"] += 1
            actor = str(r.get("actor", "") or "").strip().lower()
            if actor and actor not in ("own", "subject", ""):
                agg[field]["wrong_actor"] += 1
    hdr = f"{'field':26s} {'judged':>7s} {'in-field':>9s} {'adjacent':>9s} {'generic':>8s} {'junk':>6s} {'wrong-actor':>11s} {'misses':>7s}"
    print(hdr); print("-" * len(hdr))
    tot: Counter = Counter()
    for field in sorted(agg):
        c = agg[field]; j = c["judged"] or 1
        tot.update(c)
        print(f"{field:26s} {c['judged']:7d} {c['in_field']/j:9.1%} {c['adjacent_field']/j:9.1%} "
              f"{c['generic']/j:8.1%} {c['junk']/j:6.1%} {c['wrong_actor']/j:11.1%} {c['misses']:7d}")
    j = tot["judged"] or 1
    print("-" * len(hdr))
    print(f"{'ALL':26s} {tot['judged']:7d} {tot['in_field']/j:9.1%} {tot['adjacent_field']/j:9.1%} "
          f"{tot['generic']/j:8.1%} {tot['junk']/j:6.1%} {tot['wrong_actor']/j:11.1%} {tot['misses']:7d}")
    print("\nNOTE mixed provenance (double-judged, miss union): alecmfg material_caps"
          " (+2 misses), alecmfg process_caps (+5) vs single-judged peers.")

    # ---------- B: pooled confirmed recall ----------
    print("\n" + "=" * 78)
    print("B. POOLED CONFIRMED RECALL (mechanical, eval-set standard)")
    print("=" * 78)
    rec: dict[str, Counter] = defaultdict(Counter)
    ok_zero: list[tuple[str, str]] = []
    for sc in sorted(rd.glob("scorecard_*.json")):
        d = json.loads(sc.read_text(encoding="utf-8"))
        exp = d["metrics"].get("expectations") or {}
        f = d["field"]
        rec[f]["scored"] += exp.get("confirmed_scored") or 0
        rec[f]["covered"] += exp.get("confirmed_covered") or 0
        rec[f]["ooc"] += exp.get("confirmed_out_of_coverage") or 0
        if (exp.get("confirmed_scored") or 0) == 0 and (d["metrics"].get("verdict") or {}).get("status") == "OK":
            ok_zero.append((d["subject"], f))
    hdr = f"{'field':26s} {'scored':>7s} {'covered':>8s} {'recall':>7s} {'out-of-coverage':>16s}"
    print(hdr); print("-" * len(hdr))
    for f in sorted(rec):
        c = rec[f]
        r = f"{c['covered']/c['scored']:6.1%}" if c["scored"] else "     —"
        print(f"{f:26s} {c['scored']:7d} {c['covered']:8d} {r:>7s} {c['ooc']:16d}")

    # ---------- C: window miss:return ranking ----------
    print("\n" + "=" * 78)
    print(f"C. TOP {args.top} WINDOWS BY MISS COUNT (unit / window / misses / forms returned)")
    print("=" * 78)
    rows = []
    for (subj, field), recs in units.items():
        per_win: dict[str, Counter] = defaultdict(Counter)
        for r in recs:
            w = normalize_window(r.get("window"))
            per_win[w]["miss" if r.get("type") == "miss" else "form"] += 1
        for w, c in per_win.items():
            if c["miss"]:
                rows.append((c["miss"], c["form"], subj, field, w))
    rows.sort(reverse=True)
    for miss, form, subj, field, w in rows[: args.top]:
        print(f"  {miss:4d} misses / {form:4d} returned   {subj:24s} {field:24s} {w}")

    # ---------- D: intra-run inconsistency (mechanical) ----------
    print("\n" + "=" * 78)
    print("D. INTRA-RUN INCONSISTENCY (mechanical; forms vs the windows whose text holds them)")
    print("=" * 78)
    pulled = rd / "raw" / "search_requests.json"
    field_runs = load_run(args.run, pulled_file=pulled if pulled.is_file() else None)
    # census-code lookup so the metric can be restricted to in-field forms:
    # nav/boilerplate strings recur in EVERY window's text, so counting them
    # as inconsistency opportunities inflates the metric with cases where
    # returning them nowhere would have been the better behaviour.
    infield_forms: dict[tuple[str, str], set[str]] = defaultdict(set)
    for (subj, field), recs in units.items():
        for r in recs:
            if r.get("type") != "miss" and ROLLUP.get(str(r.get("code", "")).strip().upper()) == "in_field":
                infield_forms[(subj, field)].add(str(r.get("form", "")).strip())
    inc: dict[str, Counter] = defaultdict(Counter)
    examples: list[tuple[int, int, str, str, str]] = []
    for fr in field_runs:
        # union of parsed phrases per sub-window, across rounds
        forms_by_win: dict[str, set[str]] = defaultdict(set)
        text_by_win: dict[str, str] = {}
        for w in fr.windows:
            if w.phrases:
                forms_by_win[w.sub_bounds].update(p.strip() for p in w.phrases if p.strip())
            if w.wire_text and w.sub_bounds not in text_by_win:
                text_by_win[w.sub_bounds] = collapse(w.wire_text).casefold()
        wins = [b for b in text_by_win if b in forms_by_win or True]
        all_forms = set().union(*forms_by_win.values()) if forms_by_win else set()
        for form in all_forms:
            cf = collapse(form).casefold()
            if len(cf) < 4:
                continue
            pat = re.compile(r"(?<!\w)" + re.escape(cf) + r"(?!\w)")
            t_wins = [b for b in wins if pat.search(text_by_win[b])]
            if len(t_wins) < 2:
                continue
            hit = sum(1 for b in t_wins if form in forms_by_win.get(b, ()))
            inc[fr.field]["forms"] += 1
            inc[fr.field]["opps"] += len(t_wins)
            inc[fr.field]["hits"] += hit
            in_field = form in infield_forms.get((fr.subject.replace(".", "_"), fr.field), ())
            if in_field:
                inc[fr.field]["if_forms"] += 1
                inc[fr.field]["if_opps"] += len(t_wins)
                inc[fr.field]["if_hits"] += hit
                if hit == len(t_wins):
                    inc[fr.field]["if_consistent"] += 1
            if hit == len(t_wins):
                inc[fr.field]["consistent"] += 1
            elif len(t_wins) - hit >= 2 and in_field:
                examples.append((len(t_wins) - hit, len(t_wins), fr.subject, fr.field, form))
    hdr = (f"{'field':26s} {'probe forms':>12s} {'conversion':>11s} {'fully consistent':>17s}"
           f" {'IN-FIELD forms':>15s} {'conv':>7s} {'consistent':>11s}")
    print(hdr); print("-" * len(hdr))
    tot = Counter()
    for f in sorted(inc):
        c = inc[f]; tot.update(c)
        if_part = (f" {c['if_forms']:15d} {c['if_hits']/c['if_opps']:7.1%} {c['if_consistent']/c['if_forms']:11.1%}"
                   if c["if_forms"] else f" {'—':>15s} {'—':>7s} {'—':>11s}")
        print(f"{f:26s} {c['forms']:12d} {c['hits']/c['opps']:11.1%} {c['consistent']/c['forms']:17.1%}" + if_part)
    if tot["forms"]:
        print("-" * len(hdr))
        print(f"{'ALL':26s} {tot['forms']:12d} {tot['hits']/tot['opps']:11.1%} {tot['consistent']/tot['forms']:17.1%}"
              f" {tot['if_forms']:15d} {tot['if_hits']/tot['if_opps']:7.1%} {tot['if_consistent']/tot['if_forms']:11.1%}")
    print("\n  (conversion = of the windows whose text contains a form the run returned")
    print("   somewhere, how many actually returned it; excludes forms seen in <2 windows)")
    examples.sort(reverse=True)
    print("\n  worst examples (missed-in/present-in  subject field  form):")
    for gap, t, subj, field, form in examples[:15]:
        print(f"   {gap}/{t}  {subj:22s} {field:22s} {form[:60]!r}")

    # ---------- E: gate blind spots ----------
    print("\n" + "=" * 78)
    print("E. VERDICT=OK WITH NOTHING TO GATE ON (census in-field beside each)")
    print("=" * 78)
    for subj, f in ok_zero:
        key = (subj.replace(".", "_"), f)
        recs = units.get(key, [])
        judged = [r for r in recs if r.get("type") != "miss"]
        inf = sum(1 for r in judged if ROLLUP.get(str(r.get("code","")).strip().upper()) == "in_field")
        rate = f"{inf}/{len(judged)}" if judged else "0 returned"
        print(f"  {subj:26s} {f:26s} census in-field {rate}")

    # ---------- F: equipments metrology split ----------
    print("\n" + "=" * 78)
    print("F. EQUIPMENTS MISSES — METROLOGY SPLIT (per subject)")
    print("=" * 78)
    for (subj, field), recs in sorted(units.items()):
        if field != "equipments":
            continue
        misses = [r for r in recs if r.get("type") == "miss"]
        if not misses:
            continue
        met = [r for r in misses if METROLOGY.search(str(r.get("entity", "")))]
        print(f"  {subj:26s} misses {len(misses):4d}   metrology {len(met):3d}   other {len(misses)-len(met):4d}")
        for r in met[:6]:
            print(f"      metrology e.g.: {r.get('entity','')[:70]}")

    # ---------- G: anchor-mfg process_caps equipment-noun misses ----------
    print("\n" + "=" * 78)
    print("G. anchor-mfg process_caps MISSES (equipment nouns = candidate re-files)")
    print("=" * 78)
    for r in units.get(("anchor-mfg_com", "process_caps"), []):
        if r.get("type") == "miss":
            tag = " <-- equipment noun" if EQUIP_NOUN.search(str(r.get("entity", ""))) else ""
            print(f"  {str(r.get('entity',''))[:70]:70s}{tag}")

    # ---------- H: per-subject pipe-line miss share ----------
    print("\n" + "=" * 78)
    print("H. PIPE-TABLE MISS SHARE PER SUBJECT (misses located in wire text)")
    print("=" * 78)
    text_by_subj: dict[str, list[str]] = defaultdict(list)
    for fr in field_runs:
        for w in fr.windows:
            if w.wire_text:
                text_by_subj[fr.subject].append(w.wire_text)
    hdr = f"{'subject':26s} {'located':>8s} {'on | line':>10s} {'share':>7s} {'base rate':>10s}"
    print(hdr); print("-" * len(hdr))
    for subj in sorted(text_by_subj):
        texts = text_by_subj[subj]
        lines = [ln for t in texts for ln in t.splitlines()]
        base = sum(1 for ln in lines if "|" in ln) / (len(lines) or 1)
        located = piped = 0
        key_subj = subj.replace(".", "_")
        for (s, f), recs in units.items():
            if s != key_subj:
                continue
            for r in recs:
                if r.get("type") != "miss":
                    continue
                q = collapse(str(r.get("quote", "") or "")).strip()
                if len(q) < 8:
                    continue
                for t in texts:
                    idx = collapse(t).casefold().find(q.casefold())
                    if idx >= 0:
                        located += 1
                        seg_start = t.casefold().find(q[:40].casefold())
                        if seg_start >= 0:
                            ls = t.rfind("\n", 0, seg_start) + 1
                            le = t.find("\n", seg_start)
                            if "|" in t[ls: le if le > 0 else len(t)]:
                                piped += 1
                        break
        if located:
            print(f"{subj:26s} {located:8d} {piped:10d} {piped/located:7.1%} {base:10.1%}")


if __name__ == "__main__":
    main()
