"""Pass 2 on run 20260823T044500 — the questions dump_compare.py does not answer:
coverage in tokens, search drift, packing/re-send cost, location shape, empty-group
explanation, discovered casings, run cost.  Run from the repo root:
    python3 pipeline_v3_evidence/2026-08-23_run_044500_pages_dropped_no_address/pass2.py
"""
import json, os, sys, glob, re, collections, random, statistics as st

REPO = "/Users/amaryadav/Documents/ASU_PhD/SUDOKN/data-ETL-pipeline"
ROOT = f"{REPO}/packages/logs/extraction_dumps"
NEW = "20260823T044500"
PREV = {"alecmfg_com": "20260823T034518", "steelcraft_com": "20260822T223715"}
# products and contract_products share one dump identity — count products only.
FIELDS = ["products", "equipments", "conformity_attestations", "industries", "process_caps", "material_caps"]
SUBJECTS = ("alecmfg_com", "steelcraft_com")
SAMPLE = f"{REPO}/apps/data_etl_app/src/data_etl_app/knowledge/sample_scraped_texts"


def load(run, subj, field):
    p = f"{ROOT}/{run}/{subj}__{field}__partial.json"
    return json.load(open(p)) if os.path.exists(p) else None


def section(t):
    print("\n" + "=" * 78 + f"\n{t}\n" + "=" * 78)


# ---------------------------------------------------------------- 1. coverage
section("1. REAL-CONTENT COVERAGE (tiktoken o200k on the sample scraped text)")
sys.path.insert(0, f"{REPO}/packages/core/src")
from core.utils.floor_scan import drop_excluded_pages  # noqa: E402
import tiktoken  # noqa: E402

enc = tiktoken.get_encoding("o200k_base")
for subj in SUBJECTS:
    d = load(NEW, subj, "products")
    bounds = list(d["chunks"].keys())
    raw = open(f"{SAMPLE}/{subj.replace('_com', '.com')}.txt").read()
    pe = drop_excluded_pages(raw)
    trimmed, tot = pe.text, len(enc.encode(pe.text))
    used = sum(len(enc.encode(trimmed[int(b.split(':')[0]):min(int(b.split(':')[1]), len(trimmed))])) for b in bounds)
    print(f"  {subj}: raw {len(raw):,} ch / {len(enc.encode(raw)):,} tok"
          f" -> trimmed {len(trimmed):,} ch / {tot:,} tok (dropped {len(pe.dropped)} legal pages)")
    for b in bounds:
        s, e = (int(x) for x in b.split(":"))
        e = min(e, len(trimmed))
        print(f"     chunk {b}: {e - s:,} ch, {len(enc.encode(trimmed[s:e])):,} tok")
    print(f"     budget 40,000 tok; used {used:,}; real-content coverage {used / tot:.1%}")
    print("     excluded: " + ", ".join(f"{p['url'].rstrip('/').split('/')[-1]} ({p['chars']:,} ch)"
                                        for p in d["run"]["scraped_text"]["excluded_pages"]["pages"]))

# ------------------------------------------------------------- 2. search drift
section("2. SEARCH DRIFT — distinct phrases, new vs previous run")
for subj in SUBJECTS:
    print(f"  {subj} (prev {PREV[subj]})")
    for f in FIELDS:
        o, n = load(PREV[subj], subj, f), load(NEW, subj, f)
        if not o or not n:
            print(f"    {f:<26} missing"); continue
        po = {r["phrase"] for c in o["chunks"].values() for r in c.get("rows", [])}
        pn = {r["phrase"] for c in n["chunks"].values() for r in c.get("rows", [])}
        print(f"    {f:<26} old {len(po):>4}  new {len(pn):>4}  shared {len(po & pn):>4}"
              f"  jaccard {len(po & pn) / max(1, len(po | pn)):.2f}  new-only {len(pn - po):>4}  lost {len(po - pn):>4}")

# ------------------------------------------------------- 3. totals + 4. packing
section("3. RUN TOTALS (6 distinct phrase-field dumps per subject)")
for subj in SUBJECTS:
    for lbl, run in (("PREV " + PREV[subj], PREV[subj]), ("NEW ", NEW)):
        ti = to = m = it = gr = em = 0
        ok = True
        for f in FIELDS:
            d = load(run, subj, f)
            if not d:
                ok = False; continue
            tu = d["run"]["token_usage"]; ti += tu["input_tokens"]; to += tu["output_tokens"]
            for c in d["chunks"].values():
                s = c["fold"]["summary"]
                m += s["mentions"]; it += s["distinct_snippets"]; gr += s["groups"]; em += s["empty_groups"]
        print(f"  {subj} {lbl}: in {ti:>8,} out {to:>7,} | mentions {m:>5} items {it:>5} groups {gr:>5} empty {em:>4}"
              + ("" if ok else "   [INCOMPLETE]"))

section("4. MENTION-STAGE PACKING / WINDOW RE-SEND COST (new run)")
rows = []
for subj in SUBJECTS:
    for f in FIELDS:
        d = load(NEW, subj, f)
        for c in d["chunks"].values():
            wins = {w["sub_bounds"]: w for w in c["fold"]["windows"]}
            for sub, lst in c["requests"].get("llm_phrase_mention_collection", {}).items():
                n = wins.get(sub, {}).get("distinct_snippets", 0)
                for i, x in enumerate(lst):
                    rows.append((subj, f, sub, i, min(50, n - 50 * i), x.get("input_tokens"), x.get("output_tokens")))
sizes = [r[4] for r in rows]
tin = sum(r[5] or 0 for r in rows)
extra = sum(r[5] or 0 for r in rows if r[3] > 0)
print(f"  {len(rows)} requests; items/request median {st.median(sizes)}, mean {sum(sizes)/len(sizes):.1f}, max {max(sizes)}")
print(f"  requests with <10 items: {sum(1 for s in sizes if s < 10)}   with <5: {sum(1 for s in sizes if s < 5)}")
print(f"  mention-stage in-tokens {tin:,}; spent on the 2nd+ group of a window (window text re-sent) {extra:,} ({extra/tin:.0%})")
print("  smallest paid requests (items, in, out):")
for r in sorted((r for r in rows if r[5]), key=lambda r: r[4])[:6]:
    print(f"     {r[0]}/{r[1]} {r[2]} grp{r[3]}: {r[4]} items, in {r[5]:,}, out {r[6]:,}")
print(f"  unpaid NO_MODEL dummies (zero-form windows, never dispatched): {sum(1 for r in rows if r[5] is None)}")

# ------------------------------------------------------------- 5. locations
section("5. LOCATION SHAPE (the no-address prompt)")
URLRE = re.compile(r"https?://\S+")
locs, snips, prefixes = [], [], collections.Counter()
for subj in SUBJECTS:
    for f in FIELDS:
        d = load(NEW, subj, f)
        for c in d["chunks"].values():
            for g in c["fold"]["groups"]:
                for mm in g["mentions"]:
                    L = mm.get("location", "")
                    locs.append((subj, f, L)); snips.append(len(mm.get("snippet", "")))
                    prefixes[" ".join(L.split()[:2]).lower().strip(",.")] += 1
LL = [len(x[2]) for x in locs]
print(f"  {len(locs)} locations, all location_source=llm")
print(f"  chars: median {st.median(LL)}, p90 {sorted(LL)[int(len(LL)*.9)]}, max {max(LL)}"
      f"   (snippet median {st.median(snips)} — location is {st.median(LL)/st.median(snips):.2f}x the snippet)")
print(f"  containing a URL: {sum(1 for x in locs if URLRE.search(x[2]))} ({sum(1 for x in locs if URLRE.search(x[2]))/len(locs):.1%})")
PAGERE = re.compile(r"\bpage\b", re.I)
print(f"  naming a 'page':  {sum(1 for x in locs if PAGERE.search(x[2]))/len(locs):.0%}")
print(f"  opening words: {prefixes.most_common(8)}")
form = sum(v for k, v in prefixes.items() if k.startswith("this "))
print(f"  formulaic 'This <unit> …' opener: {form}/{len(locs)} = {form/len(locs):.1%}")
random.seed(7)
print("  samples:")
for s in random.sample(locs, 6):
    print(f"     [{s[0]}/{s[1]}] {s[2][:200]}")

section("6. SYNTHESIS PAYLOAD ESTIMATE (distinct (group, snippet), first location)")
for subj in SUBJECTS:
    for lbl, run in (("PREV " + PREV[subj], PREV[subj]), ("NEW ", NEW)):
        ent = sn = lo = 0
        ok = True
        for f in FIELDS:
            d = load(run, subj, f)
            if not d:
                ok = False; continue
            seen = {}
            for c in d["chunks"].values():
                for g in c["fold"]["groups"]:
                    for mm in g["mentions"]:
                        seen.setdefault((g["group_id"], mm.get("snippet", "")), mm.get("location", ""))
            ent += len(seen); sn += sum(len(k[1]) for k in seen); lo += sum(len(v) for v in seen.values())
        if ok:
            print(f"  {subj} {lbl}: entries {ent:>5}, snippet {sn:>8,} ch, location {lo:>8,} ch,"
                  f" total {sn+lo:>8,} ch, location share {lo/(sn+lo):.0%}")

section("7. EMPTY GROUPS + DISCOVERED CASINGS")
tot = expl = 0
unexpl = []
casings = collections.Counter()
shorts = 0
for subj in SUBJECTS:
    for f in FIELDS:
        d = load(NEW, subj, f)
        for c in d["chunks"].values():
            keys = [g["key"] for g in c["fold"]["groups"] if g.get("status") != "no_mentions"]
            for g in c["fold"]["groups"]:
                if g.get("status") == "no_mentions":
                    tot += 1
                    if any(g["key"] in k and k != g["key"] for k in keys):
                        expl += 1
                    else:
                        unexpl.append((subj, f, g["key"], g["forms"]))
            for w in c["fold"]["windows"]:
                shorts += len(w.get("short_forms") or [])
                for k, vs in (w.get("discovered_casings") or {}).items():
                    for v in vs:
                        casings[(subj, f, k, v)] += 1
print(f"  empty groups {tot}; explained by D8 containment {expl} ({expl/tot:.0%}); unexplained {len(unexpl)}")
for u in unexpl:
    print("     UNEXPLAINED:", u)
print(f"  discovered casings: {sum(casings.values())} window sightings, {len(casings)} distinct"
      " (subject, field, form, casing)")
print("  NOTE: collection matches case-INSENSITIVELY (_owning_hits reads scan.tier2), so these are casings the"
      "\n        scan ADDED beyond what search sent — not misses. Cross-check below.")
harv = miss = 0
diff = tot_m = 0
for subj in SUBJECTS:
    for f in FIELDS:
        d = load(NEW, subj, f)
        for cb, c in d["chunks"].items():
            seen = collections.defaultdict(set)
            for g in c["fold"]["groups"]:
                for mm in g["mentions"]:
                    tot_m += 1
                    seen[mm["window"]].add(mm["form"])
                    if mm.get("sent_form") and mm["sent_form"] != mm["form"]:
                        diff += 1
            for i, w in enumerate(c["fold"]["windows"]):
                for base, cs in (w.get("discovered_casings") or {}).items():
                    for cse in cs:
                        if cse in seen[i]:
                            harv += 1
                        else:
                            miss += 1
print(f"     casings present as a harvested mention form in their window: {harv}")
print(f"     not present as a standalone form (swallowed by a longer hit, D8 containment): {miss}")
print(f"  mentions harvested under a casing DIFFERENT from the sent form: {diff}/{tot_m} = {diff/tot_m:.1%}")
random.seed(11)
for k in random.sample(list(casings), 8):
    print(f"     {k[0]}/{k[1]}: {k[2]!r} -> {k[3]!r}")
print(f"  short forms (<=3 chars, case-sensitive by policy): {shorts}")

section("8. RUN COST (all 10 fields per subject, contract_products deduped)")
tin = to = 0
for p in sorted(glob.glob(f"{ROOT}/{NEW}/*.json")):
    d = json.load(open(p))
    if d["field_type"] == "contract_products":
        continue
    tu = d["run"].get("token_usage") or {}
    tin += tu.get("input_tokens", 0); to += tu.get("output_tokens", 0)
    print(f"  {d['subject_unique_id']:<16}{d['field_type']:<26} in {tu.get('input_tokens',0):>8,} out {tu.get('output_tokens',0):>7,}")
print(f"  TOTAL in {tin:,} out {to:,}  (~${tin/1e6*2 + to/1e6*8:.2f} at $2/M in, $8/M out)")
