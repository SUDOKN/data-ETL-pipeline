import json, os, re, random, collections, statistics as st
ROOT="/Users/amaryadav/Documents/ASU_PhD/SUDOKN/data-ETL-pipeline/packages/logs/extraction_dumps"
SP="<scratchpad>"
NEW="20260823T034518"; OLD="20260822T223715"
FIELDS=["products","equipments","conformity_attestations","industries","process_caps","material_caps"]
def load(run, subj, field):
    p=f"{ROOT}/{run}/{subj}__{field}__partial.json"
    return json.load(open(p)) if os.path.exists(p) else None
random.seed(7)
print("=== 1. SEARCH DRIFT (alecmfg): rows per field old vs new, phrase-set overlap ===")
for f in FIELDS:
    o=load(OLD,"alecmfg_com",f); n=load(NEW,"alecmfg_com",f)
    po=set(r["phrase"] for ch in o["chunks"].values() for r in ch["rows"]); pn=set(r["phrase"] for ch in n["chunks"].values() for r in ch["rows"])
    lo=set(p.lower() for p in po); ln=set(p.lower() for p in pn)
    print(f" {f:24s} rows old {sum(len(ch['rows']) for ch in o['chunks'].values()):4d} new {sum(len(ch['rows']) for ch in n['chunks'].values()):4d} | distinct old {len(po):4d} new {len(pn):4d} | both {len(po&pn):4d} only-old {len(po-pn):4d} only-new {len(pn-po):4d} | casefold both {len(lo&ln)} jaccard {len(lo&ln)/len(lo|ln):.2f}")
print("\n=== 2. EMPTY GROUPS in new run: swallowed by a longer group form in same chunk? ===")
def wb(sub, full):
    return re.search(r"(?<!\w)"+re.escape(sub)+r"(?!\w)", full) is not None
for f in FIELDS:
    n=load(NEW,"alecmfg_com",f)
    sw=0; un=[]; tot=0
    for cb,ch in n["chunks"].items():
        gs=ch["fold"]["groups"]
        okforms=[fm for g in gs if g.get("status")=="ok" for fm in g["forms"]]
        for g in gs:
            if g.get("status")!="no_mentions": continue
            tot+=1
            if any(wb(fm.lower(), of.lower()) and fm.lower()!=of.lower() for fm in g["forms"] for of in okforms): sw+=1
            else: un.append((cb, g["forms"]))
    print(f" {f:24s} empty {tot:3d} swallowed {sw:3d} unexplained {len(un)} {un[:6]}")
print("\n=== 3. LOCATION SAMPLES (new run) ===")
def locs_of(run,f):
    d=load(run,"alecmfg_com",f); out=[]
    for ch in d["chunks"].values():
        for g in ch["fold"]["groups"]:
            for m in g["mentions"]:
                if m.get("location_source")=="llm": out.append((g["key"], m["page"], m["snippet"], m["location"]))
    return out
for f in ("industries","products"):
    L=locs_of(NEW,f)
    # dedupe by location text
    seen=set(); U=[x for x in L if not (x[3] in seen or seen.add(x[3]))]
    print(f"\n-- {f}: {len(L)} llm-located mentions, {len(U)} distinct locations")
    for k,pg,sn,lc in random.sample(U, 8):
        print(f"  [{k}] page={pg}\n    SNIPPET: {sn[:140]!r}\n    LOC: {lc!r}")
    print("  -- longest 3:")
    for k,pg,sn,lc in sorted(U,key=lambda x:-len(x[3]))[:3]: print(f"    ({len(lc)}) {lc!r}")
    url_exact=sum(1 for k,pg,sn,lc in U if pg and pg in lc); url_any=sum(1 for k,pg,sn,lc in U if re.search(r"https?://",lc))
    print(f"  URL present: {url_any}/{len(U)} ; cites its own code-derived page URL exactly: {url_exact}/{len(U)}")
print("\n=== 3b. OLD-run industries samples (for contrast) ===")
L=locs_of(OLD,"industries"); seen=set(); U=[x for x in L if not (x[3] in seen or seen.add(x[3]))]
for k,pg,sn,lc in random.sample(U,5): print(f"  [{k}] LOC: {lc!r}")
print("\n=== 4. SYNTHESIS PAYLOAD ESTIMATE (alecmfg, 6 fields): entries=(group,snippet) distinct, first-occurrence location ===")
for run in (OLD,NEW):
    E=0; sc=0; lc=0; G=0; biggest=(0,None)
    for f in FIELDS:
        d=load(run,"alecmfg_com",f)
        for ch in d["chunks"].values():
            for g in ch["fold"]["groups"]:
                if g.get("status")!="ok": continue
                G+=1; seen=set(); gl=0
                for m in g["mentions"]:
                    if m["snippet"] in seen: continue
                    seen.add(m["snippet"]); E+=1; sc+=len(m["snippet"]); ll=len(m["location"]) if m.get("location_source")=="llm" else 0; lc+=ll; gl+=len(m["snippet"])+ll
                if gl>biggest[0]: biggest=(gl,(f,g["key"],len(seen)))
    print(f" {run}: groups {G} entries {E} snippet chars {sc:,} location chars {lc:,} total {sc+lc:,} (locations {lc/(sc+lc):.0%}) | biggest record {biggest}")
print("\n=== 5. OUTPUT TOKENS PER DESCRIBED ITEM, per field ===")
for f in FIELDS:
    for run in (OLD,NEW):
        d=load(run,"alecmfg_com",f); ot=d["run"]["token_usage"]["by_stage"]["llm_phrase_mention_collection"]["output_tokens"]; it=d["run"]["token_usage"]["by_stage"]["llm_phrase_mention_collection"]["input_tokens"]
        desc=sum(ch["fold"]["summary"]["described"] for ch in d["chunks"].values()); ds=sum(ch["fold"]["summary"]["distinct_snippets"] for ch in d["chunks"].values())
        print(f" {f:24s} {run} in {it:6d} out {ot:6d} items {ds:4d} described {desc:4d} -> out/item {ot/max(1,desc):5.1f} in/item {it/max(1,ds):5.1f}")
print("\n=== 6. GROUP PACKING: requests with <10 items (new run) and their input tokens ===")
for f in FIELDS:
    d=load(NEW,"alecmfg_com",f); small=[]
    for cb,ch in d["chunks"].items():
        for sub,reqs in ch["requests"]["llm_phrase_mention_collection"].items():
            w=next(w for w in ch["fold"]["windows"] if w["sub_bounds"]==sub)
            n_items=w["distinct_snippets"]; k=len(reqs)
            sizes=[50]*(k-1)+[n_items-50*(k-1)] if k>1 else [n_items]
            for r,sz in zip(reqs,sizes):
                if sz<10: small.append((sub,sz,r.get("input_tokens"),r.get("output_tokens")))
    print(f" {f:24s} {small}")
print("\n=== 7. STEELCRAFT coverage: old run chunks/windows/excluded ===")
o=load(OLD,"steelcraft_com","products")
print(" old chunks:",list(o["chunks"].keys()))
for cb,ch in o["chunks"].items():
    print("  ",cb,[(w["sub_bounds"],w["distinct_snippets"],w.get("excluded_pages")) for w in ch["fold"]["windows"]], "reqs:",{s:len(r) for s,r in ch["requests"]["llm_phrase_mention_collection"].items()})
print(" old steelcraft text tokens:",o["run"]["scraped_text"]["num_tokens"])
print(" new: chunks 0:63780, 63780:157472 of 224,094 chars; legal windows 63780:90445 (26,665) + 136253:157472 (21,219) blank")
print(f"   new coverage {157472/224094:.0%} of chars; effective (minus blanked) {(157472-26665-21219)/224094:.0%}")
print("\n=== 8. legal-only windows: search usage (Mongo) ===")
docs=json.load(open(f"{SP}/all_requests_034518.json"))
for d in docs:
    c=d["custom_id"]
    if d["subject"]=="steelcraft.com" and ">llm_search>" in c and ("sub>63780:90445" in c or "sub>136253:157472" in c): print("  ",c.split(">")[5].split("|")[0], d["usage"])
# alecmfg search usage per window and mention
tot=collections.Counter()
for d in docs:
    if d["subject"]=="alecmfg.com" and d["usage"]:
        k="mention" if ">llm_phrase_mention_collection>" in d["custom_id"] else "search"
        tot[k+"_in"]+=d["usage"]["prompt_tokens"]; tot[k+"_out"]+=d["usage"]["completion_tokens"]; tot[k+"_n"]+=1
print(" alecmfg Mongo totals:",dict(tot))
