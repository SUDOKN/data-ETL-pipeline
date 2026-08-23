import json, re, sys, os, glob, collections
sys.path.insert(0, "/Users/amaryadav/Documents/ASU_PhD/SUDOKN/data-ETL-pipeline/packages/core/src")
from core.utils.floor_scan import floor_scan
S=os.path.dirname(os.path.abspath(__file__)); ROOT="/Users/amaryadav/Documents/ASU_PhD/SUDOKN/data-ETL-pipeline"
full={s: open(f"{ROOT}/apps/data_etl_app/src/data_etl_app/knowledge/sample_scraped_texts/{s}.txt",encoding="utf-8").read() for s in ["alecmfg.com","steelcraft.com"]}
PRIV="https://www.steelcraft.com/en/privacy-policy.html"
print("privacy page offset in steelcraft text:", full["steelcraft.com"].find(PRIV), "| next URL after it at:", re.search(r"^https?://\S+$", full["steelcraft.com"][full["steelcraft.com"].find(PRIV)+10:], re.M).start()+full["steelcraft.com"].find(PRIV)+10)
print("\n=== (1) forms whose mentions sit on the privacy page, steelcraft, per run/field")
for run in ["20260822T061410","20260822T195947"]:
    for field in ["process_caps","products","industries","conformity_attestations"]:
        d=json.load(open(f"{ROOT}/packages/logs/extraction_dumps/{run}/steelcraft_com__{field}__partial.json"))
        forms=[]; 
        for cb,ch in d["chunks"].items():
            for g in (ch.get("fold") or {}).get("groups",[]):
                if g["mentions"] and all(m.get("page")==PRIV for m in g["mentions"]): forms+=g["forms"]
        print(f"  {run[-6:]} {field:24} n={len(forms):3} e.g. {forms[:12]}")
print("\n=== (2) NEW run: non-bare unanchored samples (reported_form -> snippet)")
for field in ["material_caps","products","industries"]:
    for subj in ["steelcraft_com","alecmfg_com"]:
        p=f"{ROOT}/packages/logs/extraction_dumps/20260822T195947/{subj}__{field}__partial.json"
        if not os.path.exists(p): continue
        d=json.load(open(p)); c=collections.Counter(); ex={}
        for cb,ch in d["chunks"].items():
            for w in (ch.get("fold") or {}).get("windows",[]):
                for u in w.get("unanchored") or []:
                    if u["snippet"].strip()!=u["reported_form"].strip():
                        k=u["reported_form"]; c[k]+=1; ex.setdefault(k, u["snippet"][:90].replace("\n","⏎"))
        if c: print(f"  {subj[:9]} {field:14} other={sum(c.values())} top: {[(f,n,ex[f]) for f,n in c.most_common(4)]}")
print("\n=== (3) NEW run: alecmfg material_caps worst windows — skipped forms and where their hits sit")
MARK="text scraped from a manufacturer's website:\n"
reqs=[r for r in json.load(open(f"{S}/all_requests.json")) if r.get("custom_id") and ">llm_phrase_mention_collection>" in r["custom_id"] and r["custom_id"].startswith("alecmfg.com>material_caps>")]
def ltype(text,start):
    ls=text.rfind("\n",0,start)+1; le=text.find("\n",start); le=len(text) if le<0 else le; line=text[ls:le]; w=len(line.split())
    rep=text.count(line.strip()) if len(line.strip())>=12 else 1
    t="prose" if (w>=12 and re.search(r"[.!?]",line)) else "short/heading/list"
    return t+(" (rep)" if rep>=2 else ""), line[:110].replace("\n","⏎")
for r in reqs:
    um=r["user_message"] or ""
    if MARK not in um: continue
    m=re.search(r">sub>([^>]+)>group>(\d+)>",r["custom_id"]); sub,gi=m.groups()
    if sub not in ("98612:123329","123329:142870","73263:98227"): continue
    a=um.index("<<<PHRASES")+len("<<<PHRASES"); b=um.index("PHRASES>>>"); sent=json.loads(um[a:b].strip()); text=um.split(MARK,1)[1].rsplit("<<<PHRASES",1)[0]
    scan=floor_scan(text,sent); by={f["form"]:(f.get("mentions") or []) for f in json.loads(r["content"])["forms"]}
    skipped=[f for f in sent if scan.tier1[f] and not by.get(f)]; answered=[f for f in sent if by.get(f)]
    print(f"\n  window {sub} g{gi}: sent {len(sent)}, answered {len(answered)} {answered[:10]}")
    print(f"    skipped {len(skipped)}: " + "; ".join(f"{f}×{len(scan.tier1[f])}" for f in skipped))
    lt=collections.Counter()
    for f in skipped:
        for h in scan.tier1[f]: lt[ltype(text,h.start)[0]]+=1
    print("    skipped hits by line type:", dict(lt))
    for f in skipped[:4]:
        h=scan.tier1[f][0]; t,line=ltype(text,h.start); print(f"      {f!r} first hit [{t}]: {line}")
    # what do the answered ones look like — line types of reported snippets
    lt2=collections.Counter()
    for f in answered:
        for mm in by[f]:
            pos=text.find(mm.get("snippet","")); 
            if pos>=0: lt2[ltype(text,pos)[0]]+=1
    print("    answered mentions by line type:", dict(lt2))
