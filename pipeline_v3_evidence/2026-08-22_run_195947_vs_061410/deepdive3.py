import json, re, sys, os, glob, collections, statistics
sys.path.insert(0, "/Users/amaryadav/Documents/ASU_PhD/SUDOKN/data-ETL-pipeline/packages/core/src")
from core.utils.floor_scan import find_form_occurrences
S=os.path.dirname(os.path.abspath(__file__)); ROOT="/Users/amaryadav/Documents/ASU_PhD/SUDOKN/data-ETL-pipeline"
print("=== (A) UNANCHORED composition per run: distinct (form,snippet) pairs; casing-only (form occurs in snippet case-insensitively as whole word, not exact-case)")
for run in ["20260822T061410","20260822T195947"]:
    tot=0; pairs=collections.Counter(); casing=0; exact_in=0; none=0
    for p in glob.glob(f"{ROOT}/packages/logs/extraction_dumps/{run}/*__partial.json"):
        d=json.load(open(p))
        if d["field_type"]=="contract_products": continue
        for cb,ch in d["chunks"].items():
            for w in (ch.get("fold") or {}).get("windows",[]):
                for u in w.get("unanchored") or []:
                    tot+=1; pairs[(d["field_type"],u["reported_form"],u["snippet"])]+=1
                    f=u["reported_form"]; sn=u["snippet"]
                    if find_form_occurrences(sn,f,case_sensitive=True): exact_in+=1
                    elif find_form_occurrences(sn,f,case_sensitive=False): casing+=1
                    else: none+=1
    top=pairs.most_common(5)
    print(f"  {run[-6:]}: unanchored {tot} from {len(pairs)} distinct (field,form,snippet) pairs; top-5 pairs cover {sum(n for _,n in top)}; exact-case form inside snippet (boundary-rejected, e.g. Steel|craft) {exact_in}; CASING-ONLY {casing}; form absent {none}")
    for (f,form,sn),n in top: print(f"      ×{n:3d} {f[:12]} {form!r} <- {sn[:70].replace(chr(10),'⏎')!r}")
print("\n=== (H) locations carrying a URL, per run")
for label,path in [("prev",f"{S}/mention_requests_prev.json"),("new",f"{S}/all_requests.json")]:
    n=0; url=0; onpage=0; L=[]
    for r in json.load(open(path)):
        if not r.get("custom_id") or ">llm_phrase_mention_collection>" not in r["custom_id"] or not r.get("content"): continue
        try: forms=json.loads(r["content"])["forms"]
        except Exception: continue
        for f in forms:
            for m in f.get("mentions") or []:
                lc=m.get("location","") or ""; n+=1; L.append(len(lc))
                if re.search(r"https?://",lc): url+=1
                if lc.lower().startswith("on the page"): onpage+=1
    print(f"  {label}: mentions {n}; locations with a URL {url} ({url/n:.0%}); starting 'On the page' {onpage}; median chars {statistics.median(L):.0f}; total location chars {sum(L):,}")
print("\n=== (I) token cost of the privacy-page windows (steelcraft 70223:96297 + 96297:97071 fully inside; 46521:70223 and 97071:... partially) by stage, new run")
cost=collections.Counter(); tot=collections.Counter()
for r in json.load(open(f"{S}/all_requests.json")):
    cid=r.get("custom_id") or ""; u=r.get("usage") or {}
    stage="mention" if ">llm_phrase_mention_collection>" in cid else "search"
    tot[(stage,"in")]+=u.get("prompt_tokens",0); tot[(stage,"out")]+=u.get("completion_tokens",0)
    if cid.startswith("steelcraft.com>") and (">sub>70223:96297>" in cid or ">sub>96297:97071>" in cid):
        cost[(stage,"in")]+=u.get("prompt_tokens",0); cost[(stage,"out")]+=u.get("completion_tokens",0); cost[(stage,"n")]+=1
print("  fully-privacy windows:", dict(cost), "| stage totals (both subjects):", dict(tot))
print("\n=== sanity: new run mention requests per subject/field (real) and total groups")
c=collections.Counter()
for r in json.load(open(f"{S}/all_requests.json")):
    cid=r.get("custom_id") or ""
    if ">llm_phrase_mention_collection>" in cid and "<<<PHRASES" in (r.get("user_message") or ""): c[tuple(cid.split(">")[:2])]+=1
print("  ", dict(c))
