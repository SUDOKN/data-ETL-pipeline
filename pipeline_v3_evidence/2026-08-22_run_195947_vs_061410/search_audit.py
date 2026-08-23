"""Search-stage audit of run 20260822T195947 from the stored requests (all_requests.json)."""
import json, re, sys, collections, statistics
sys.path.insert(0, "/Users/amaryadav/Documents/ASU_PhD/SUDOKN/data-ETL-pipeline/packages/core/src")
from core.utils.floor_scan import find_form_occurrences, mask_page_headers
import os
S=os.path.dirname(os.path.abspath(__file__))
reqs=[r for r in json.load(open(f"{S}/all_requests.json")) if r.get("custom_id") and ">llm_search>chunk>" in r["custom_id"]]
def wtext(um):
    body=um.split("\n",1)[1] if re.match(r"^[0-9a-f]{32}\n", um) else um
    mk="text scraped from a manufacturer's website:\n"
    if mk in body: body=body.split(mk,1)[1]
    return body.rsplit("<<<",1)[0] if "<<<" in body else body
tot=collections.Counter(); byfield=collections.defaultdict(collections.Counter); ex=collections.defaultdict(list)
per_win=[]; lens=collections.defaultdict(list); nl_forms=[]; dups=0; loops=[]; unparsed=0; empty_windows=[]; sliver=[]
for r in reqs:
    cid=r["custom_id"]; m=re.search(r"^([^>]+)>([^>]+)>llm_search>chunk>([^>]+)>sub>([^>]+)>",cid); subj,field,chunk,sub=m.groups()
    text=wtext(r["user_message"]); dom=mask_page_headers(text); u=r["usage"] or {}
    try: phrases=json.loads(r["content"])["phrases"]
    except Exception: unparsed+=1; loops.append((subj,field,sub,"UNPARSED",u.get("completion_tokens"))); continue
    if u.get("completion_tokens",0)>=3900: loops.append((subj,field,sub,"near-cap",u.get("completion_tokens")))
    ph=[p for p in phrases if isinstance(p,str)]
    per_win.append((subj,field,sub,len(text),len(ph),u.get("prompt_tokens"),u.get("completion_tokens")))
    if len(text)<3000: sliver.append((subj,field,sub,len(text),len(ph),u.get("prompt_tokens"),u.get("completion_tokens"), ph[:6]))
    if not ph: empty_windows.append((subj,field,sub,len(text)))
    dups+=len(ph)-len(set(ph))
    for p in ph:
        lens[field].append(len(p.split()))
        if "\n" in p: nl_forms.append((field,p[:60]))
        if find_form_occurrences(dom,p,case_sensitive=True): c="exact"
        elif find_form_occurrences(dom,p,case_sensitive=False): c="casing-variant only"
        elif find_form_occurrences(dom,p.strip(),case_sensitive=False): c="whitespace"
        elif p.lower() in dom.lower(): c="substring (not whole-word)"
        elif find_form_occurrences(text,p,case_sensitive=False): c="only in URL/header lines"
        else: c="NOT IN WINDOW"
        tot[c]+=1; byfield[field][c]+=1
        if c in ("NOT IN WINDOW","substring (not whole-word)","only in URL/header lines") and len(ex[(field,c)])<3: ex[(field,c)].append(p[:60])
n=sum(tot.values())
print(f"SEARCH requests {len(reqs)}; unparsed {unparsed}; phrases returned {n}; within-window exact duplicates {dups}; newline forms {len(nl_forms)} {nl_forms[:3]}")
for c,v in sorted(tot.items(), key=lambda x:-x[1]): print(f"   {c:28s} {v:5d} ({v/n:.0%})")
print("by field (not-in-window / total):", {f: f"{byfield[f]['NOT IN WINDOW']}/{sum(byfield[f].values())}" for f in sorted(byfield)})
for k,v in sorted(ex.items()): print("   e.g.", k, v)
print(f"\nloops / near-cap / unparsed: {loops}")
print(f"empty windows (0 phrases): {len(empty_windows)} {empty_windows}")
print(f"sliver windows (<3000 chars): {len(sliver)}")
for s in sliver: print("   ", s)
print(f"   sliver cost: prompt {sum(s[5] or 0 for s in sliver):,} / completion {sum(s[6] or 0 for s in sliver):,} tokens of search; total search prompt {sum(p[5] or 0 for p in per_win):,}")
print("\nphrases per window: median", statistics.median(p[4] for p in per_win), "max", max(p[4] for p in per_win), "| per field median:", {f: statistics.median(p[4] for p in per_win if p[1]==f) for f in sorted({p[1] for p in per_win})})
print("\nSEARCH OUTPUT form length (words) per field:")
for f,L in sorted(lens.items()):
    L.sort(); p=lambda q: L[min(len(L)-1,int(q*len(L)))]
    print(f"   {f:24} n={len(L):5} mean={statistics.mean(L):4.1f} med={p(.5)} p90={p(.9)} max={L[-1]:3} >=6w={sum(1 for x in L if x>=6)/len(L)*100:4.1f}%")
# per-subject prompt tokens in search by field
st=collections.Counter()
for p in per_win: st[(p[0],p[1])]+= p[5] or 0
print("\nsearch prompt tokens per subject/field:", dict(st))
