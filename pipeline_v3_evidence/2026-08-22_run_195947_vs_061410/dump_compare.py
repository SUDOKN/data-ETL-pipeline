"""Dump-only side-by-side of runs 20260822T061410 (prev) and 20260822T195947 (new)."""
import json, glob, os, statistics as st, collections
ROOT="/Users/amaryadav/Documents/ASU_PhD/SUDOKN/data-ETL-pipeline/packages/logs/extraction_dumps"
RUNS={"prev":"20260822T061410","new":"20260822T195947"}
FIELDS=["conformity_attestations","industries","material_caps","process_caps","products","contract_products","equipments"]
def words(s): return len(s.split())
def analyze(run):
    R={}
    for path in sorted(glob.glob(f"{ROOT}/{run}/*__partial.json")):
        base=os.path.basename(path); subj,rest=base.split("__",1); field=rest.replace("__partial.json","")
        if field not in FIELDS: continue
        d=json.load(open(path)); a=collections.Counter()
        forms_all=set(); empty_forms=[]; ok_forms=[]; nl_forms=set()
        unacc_by_form=collections.Counter(); reported=set(); unanch=[]; rekeyed=[]
        win_sizes=[]; chunk_sizes=[]; prov=collections.Counter(); rows_n=0; tok=collections.Counter()
        seen_abs=collections.defaultdict(set); dup_chunk=0; dup_win=0; group_sizes=[]; multi=[]; disc=[]
        for cb,ch in d["chunks"].items():
            cs,ce=map(int,cb.split(":")); chunk_sizes.append(ce-cs)
            rows=ch.get("rows",[]); rows_n+=len(rows)
            for r in rows: prov[r.get("provenance")]+=1
            for stage,v in (ch.get("requests") or {}).items():
                entries=v if isinstance(v,list) else [e for lst in v.values() for e in lst]
                for e in entries: tok[(stage,"in")]+=(e.get("input_tokens") or 0); tok[(stage,"out")]+=(e.get("output_tokens") or 0); tok[(stage,"n")]+=1
            fold=ch.get("fold")
            if not fold: a["no_fold"]+=1; continue
            s=fold["summary"]
            for k in ("groups","empty_groups","mentions","windows","candidates","obligations","unaccounted","windows_with_discrepancy","unlocated","unanchored","rekeyed","discovered_casings"): a[k]+=s.get(k,0)
            wb=[tuple(map(int,w["sub_bounds"].split(":"))) for w in fold["windows"]]
            win_sizes+= [y-x for x,y in wb]
            for w in fold["windows"]:
                sf=w["sent_forms"]; a["sent_forms_total"]+= sf if isinstance(sf,int) else len(sf)
                for f,spans in (w.get("unaccounted") or {}).items(): unacc_by_form[(cb,f)]+=len(spans)
                unanch+= w.get("unanchored") or []; rekeyed+= w.get("rekeyed") or []
                dc=w.get("discovered_casings") or {}
                if isinstance(dc,dict): disc+= [(k,v) for k,v in dc.items()]
            for g in fold["groups"]:
                group_sizes.append(len(g["forms"]))
                if len(g["forms"])>1: multi.append((g["key"],g["forms"],g["mention_count"]))
                for f in g["forms"]:
                    forms_all.add(f)
                    if "\n" in f: nl_forms.add(f)
                    (empty_forms if g["status"]=="no_mentions" else ok_forms).append(f)
                starts=collections.Counter()
                for m in g["mentions"]:
                    reported.add((cb,m["form"])); wi=m.get("window"); sp=m.get("span")
                    if wi is None or sp is None: continue
                    absst=wb[wi][0]+sp[0]; starts[absst]+=1
                    if absst in seen_abs[g["key"]]: dup_chunk+=1
                    seen_abs[g["key"]].add(absst)
                dup_win+=sum(c-1 for c in starts.values() if c>1)
        sat=sum(c for (cb,f),c in unacc_by_form.items() if (cb,f) in reported); missed=sum(unacc_by_form.values())-sat
        long_forms=[f for f in empty_forms+ok_forms if words(f)>=6]; long_empty=[f for f in empty_forms if words(f)>=6]
        rc=sum(1 for r in rekeyed if any(x.casefold()==r["reported_form"].casefold() for x in r["attributed_forms"]))
        rs=sum(1 for r in rekeyed if any(r["reported_form"] in x and x!=r["reported_form"] for x in r["attributed_forms"]))
        frag=sum(1 for r in unanch if r["snippet"].strip()==r["reported_form"].strip())
        R[(subj,field)]=dict(a=a,n_forms=len(forms_all),L=sorted(words(f) for f in forms_all),nl=len(nl_forms),sat=sat,missed=missed,long_forms=len(long_forms),long_empty=len(long_empty),
            rekey_case=rc,rekey_swal=rs,rekey_other=len(rekeyed)-rc-rs,unanch_frag=frag,unanch_n=len(unanch),win_sizes=win_sizes,chunk_sizes=chunk_sizes,prov=prov,rows_n=rows_n,tok=tok,
            dup_chunk=dup_chunk,dup_win=dup_win,group_sizes=group_sizes,multi=multi,run_tok=d["run"].get("token_usage"),run_time=(d["run"].get("time_span") or {}).get("seconds"),
            forms_all=forms_all,top_unacc=unacc_by_form.most_common(4),disc=disc,rekeyed_other_ex=[(r["reported_form"][:30],r["attributed_forms"][:2]) for r in rekeyed if not any(x.casefold()==r["reported_form"].casefold() for x in r["attributed_forms"]) and not any(r["reported_form"] in x for x in r["attributed_forms"])][:3])
    return R
P=analyze(RUNS["prev"]); N=analyze(RUNS["new"])
keys=sorted(set(P)|set(N))
def pct(x,y): return f"{(x/y*100) if y else 0:4.0f}%"
print("=== GEOMETRY (new run): chunk sizes / window sizes per subject (process_caps as representative)")
for k in keys:
    if k[1]=="process_caps":
        for lab,R in (("prev",P),("new",N)):
            r=R.get(k); 
            if r: print(f"  {lab} {k[0]:14} chunks {r['chunk_sizes']} windows(min/med/max) {min(r['win_sizes'])}/{int(st.median(r['win_sizes']))}/{max(r['win_sizes'])} n={len(r['win_sizes'])} sizes={r['win_sizes']}")
print("\n=== PER SUBJECT/FIELD: prev -> new   (forms=unique sent forms; empty%=empty groups/groups; unacc%=unaccounted/obligations)")
hdr=f"{'subj':9} {'field':14} | {'forms':>11} | {'groups':>11} | {'empty%':>11} | {'mentions':>11} | {'oblig':>11} | {'unacc%':>11} | {'unloc':>9} | {'unanch':>9} | {'rekey':>9} | {'disc':>9} | {'dupW/dupC':>9}"
print(hdr)
for k in keys:
    p=P.get(k); n=N.get(k)
    if not p or not n: print(k, "missing in one run"); continue
    pa,na=p["a"],n["a"]
    print(f"{k[0][:9]:9} {k[1][:14]:14} | {p['n_forms']:5}->{n['n_forms']:5} | {pa['groups']:5}->{na['groups']:5} | {pct(pa['empty_groups'],pa['groups'])}->{pct(na['empty_groups'],na['groups'])} | {pa['mentions']:5}->{na['mentions']:5} | {pa['obligations']:5}->{na['obligations']:5} | {pct(pa['unaccounted'],pa['obligations'])}->{pct(na['unaccounted'],na['obligations'])} | {pa['unlocated']:3}->{na['unlocated']:3} | {pa['unanchored']:3}->{na['unanchored']:3} | {pa['rekeyed']:3}->{na['rekeyed']:3} | {pa['discovered_casings']:3}->{na['discovered_casings']:3} | {p['dup_win']}/{p['dup_chunk']}->{n['dup_win']}/{n['dup_chunk']}")
def tot(R,key): return sum(r["a"][key] for r in R.values())
print("\n=== TOTALS over the 14 field-subjects")
for key in ["sent_forms_total","groups","empty_groups","mentions","candidates","obligations","unaccounted","unlocated","unanchored","rekeyed","discovered_casings","windows","windows_with_discrepancy"]:
    print(f"  {key:26} {tot(P,key):6} -> {tot(N,key):6}")
print(f"  {'unique forms':26} {sum(r['n_forms'] for r in P.values()):6} -> {sum(r['n_forms'] for r in N.values()):6}")
print(f"  {'unaccounted/obligations':26} {pct(tot(P,'unaccounted'),tot(P,'obligations'))} -> {pct(tot(N,'unaccounted'),tot(N,'obligations'))}")
print(f"  {'empty/groups':26} {pct(tot(P,'empty_groups'),tot(P,'groups'))} -> {pct(tot(N,'empty_groups'),tot(N,'groups'))}")
print(f"  {'dup occ (win / chunk)':26} {sum(r['dup_win'] for r in P.values())}/{sum(r['dup_chunk'] for r in P.values())} -> {sum(r['dup_win'] for r in N.values())}/{sum(r['dup_chunk'] for r in N.values())}")
print(f"  {'satisficing vs missed':26} {sum(r['sat'] for r in P.values())}/{sum(r['missed'] for r in P.values())} -> {sum(r['sat'] for r in N.values())}/{sum(r['missed'] for r in N.values())}")
print(f"  {'rekey case/swallowed/other':26} {sum(r['rekey_case'] for r in P.values())}/{sum(r['rekey_swal'] for r in P.values())}/{sum(r['rekey_other'] for r in P.values())} -> {sum(r['rekey_case'] for r in N.values())}/{sum(r['rekey_swal'] for r in N.values())}/{sum(r['rekey_other'] for r in N.values())}")
print(f"  {'unanchored frag(snip==form)':26} {sum(r['unanch_frag'] for r in P.values())}/{sum(r['unanch_n'] for r in P.values())} -> {sum(r['unanch_frag'] for r in N.values())}/{sum(r['unanch_n'] for r in N.values())}")
print(f"  {'newline forms':26} {sum(r['nl'] for r in P.values())} -> {sum(r['nl'] for r in N.values())}")
print(f"  {'>=6w forms (empty/all)':26} {sum(r['long_empty'] for r in P.values())}/{sum(r['long_forms'] for r in P.values())} -> {sum(r['long_empty'] for r in N.values())}/{sum(r['long_forms'] for r in N.values())}")
pp=collections.Counter(); 
for r in P.values(): pp.update(r["prov"])
pn=collections.Counter(); 
for r in N.values(): pn.update(r["prov"])
print(f"  {'search rows by provenance':26} {dict(pp)} -> {dict(pn)}")
print("\n=== TOKENS + TIME (sum over the 14 dumps' request witnesses)")
for lab,R in (("prev",P),("new",N)):
    t=collections.Counter()
    for r in R.values(): t.update(r["tok"])
    stages=sorted({k[0] for k in t})
    print(f"  {lab}: "+" | ".join(f"{s.replace('llm_phrase_','')}: n={t[(s,'n')]} in={t[(s,'in')]:,} out={t[(s,'out')]:,}" for s in stages)+f" | wall sum {sum(r['run_time'] or 0 for r in R.values())}s")
print("\n=== FORM LENGTH (words) per field, unique forms both subjects: prev -> new")
for field in FIELDS:
    for lab,R in (("prev",P),("new",N)):
        L=sorted(x for (s,f),r in R.items() if f==field for x in r["L"])
        if not L: continue
        p=lambda q: L[min(len(L)-1,int(q*len(L)))]
        print(f"  {lab} {field:24} n={len(L):5} mean={st.mean(L):4.1f} med={p(.5)} p90={p(.9)} max={L[-1]:3}  >=4w={sum(1 for x in L if x>=4)/len(L)*100:4.1f}%  >=6w={sum(1 for x in L if x>=6)/len(L)*100:4.1f}%")
print("\n=== NEW run: longest forms per field (top 4)")
for field in FIELDS:
    forms=set().union(*[r["forms_all"] for (s,f),r in N.items() if f==field]) if any(f==field for (s,f) in N) else set()
    for f in sorted(forms,key=words,reverse=True)[:4]: print(f"  {field[:14]:14} [{words(f):2}w] {f[:140]!r}")
print("\n=== NEW run: top unaccounted forms per field-subject")
for k in keys:
    n=N.get(k); 
    if n: print(f"  {k[0][:9]:9} {k[1][:14]:14} sat={n['sat']:3} missed={n['missed']:3} top={[(f[:28],c) for (cb,f),c in n['top_unacc']]}")
print("\n=== NEW run: rekey 'other' samples + group-size distribution")
for k in keys:
    n=N.get(k)
    if n and n["rekeyed_other_ex"]: print(f"  {k[0][:9]:9} {k[1][:14]:14} other={n['rekey_other']} e.g. {n['rekeyed_other_ex'][:2]}")
for lab,R in (("prev",P),("new",N)):
    G=[g for r in R.values() for g in r["group_sizes"]]; c=collections.Counter(min(g,3) for g in G)
    print(f"  {lab}: groups={len(G)} 1-form={c[1]/len(G)*100:.1f}% 2-form={c[2]/len(G)*100:.1f}% 3+={c[3]/len(G)*100:.1f}% max={max(G)}")
print("\n=== NEW run: largest multi-form groups (sample)")
allm=sorted([(k[1],)+m for k,r in N.items() for m in r["multi"]], key=lambda t:-len(t[2]))[:10]
for f,key,forms,mc in allm: print(f"  {f[:14]:14} key={key!r} mentions={mc} forms={forms[:6]}{'...' if len(forms)>6 else ''}")
