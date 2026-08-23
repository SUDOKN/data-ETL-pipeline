import json, glob, os, re, statistics as st, collections
ROOT="/Users/amaryadav/Documents/ASU_PhD/SUDOKN/data-ETL-pipeline/packages/logs/extraction_dumps"
NEW="20260823T034518"; OLD="20260822T223715"
FIELDS=["products","contract_products","equipments","conformity_attestations","industries","process_caps","material_caps"]
def load(run, subj, field):
    p=f"{ROOT}/{run}/{subj}__{field}__partial.json"
    return json.load(open(p)) if os.path.exists(p) else None
def s(d,*ks,default=None):
    for k in ks:
        if not isinstance(d,dict) or k not in d: return default
        d=d[k]
    return d
URL=re.compile(r"https?://\S+")
REL=re.compile(r"\b(previous|preceding|following|same (testimonial|section|paragraph|list|page|passage)|another mention|other mention|above mention|earlier mention|the first mention|the second mention|immediately after|immediately before|directly after|directly before)\b", re.I)
def field_report(run, subj, field):
    d=load(run,subj,field)
    if d is None: return None
    r={}
    r["pv_meta"]=s(d,"run","extraction_metadata","llm_phrase_mention_collection","prompt_version_id")
    r["pv_search_meta"]=s(d,"run","extraction_metadata","llm_phrase_search","prompt_version_id")
    r["chunk_strat"]=s(d,"run","extraction_metadata","chunk_strat")
    r["mention_meta"]={k:v for k,v in (s(d,"run","extraction_metadata","llm_phrase_mention_collection") or {}).items() if k not in ("llm_model","model_params","prompt_name")}
    r["tokens"]=s(d,"run","token_usage")
    r["time"]=s(d,"run","time_span","seconds")
    r["num_tokens_text"]=s(d,"run","scraped_text","num_tokens")
    chunks=d.get("chunks",{})
    r["chunks"]=list(chunks.keys())
    tot=collections.Counter(); wins=[]; groups=[]; reqs=collections.Counter(); retry_ids=[]; synth=0; req_rows=[]
    locs=[]; nd_ids=[]; unknown=[]; retried=[]; excluded=set(); zero_hit=[]; disc=0; rows_prov=collections.Counter()
    pv_ids=set(); per_req_items=[]
    for cb,ch in chunks.items():
        for row in ch.get("rows",[]): rows_prov[row.get("provenance")]+=1
        rq=ch.get("requests",{})
        for stage,v in rq.items():
            lst = v if isinstance(v,list) else [x for sub in v.values() for x in (sub if isinstance(sub,list) else [sub])]
            for x in lst:
                reqs[stage]+=1
                cid=x.get("custom_id","")
                if ">retry>" in cid: retry_ids.append(cid)
                if x.get("synthetic_response") or x.get("input_tokens") is None: synth+=1
                m=re.search(r"pv=([^|]+)",cid); 
                if m: pv_ids.add((stage,m.group(1)))
                req_rows.append((stage,cid,x.get("input_tokens"),x.get("output_tokens"),x.get("client_latency_ms")))
        f=ch.get("fold",{})
        sm=f.get("summary",{})
        for k,v in sm.items():
            if isinstance(v,(int,float)): tot[k]+=v
        for w in f.get("windows",[]):
            wins.append(w)
            nd=w.get("not_described") or []
            nd_ids.extend(nd if isinstance(nd,list) else [])
            u=w.get("unknown_answer_ids") or w.get("unknown_ids") or []
            if u: unknown.append((cb,w.get("sub_bounds"),len(u) if isinstance(u,list) else u))
            if w.get("retried"): retried.append((cb,w.get("sub_bounds"),w.get("retried")))
            for e in (w.get("excluded_pages") or []): excluded.add(e)
            zh=w.get("zero_hit_forms") or []
            zero_hit.extend(zh)
            dc=w.get("discovered_casings") or {}
            disc+= sum(len(v) for v in dc.values()) if isinstance(dc,dict) else len(dc)
        for g in f.get("groups",[]):
            groups.append(g)
            for m in g.get("mentions",[]):
                if m.get("location_source")=="llm": locs.append(m.get("location",""))
    r["requests"]=dict(reqs); r["synthetic"]=synth; r["retry_ids"]=len(retry_ids); r["retry_id_list"]=retry_ids
    r["pv_ids"]=sorted(pv_ids)
    r["summary_tot"]=dict(tot)
    r["windows"]=[(w.get("sub_bounds"),w.get("sent_forms"),w.get("forms_with_hits"),len(w.get("zero_hit_forms") or []),w.get("mentions"),w.get("distinct_snippets"),w.get("described"),len(w.get("not_described") or []),w.get("retried"),(len(w.get("unknown_answer_ids") or []) if isinstance(w.get("unknown_answer_ids"),list) else w.get("unknown_answer_ids")),len(w.get("excluded_pages") or [])) for w in wins]
    r["window_keys"]=sorted(set(k for w in wins for k in w.keys()))
    r["groups_n"]=len(groups); r["empty_groups"]=sum(1 for g in groups if g.get("status")=="no_mentions")
    r["rows_prov"]=dict(rows_prov)
    r["excluded"]=sorted(excluded); r["zero_hit_forms"]=zero_hit; r["discovered_casings"]=disc
    r["unknown"]=unknown; r["retried"]=retried
    # location stats (dedupe by (mention_id,location) not available here; use raw list)
    if locs:
        L=[len(x) for x in locs]
        r["loc_n"]=len(locs); r["loc_median"]=st.median(L); r["loc_p90"]=sorted(L)[int(len(L)*0.9)]; r["loc_max"]=max(L)
        r["loc_url_share"]=sum(1 for x in locs if URL.search(x))/len(locs)
        r["loc_url_chars"]=sum(len(m.group(0)) for x in locs for m in URL.finditer(x))/max(1,sum(L))
        r["loc_rel"]=sum(1 for x in locs if REL.search(x))/len(locs)
        r["loc_prefix_this_passage"]=sum(1 for x in locs if x.lower().startswith("this passage"))/len(locs)
        r["loc_mentions_continued"]=sum(1 for x in locs if "began before" in x or "not included" in x or "continued" in x.lower())/len(locs)
    r["req_rows"]=req_rows
    return r
out={}
for run in (OLD,NEW):
    for subj in ("alecmfg_com","steelcraft_com"):
        for fld in FIELDS:
            rep=field_report(run,subj,fld)
            if rep: out[(run,subj,fld)]=rep
def P(*a): print(*a)
P("RUN-LEVEL (alecmfg):")
for fld in FIELDS:
    o=out.get((OLD,"alecmfg_com",fld)); n=out.get((NEW,"alecmfg_com",fld))
    if not n: P(fld,"MISSING in new"); continue
    P(f"\n== {fld} ==")
    P(" chunks old",o["chunks"] if o else None,"| new",n["chunks"])
    P(" chunk_strat new",n["chunk_strat"])
    P(" mention_meta new",n["mention_meta"])
    P(" pv meta mention old",o["pv_meta"] if o else None,"new",n["pv_meta"]," | pv in ids new",n["pv_ids"])
    P(" text tokens", n["num_tokens_text"], "time s old",o["time"] if o else None,"new",n["time"])
    P(" tokens old",o["tokens"] if o else None)
    P(" tokens new",n["tokens"])
    P(" requests old",o["requests"] if o else None,"synthetic",o["synthetic"] if o else None," | new",n["requests"],"synthetic",n["synthetic"],"retry ids",n["retry_ids"])
    P(" summary old",o["summary_tot"] if o else None)
    P(" summary new",n["summary_tot"])
    P(" groups old",(o["groups_n"],o["empty_groups"]) if o else None,"new",(n["groups_n"],n["empty_groups"]),"rows_prov new",n["rows_prov"])
    P(" windows new: (sub_bounds, sent, with_hits, zero_hit, mentions, distinct, described, not_described, retried, unknown, excluded)")
    for w in n["windows"]: P("   ",w)
    if o:
        P(" windows old:"); 
        for w in o["windows"]: P("   ",w)
    P(" window keys new",n["window_keys"])
    P(" excluded new",n["excluded"]); P(" zero_hit new",n["zero_hit_forms"][:20],"(n=%d)"%len(n["zero_hit_forms"]), "disc casings",n["discovered_casings"])
    P(" unknown new",n["unknown"],"retried",n["retried"],"retry ids",n["retry_id_list"][:3])
    for k in ("loc_n","loc_median","loc_p90","loc_max","loc_url_share","loc_url_chars","loc_rel","loc_prefix_this_passage","loc_mentions_continued"):
        P(f"  {k}: old={o.get(k) if o else None} new={n.get(k)}")
    P(" per-request new (stage, in, out, latency):")
    for st_,cid,i,oo,lat in n["req_rows"]:
        if "mention" in st_: P("   ",cid.split(">",3)[3][:60], i,oo,lat)
json.dump({f"{k[0]}|{k[1]}|{k[2]}":{kk:vv for kk,vv in v.items() if kk!="req_rows"} for k,v in out.items()}, open(os.path.dirname(os.path.abspath(__file__))+"/dump_compare_out.json","w"), default=str)
