import json, os, re, statistics as st, collections
ROOT="/Users/amaryadav/Documents/ASU_PhD/SUDOKN/data-ETL-pipeline/packages/logs/extraction_dumps"
NEW="20260823T044500"
PREV={"alecmfg_com":"20260823T034518","steelcraft_com":"20260822T223715"}
FIELDS=["products","contract_products","equipments","conformity_attestations","industries","process_caps","material_caps"]
URL=re.compile(r"https?://\S+")
REL=re.compile(r"\b(previous|preceding|following|same (testimonial|section|paragraph|list|page|passage)|another mention|other mention|above mention|earlier mention|the first mention|the second mention|immediately after|immediately before|directly after|directly before)\b", re.I)
def load(run,subj,field):
    p=f"{ROOT}/{run}/{subj}__{field}__partial.json"
    return json.load(open(p)) if os.path.exists(p) else None
def s(d,*ks,default=None):
    for k in ks:
        if not isinstance(d,dict) or k not in d: return default
        d=d[k]
    return d
def rep(run,subj,field):
    d=load(run,subj,field)
    if d is None: return None
    r={"run":run}
    r["chunk_strat"]=s(d,"run","extraction_metadata","chunk_strat")
    r["pv_mention"]=s(d,"run","extraction_metadata","llm_phrase_mention_collection","prompt_version_id")
    r["pv_search"]=s(d,"run","extraction_metadata","llm_phrase_search","prompt_version_id")
    r["tokens"]=s(d,"run","token_usage")
    r["time"]=s(d,"run","time_span","seconds")
    r["by_stage_time"]=s(d,"run","time_span","by_stage")
    r["text_tokens"]=s(d,"run","scraped_text","num_tokens")
    r["excl"]=s(d,"run","scraped_text","excluded_pages")
    chunks=d.get("chunks",{})
    r["chunks"]=list(chunks.keys())
    tot=collections.Counter(); wins=[]; groups=[]; reqs=collections.Counter(); synth=0; req_rows=[]
    locs=[]; snips=[]; rows_prov=collections.Counter(); retry_ids=[]
    for cb,ch in chunks.items():
        for row in ch.get("rows",[]): rows_prov[row.get("provenance")]+=1
        for stage,v in ch.get("requests",{}).items():
            lst = v if isinstance(v,list) else [x for sub in v.values() for x in (sub if isinstance(sub,list) else [sub])]
            for x in lst:
                reqs[stage]+=1
                cid=x.get("custom_id","")
                if ">retry>" in cid or "retry" in stage: retry_ids.append(cid)
                if x.get("input_tokens") is None: synth+=1
                req_rows.append((stage,cid,x.get("input_tokens"),x.get("output_tokens"),x.get("client_latency_ms")))
        f=ch.get("fold",{})
        for k,v in f.get("summary",{}).items():
            if isinstance(v,(int,float)): tot[k]+=v
        for w in f.get("windows",[]): wins.append((cb,w))
        for g in f.get("groups",[]):
            groups.append(g)
            for m in g.get("mentions",[]):
                if m.get("location_source")=="llm": locs.append(m.get("location",""))
                snips.append((g.get("group_id"),m.get("snippet","")))
    r["requests"]=dict(reqs); r["synthetic"]=synth; r["retry_reqs"]=len(retry_ids)
    r["summary"]=dict(tot)
    r["groups_n"]=len(groups); r["empty_groups"]=sum(1 for g in groups if g.get("status")=="no_mentions")
    r["rows_prov"]=dict(rows_prov)
    r["windows"]=[(cb,w.get("sub_bounds"),w.get("sent_forms"),w.get("forms_with_hits"),len(w.get("zero_hit_forms") or []),
                   w.get("mentions"),w.get("distinct_snippets"),w.get("described"),len(w.get("not_described") or []),
                   len(w.get("retried") or []),len(w.get("unknown_answer_ids") or []),len(w.get("excluded_pages") or []))
                  for cb,w in wins]
    if locs:
        L=[len(x) for x in locs]
        r["loc_n"]=len(locs); r["loc_median"]=st.median(L); r["loc_p90"]=sorted(L)[int(len(L)*0.9)]; r["loc_max"]=max(L)
        r["loc_chars"]=sum(L)
        r["loc_url_share"]=round(sum(1 for x in locs if URL.search(x))/len(locs),3)
        r["loc_url_chars"]=round(sum(len(m.group(0)) for x in locs for m in URL.finditer(x))/max(1,sum(L)),3)
        r["loc_rel"]=round(sum(1 for x in locs if REL.search(x))/len(locs),3)
        r["loc_this_passage"]=round(sum(1 for x in locs if x.lower().startswith(("this passage","this sentence","this line","this text")))/len(locs),3)
    dl=set(locs); r["loc_distinct"]=len(dl)
    if dl:
        r["locd_median"]=st.median([len(x) for x in dl])
        r["locd_url_share"]=round(sum(1 for x in dl if URL.search(x))/len(dl),3)
    # synthesis payload estimate: distinct (group, snippet), first location
    seen={}
    for cb,ch in chunks.items():
        for g in ch.get("fold",{}).get("groups",[]):
            for m in g.get("mentions",[]):
                k=(g.get("group_id"),m.get("snippet",""))
                if k not in seen: seen[k]=m.get("location","")
    r["entries"]=len(seen); r["snip_chars"]=sum(len(k[1]) for k in seen); r["entry_loc_chars"]=sum(len(v) for v in seen.values())
    r["req_rows"]=req_rows
    return r
if __name__=="__main__":
    import sys
    out={}
    for subj in ("alecmfg_com","steelcraft_com"):
        for fld in FIELDS:
            for run in (PREV[subj],NEW):
                x=rep(run,subj,fld)
                if x: out[(run,subj,fld)]=x
    for subj in ("alecmfg_com","steelcraft_com"):
        print("#"*80); print("SUBJECT",subj,"prev run",PREV[subj])
        for fld in FIELDS:
            o=out.get((PREV[subj],subj,fld)); n=out.get((NEW,subj,fld))
            print(f"\n== {fld} ==")
            if not n: print("  MISSING in new"); continue
            print("  chunk_strat", n["chunk_strat"])
            print("  text_tokens", n["text_tokens"], "excl", {k:v for k,v in (n["excl"] or {}).items() if k!="pages"} if n["excl"] else None)
            print("  chunks old", o["chunks"] if o else None); print("  chunks new", n["chunks"])
            print("  pv mention old/new", (o["pv_mention"] if o else None), n["pv_mention"], "| pv search", (o["pv_search"] if o else None), n["pv_search"])
            print("  time s old/new", (o["time"] if o else None), n["time"])
            print("  tokens old", o["tokens"] if o else None)
            print("  tokens new", n["tokens"])
            print("  requests old", (o["requests"], o["synthetic"]) if o else None, "new", n["requests"], "synthetic", n["synthetic"])
            print("  summary old", o["summary"] if o else None)
            print("  summary new", n["summary"])
            print("  groups old", (o["groups_n"],o["empty_groups"]) if o else None, "new", (n["groups_n"],n["empty_groups"]), "rows", n["rows_prov"])
            print("  windows new (chunk, sub, sent, hits, zero, mentions, distinct, described, notdesc, retried, unknown, excl):")
            for w in n["windows"]: print("    ",w)
            if o:
                print("  windows old:")
                for w in o["windows"]: print("    ",w)
            for k in ("loc_n","loc_distinct","loc_median","locd_median","loc_p90","loc_max","loc_url_share","locd_url_share","loc_url_chars","loc_rel","loc_this_passage","entries","snip_chars","entry_loc_chars"):
                print(f"   {k}: old={o.get(k) if o else None} new={n.get(k)}")
