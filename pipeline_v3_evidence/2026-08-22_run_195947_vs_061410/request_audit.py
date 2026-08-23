"""Request-level audit of the mention-collection stage.  usage: request_audit.py <label> <requests.json> <run_id>"""
import json, re, sys, collections, statistics, glob, bisect, difflib, itertools
sys.path.insert(0, "/Users/amaryadav/Documents/ASU_PhD/SUDOKN/data-ETL-pipeline/packages/core/src")
from core.utils.floor_scan import floor_scan, find_form_occurrences, mask_page_headers
from core.utils.form_normalizer import normalize
label, path, run = sys.argv[1:4]
ROOT="/Users/amaryadav/Documents/ASU_PhD/SUDOKN/data-ETL-pipeline"
reqs=[r for r in json.load(open(path)) if r.get("custom_id") and ">llm_phrase_mention_collection>" in r["custom_id"]]
full={s: open(f"{ROOT}/apps/data_etl_app/src/data_etl_app/knowledge/sample_scraped_texts/{s}.txt",encoding="utf-8").read() for s in ["alecmfg.com","steelcraft.com"]}
prov={}
for p in glob.glob(f"{ROOT}/packages/logs/extraction_dumps/{run}/*__partial.json"):
    d=json.load(open(p))
    for cb,ch in d["chunks"].items():
        for row in ch.get("rows",[]): prov[(d["subject_unique_id"],d["field_type"],cb,row.get("phrase"))]=row.get("provenance")
MARK="text scraped from a manufacturer's website:\n"
def parse(um):
    a=um.index("<<<PHRASES")+len("<<<PHRASES"); b=um.index("PHRASES>>>")
    sent=json.loads(um[a:b].strip())
    text=um.split(MARK,1)[1].rsplit("<<<PHRASES",1)[0] if MARK in um else None
    return sent,text
C=collections.Counter
n_real=n_dummy=0; in_tok=out_tok=0; no_marker=0; unparse=0
forms_sent=0; zero_hit=0; zero_hit_given=0; loose=0; halluc=0; cls=C(); cls_ex=collections.defaultdict(list); zero_by_field=C(); sent_by_field=C()
snip_len=[]; loc_len=[]; multiline=0; lines_per=C(); longest=("",0,"",""); bare=0; bare_in_sentence=0; multi_sent=0; over_line=0; located=0
true_dup=0; legit=0; nonmono=0; echo_mis=0; unknown_loc=0; snip_chars=loc_chars=form_chars=0
hv=collections.defaultdict(lambda:[0,0,0]); rows=[]; fin=C(); gs=C()
cov=collections.defaultdict(lambda:[0,0,0,0,0]); _SENT=re.compile(r"(?<=[.!?])\s+")
def ctx(text,off,lines,starts):
    li=bisect.bisect_right(starts,off)-1; ls=starts[li]; line=lines[li]
    cuts=[0]+[m.end() for m in _SENT.finditer(line)]+[len(line)]; rel=off-ls
    sb=max(c for c in cuts if c<=rel); se=min([c for c in cuts if c>rel] or [len(line)]); return line[sb:se].strip().casefold()
wg=C(); win_tok={}
for r in reqs:
    um=r["user_message"] or ""
    if "<<<PHRASES" not in um: n_dummy+=1; continue
    n_real+=1; fin[str(r.get("finish_reason"))]+=1
    m=re.search(r"^([^>]+)>([^>]+)>llm_phrase_mention_collection>chunk>([^>]+)>sub>([^>]+)>group>(\d+)>",r["custom_id"]); subj,field,chunk,sub,gi=m.groups()
    sent,text=parse(um)
    if text is None: no_marker+=1; continue
    u=r["usage"] or {}; in_tok+=u.get("prompt_tokens",0); out_tok+=u.get("completion_tokens",0)
    wg[(subj,field,sub)]+=1; win_tok[(subj,field,sub)]=u.get("prompt_tokens",0); gs[len(sent)]+=1
    scan=floor_scan(text,sent)
    try: forms=json.loads(r["content"])["forms"]
    except Exception: unparse+=1; continue
    returned=[f["form"] for f in forms]; echo_mis+=sum(1 for f in returned if f not in sent)
    by={f["form"]:(f.get("mentions") or []) for f in forms}
    with_hits=[f for f in sent if scan.tier1[f]]; answered={f for f in sent if by.get(f)}
    rows.append(dict(subj=subj,field=field,sub=sub,gi=int(gi),n_sent=len(sent),n_hits=len(with_hits),n_ans_hits=len(answered&set(with_hits)),n_returned=len(returned),
        n_m=sum(len(v) for v in by.values()),out_tok=u.get("completion_tokens"),skipped=[f for f in with_hits if f not in answered]))
    cs,ce=map(int,chunk.split(":")); chunk_text=full[subj][cs:ce]
    lines=text.split("\n"); starts=[0]
    for ln in lines: starts.append(starts[-1]+len(ln)+1)
    for form in sent:
        forms_sent+=1; sent_by_field[field]+=1
        hits=scan.tier1[form]; ms=by.get(form,[]); nh=len(hits); nm=len(ms)
        b="0" if nh==0 else "1" if nh==1 else "2-4" if nh<=4 else "5-9" if nh<=9 else "10-24" if nh<=24 else "25+"
        x=hv[b]; x[0]+=1; x[1]+=nh; x[2]+=min(nm,nh) if nh else 0
        if nh==0:
            zero_hit+=1; zero_by_field[field]+=1
            if scan.tier2[form]: c="A casing/punct variant occurs in window"
            elif find_form_occurrences(chunk_text,form,case_sensitive=False): c="B occurs elsewhere in the CHUNK"
            elif find_form_occurrences(full[subj],form,case_sensitive=False): c="C occurs elsewhere in the SUBJECT"
            else: c="D occurs NOWHERE in the subject text"
            cls[c]+=1
            if len(cls_ex[c])<3: cls_ex[c].append((field,form[:50],prov.get((subj,field,chunk,form))))
            if nm:
                zero_hit_given+=1
                for mm in ms:
                    if form.lower() in mm.get("snippet","").lower(): loose+=1
                    else: halluc+=1
        if nh>=3:
            ctxs={ctx(text,h.start,lines,starts) for h in hits}; covered=set(); red=0; rep=0
            for mm in ms:
                sn=mm.get("snippet",""); pos=text.find(sn); fi=sn.find(form) if pos>=0 else -1
                if fi<0: continue
                rep+=1; cc=ctx(text,pos+fi,lines,starts); red+= cc in covered; covered.add(cc)
            bk="3-4" if nh<=4 else "5-9" if nh<=9 else "10+"; v=cov[bk]; v[0]+=1; v[1]+=len(ctxs); v[2]+=len(covered&ctxs); v[3]+=rep; v[4]+=red
        form_chars+=len(form); last=-1
        cnt=C(mm.get("snippet","") for mm in ms)
        for sn,c in cnt.items():
            if c>1:
                occ=text.count(sn) if sn else 0
                if c>occ: true_dup+=c-occ
                else: legit+=c
        for mm in ms:
            sn=mm.get("snippet",""); lc=mm.get("location","")
            snip_len.append(len(sn)); loc_len.append(len(lc)); snip_chars+=len(sn); loc_chars+=len(lc)
            nl=sn.count("\n")+1 if sn else 0; lines_per[min(nl,5)]+=1
            if "\n" in sn: multiline+=1
            if len(sn)>longest[1]: longest=(field,len(sn),form,sn[:120].replace("\n","⏎"))
            if len(re.findall(r"[.!?]\s",sn))>=3: multi_sent+=1
            if re.search(r"\bunknown\b|not shown|mid-page|before any URL|cannot (be )?determine",lc,re.I): unknown_loc+=1
            pos=text.find(sn)
            if pos>=0:
                located+=1
                if pos<last: nonmono+=1
                last=pos
                fi=sn.find(form)
                if fi>=0:
                    ls_=text.rfind("\n",0,pos+fi)+1; le_=text.find("\n",pos+fi); le_=len(text) if le_<0 else le_
                    if pos<ls_ or pos+len(sn)>le_: over_line+=1
                    if sn.strip()==form:
                        bare+=1; line=text[ls_:le_]
                        if len(line.strip())>len(form)+15 and re.search(r"[.!?]",line): bare_in_sentence+=1
print(f"##### {label} (run {run}) #####")
print(f"requests real {n_real}, dummy {n_dummy}, unparsable {unparse}, no-marker {no_marker}; prompt tokens {in_tok:,}, completion {out_tok:,}; finish_reason {dict(fin)}; group sizes {sorted(gs.items())}")
print(f"forms sent {forms_sent}; ZERO exact hits in window {zero_hit} ({zero_hit/forms_sent:.0%}); given mentions anyway {zero_hit_given} (loose-casing {loose}, not holding form {halluc})")
for c,n in sorted(cls.items()): print(f"    {n:4d} {c}  e.g. {cls_ex[c]}")
print("    zero-hit by field:", {f: f"{zero_by_field[f]}/{sent_by_field[f]}" for f in sorted(sent_by_field)})
th=sum(r["n_hits"] for r in rows); ta=sum(r["n_ans_hits"] for r in rows)
print(f"\nUNDER-ANSWER: with-hit forms {th}, answered {ta} ({ta/th:.0%}); returned-all-forms {sum(r['n_returned']==r['n_sent'] for r in rows)}/{len(rows)}; fewer-returned: {[(r['subj'][:5],r['field'][:10],r['sub'],r['gi'],f'{r['n_returned']}/{r['n_sent']}') for r in rows if r['n_returned']<r['n_sent']]}")
for r in rows: r["skip"]=(1-r["n_ans_hits"]/r["n_hits"]) if r["n_hits"] else None
valid=[r for r in rows if r["skip"] is not None and r["n_hits"]>=3]
print("  skip-ratio dist (groups with >=3 with-hit forms):", dict(C(("0%" if r["skip"]==0 else "<20%" if r["skip"]<.2 else "<50%" if r["skip"]<.5 else "<80%" if r["skip"]<.8 else ">=80%") for r in valid)))
print("  WORST (skip>=40%):", [(r['subj'][:5],r['field'][:12],r['sub'],r['gi'],f"sent {r['n_sent']} hits {r['n_hits']} ans {r['n_ans_hits']} m {r['n_m']} tok {r['out_tok']}") for r in sorted(valid,key=lambda r:-r['skip']) if r['skip']>=.4])
print("\nSATISFICING by frequency (hits bucket: forms, hits, reported<=hits, recall):")
for k in ["1","2-4","5-9","10-24","25+"]:
    f_,h_,rp=hv[k]; print(f"   {k:6s} forms {f_:4d} hits {h_:5d} reported {rp:5d} recall {rp/h_ if h_ else 0:.0%}")
print("CONTEXT COVERAGE (forms with >=3 exact hits): forms | distinct contexts, covered | reported, redundant")
for bk in ["3-4","5-9","10+"]:
    f_,dc,cc,rep,red=cov[bk]; print(f"   {bk:4s} forms {f_:4d} | ctx {dc:5d} covered {cc:5d} ({cc/max(dc,1):.0%}) | reported {rep:5d} redundant {red:4d} ({red/max(rep,1):.0%})")
n=len(snip_len)
print(f"\nSNIPPETS: mentions {n}; chars median {statistics.median(snip_len):.0f} p90 {sorted(snip_len)[int(.9*n)]} max {max(snip_len)}; multi-line {multiline} ({multiline/n:.1%}); lines/snippet {dict(sorted(lines_per.items()))}; >=3 sentences {multi_sent}; located {located}; beyond occurrence's line {over_line} ({over_line/max(located,1):.0%}); bare-form snippets {bare} (inside a sentence {bare_in_sentence})")
print(f"   longest: {longest}")
print(f"   true duplicate snippets {true_dup}, legit repeats {legit}; non-monotonic {nonmono}; echo mismatches {echo_mis}")
tot=snip_chars+loc_chars+form_chars
print(f"LOCATIONS: chars median {statistics.median(loc_len):.0f} p90 {sorted(loc_len)[int(.9*len(loc_len))]}; unknown/mid-page {unknown_loc}; output chars: snippets {snip_chars/tot:.0%} locations {loc_chars/tot:.0%} echoes {form_chars/tot:.0%}")
multi=[k for k,v in wg.items() if v>1]; dup_tok=sum(win_tok[k]*(wg[k]-1) for k in multi)
print(f"RE-SEND: windows {len(wg)}; >1 group {len(multi)} (max {max(wg.values())}); groups/window dist {sorted(C(wg.values()).items())}; re-sent window text ≈ {dup_tok:,} prompt tokens ({dup_tok/in_tok:.0%})")
import os; json.dump(rows, open(os.path.join(os.path.dirname(os.path.abspath(__file__)), f"group_rows_{label}.json"),"w"))
