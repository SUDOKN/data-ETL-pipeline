"""Broad audit of the mention-collection stage, run 20260822T061410 (raw answers + fold dumps)."""
import json, re, sys, collections, statistics, glob, bisect
sys.path.insert(0, "/Users/amaryadav/Documents/ASU_PhD/SUDOKN/data-ETL-pipeline/packages/core/src")
from core.utils.floor_scan import floor_scan, find_form_occurrences, mask_page_headers
S="/private/tmp/claude-501/-Users-amaryadav-Documents-ASU-PhD-SUDOKN-data-ETL-pipeline/b0b8a829-bab9-4333-b43c-60e0b05cf84b/scratchpad"
reqs=json.load(open(f"{S}/mention_requests.json"))
C=collections.Counter; tot=C()
snip_len=[]; loc_len=[]; out_tok=0; in_tok=0; n_real=0; n_dummy=0
zero_hit_forms=0; zero_hit_with_mentions=0; zero_hit_examples=[]
loose_case=0; halluc=0; halluc_ex=[]
multiline=0; multiline_ex=[]; header_snips=0
dup_same_snippet=0; dup_ex=[]
nonmono=0
echo_mismatch=0; echo_ex=[]
unknown_loc=0; unknown_loc_known_page=0; short_loc=0
hits_vs_reported=collections.defaultdict(lambda:[0,0,0])  # bucket -> [forms, hits, reported]
mention_len_by_kind=collections.defaultdict(list)
windows=collections.defaultdict(set); window_groups=collections.defaultdict(int); window_in_tok={}
sent_only_form=0; sentence_multi=0; whole_para=0
per_form_records=[]
for r in reqs:
    um=r["user_message"] or ""
    if "<<<PHRASES" not in um:
        n_dummy+=1; continue
    n_real+=1
    cid=r["custom_id"]; m=re.search(r"^([^>]+)>([^>]+)>llm_phrase_mention_collection>chunk>([^>]+)>sub>([^>]+)>group>(\d+)>",cid)
    subj,field,chunk,sub,gi=m.groups()
    a=um.index("<<<PHRASES")+len("<<<PHRASES"); b=um.index("PHRASES>>>")
    sent=json.loads(um[a:b].strip()); text=um.split("text scraped from a manufacturer's website:\n",1)[1].rsplit("<<<PHRASES",1)[0]
    u=r["usage"] or {}; out_tok+=u.get("completion_tokens",0); in_tok+=u.get("prompt_tokens",0)
    window_groups[(subj,field,sub)]+=1; window_in_tok[(subj,field,sub)]=u.get("prompt_tokens",0)
    scan=floor_scan(text, sent); domain=mask_page_headers(text)
    try: forms=json.loads(r["content"])["forms"]
    except Exception: tot["unparseable"]+=1; continue
    returned=[f["form"] for f in forms]
    for f in returned:
        if f not in sent: echo_mismatch+=1; echo_ex.append((f, [s_ for s_ in sent if s_.lower().strip()==f.lower().strip()][:1]))
    by_form={f["form"]: f.get("mentions") or [] for f in forms}
    for form in sent:
        hits=scan.tier1.get(form,[]); ms=by_form.get(form,[])
        nh=len(hits); nm=len(ms)
        bucket = "0" if nh==0 else "1" if nh==1 else "2-4" if nh<=4 else "5-9" if nh<=9 else "10-24" if nh<=24 else "25+"
        hv=hits_vs_reported[bucket]; hv[0]+=1; hv[1]+=nh; hv[2]+=min(nm,nh) if nh else 0
        if nh==0:
            zero_hit_forms+=1
            if nm:
                zero_hit_with_mentions+=1
                for mm in ms:
                    sn=mm.get("snippet","")
                    if form.lower() in sn.lower(): loose_case+=1
                    else: halluc+=1; 
                    if len(zero_hit_examples)<6: zero_hit_examples.append((field, form, sn[:100]))
        seen_snips=set(); last_pos=-1
        for mm in ms:
            sn=mm.get("snippet",""); lc=mm.get("location","")
            snip_len.append(len(sn)); loc_len.append(len(lc))
            if "\n" in sn:
                multiline+=1
                if len(multiline_ex)<4: multiline_ex.append((field, form, sn[:160].replace("\n","⏎")))
            if re.search(r"^#{10,}|https?://\S+$", sn.strip(), re.M): header_snips+=1
            if sn in seen_snips:
                dup_same_snippet+=1
                if len(dup_ex)<3: dup_ex.append((field, form, sn[:80]))
            seen_snips.add(sn)
            pos=text.find(sn)
            if pos>=0:
                if pos<last_pos: nonmono+=1
                last_pos=pos
                if sn.strip()==form: sent_only_form+=1
                # whole-paragraph: snippet equals a whole line and line has >=3 sentences
                if len(re.findall(r"[.!?]\s", sn))>=3: sentence_multi+=1
            if re.search(r"\bunknown\b|not shown|mid-page|before any URL|cannot (be )?determine", lc, re.I):
                unknown_loc+=1
                if pos>=0 and scan.page_of(type("O",(),{"start":pos})()) : unknown_loc_known_page+=1
            if len(lc)<=12: short_loc+=1
        per_form_records.append((subj,field,sub,form,nh,nm))
print(f"real requests {n_real}, dummy {n_dummy}; prompt tokens {in_tok:,}, completion tokens {out_tok:,}")
print(f"forms sent {len(per_form_records)}; with 0 exact hits in their window: {zero_hit_forms} ({zero_hit_forms/len(per_form_records):.0%}); of those given mentions anyway: {zero_hit_with_mentions} -> snippets holding the form in another casing: {loose_case}, not holding it at all: {halluc}")
for e in zero_hit_examples: print("    zero-hit example:", e)
print(f"\nmentions total {len(snip_len)}; snippet chars median {statistics.median(snip_len):.0f} p90 {sorted(snip_len)[int(.9*len(snip_len))]} max {max(snip_len)}; multi-line snippets {multiline} ({multiline/len(snip_len):.1%}); header-line snippets {header_snips}; snippet == bare form {sent_only_form}; snippets with >=3 sentences {sentence_multi}")
for e in multiline_ex: print("    multiline:", e)
print(f"duplicate identical snippets under one form {dup_same_snippet}; non-monotonic order events {nonmono}; echoed form not in sent list {echo_mismatch} {echo_ex[:3]}")
print(f"location chars median {statistics.median(loc_len):.0f} p90 {sorted(loc_len)[int(.9*len(loc_len))]}; 'unknown/mid-page' locations {unknown_loc} (page actually known by code: {unknown_loc_known_page}); very short locations {short_loc}")
print("\nsatisficing by frequency (hits bucket: forms, hits, reported≤hits, recall):")
for k in ["1","2-4","5-9","10-24","25+"]:
    f_,h_,rp=hits_vs_reported[k]; print(f"   {k:6s} forms {f_:4d} hits {h_:5d} reported {rp:5d} recall {rp/h_ if h_ else 0:.0%}")
# window duplication cost
multi=[k for k,v in window_groups.items() if v>1]
dup_tok=sum(window_in_tok[k]*(window_groups[k]-1) for k in multi)
print(f"\nwindows {len(window_groups)}; windows needing >1 group {len(multi)} (max groups {max(window_groups.values())}); window text re-sent for extra groups ≈ {dup_tok:,} prompt tokens ({dup_tok/in_tok:.0%} of the stage's prompt tokens)")
# zero-hit forms: where do they come from? forms sent to a window but occurring nowhere in it
