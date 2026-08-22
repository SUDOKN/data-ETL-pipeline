import json, re, sys, collections, statistics
sys.path.insert(0, "/Users/amaryadav/Documents/ASU_PhD/SUDOKN/data-ETL-pipeline/packages/core/src")
from core.utils.floor_scan import floor_scan
S="/private/tmp/claude-501/-Users-amaryadav-Documents-ASU-PhD-SUDOKN-data-ETL-pipeline/b0b8a829-bab9-4333-b43c-60e0b05cf84b/scratchpad"
reqs=[r for r in json.load(open(f"{S}/mention_requests.json")) if r["user_message"] and "<<<PHRASES" in r["user_message"]]
wg=collections.Counter(); wsent=collections.Counter(); wfield={}
true_dup=0; legit_repeat=0; dup_ex=[]
loc_chars=0; snip_chars=0; form_chars=0
lines_per=collections.Counter(); longest=("",0,"",""); bare_in_sentence=0; bare_total=0
over_line=0; located=0
for r in reqs:
    m=re.search(r"^([^>]+)>([^>]+)>llm_phrase_mention_collection>chunk>([^>]+)>sub>([^>]+)>group>(\d+)>",r["custom_id"]); subj,field,chunk,sub,gi=m.groups()
    um=r["user_message"]; a=um.index("<<<PHRASES")+len("<<<PHRASES"); b=um.index("PHRASES>>>")
    sent=json.loads(um[a:b].strip()); text=um.split("text scraped from a manufacturer's website:\n",1)[1].rsplit("<<<PHRASES",1)[0]
    k=(subj,field,sub); wg[k]+=1; wsent[k]+=len(sent); wfield[k]=field
    try: forms=json.loads(r["content"])["forms"]
    except Exception: continue
    for f in forms:
        form=f["form"]; ms=f.get("mentions") or []
        form_chars+=len(form)
        cnt=collections.Counter(mm.get("snippet","") for mm in ms)
        for sn,c in cnt.items():
            if c>1:
                occ=text.count(sn) if sn else 0
                if c>occ: true_dup+=c-occ; dup_ex.append((field,form,c,occ,sn[:70])) if len(dup_ex)<4 else None
                else: legit_repeat+=c
        for mm in ms:
            sn=mm.get("snippet",""); lc=mm.get("location","")
            loc_chars+=len(lc); snip_chars+=len(sn)
            nl=sn.count("\n")+1 if sn else 0; lines_per[min(nl,5)]+=1
            if len(sn)>longest[1]: longest=(field,len(sn),form,sn[:200].replace("\n","⏎"))
            pos=text.find(sn)
            if pos>=0:
                located+=1
                # did the snippet run past the line of the occurrence? find the form inside the snippet
                fi=sn.find(form)
                if fi>=0:
                    line_start=text.rfind("\n",0,pos+fi)+1; line_end=text.find("\n",pos+fi); line_end=len(text) if line_end<0 else line_end
                    if pos<line_start or pos+len(sn)>line_end: over_line+=1
                    if sn.strip()==form:
                        bare_total+=1
                        line=text[line_start:line_end]
                        if len(line.strip())>len(form)+15 and re.search(r"[.!?]", line): bare_in_sentence+=1
print("windows:", len(wg), "| groups per window distribution:", sorted(collections.Counter(wg.values()).items()))
print("top windows by groups:", [(wfield[k], k[0][:9], k[2], wg[k], wsent[k]) for k in sorted(wg, key=lambda k:-wg[k])[:6]])
print(f"\nidentical snippets under one form: true duplicates (more copies than occurrences) {true_dup}; legitimate repeats (identical line at distinct positions) {legit_repeat}")
for e in dup_ex: print("   dup:", e)
tot=loc_chars+snip_chars+form_chars
print(f"\noutput chars: snippets {snip_chars:,} ({snip_chars/tot:.0%}), locations {loc_chars:,} ({loc_chars/tot:.0%}), form echoes {form_chars:,} ({form_chars/tot:.0%})")
print("lines per snippet:", dict(sorted(lines_per.items())), "| longest:", longest)
print(f"located snippets {located}; snippet runs beyond the occurrence's line {over_line} ({over_line/located:.0%}); bare-form snippets {bare_total} of which the form sat inside a sentence/longer line {bare_in_sentence}")
