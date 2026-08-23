import json, os, re, statistics as st
from collections import Counter, defaultdict
from bisect import bisect_right
ROOT="/Users/amaryadav/Documents/ASU_PhD/SUDOKN/data-ETL-pipeline"
NEW=f"{ROOT}/packages/logs/extraction_dumps/20260822T223715"; PREV=f"{ROOT}/packages/logs/extraction_dumps/20260822T195947"
TXT=f"{ROOT}/apps/data_etl_app/src/data_etl_app/knowledge/sample_scraped_texts"
SP="/private/tmp/claude-501/-Users-amaryadav-Documents-ASU-PhD-SUDOKN-data-ETL-pipeline/a21c5f1c-3dd6-45e1-80fd-e0ee1e3bd1ea/scratchpad"
FIELDS=["conformity_attestations","equipments","industries","material_caps","process_caps","products"]
SUBJ={"alecmfg_com":"alecmfg.com","steelcraft_com":"steelcraft.com"}
texts={s:open(f"{TXT}/{SUBJ[s]}.txt").read() for s in SUBJ}
def load(run,s,f):
    p=f"{run}/{s}__{f}__partial.json"; return json.load(open(p)) if os.path.exists(p) else None
URL_RE=re.compile(r"^[ \t]*https?://\S+[ \t]*$", re.M); SEP_RE=re.compile(r"^[ \t]*#{10,}[ \t]*$", re.M)
EXCL=re.compile(r"privacy|cookie|terms|legal|disclaimer|gdpr|imprint|impressum", re.I)
def path(u):
    w=u.split("://",1)[-1]; i=w.find("/"); return "" if i<0 else w[i:]
def word_re(form):
    l=r"(?<!\w)" if re.match(r"\w",form[0]) else ""; r=r"(?!\w)" if re.match(r"\w",form[-1]) else ""
    return re.compile(l+re.escape(form)+r, 0 if len(form)<=3 else re.I)
def scan_domain(wt, pre):
    out=list(wt)
    def blank(a,b):
        for i in range(a,b):
            if out[i]!="\n": out[i]=" "
    for m in URL_RE.finditer(wt): blank(m.start(),m.end())
    for m in SEP_RE.finditer(wt): blank(m.start(),m.end())
    urls=[(m.start(),m.end(),m.group().strip()) for m in URL_RE.finditer(wt)]
    if pre and EXCL.search(path(pre)):
        blank(0, urls[0][0] if urls else len(wt))
    for i,(a,b,u) in enumerate(urls):
        if EXCL.search(path(u)): blank(a, urls[i+1][0] if i+1<len(urls) else len(wt))
    return "".join(out)

print("=== STEELCRAFT exact-text verification (local file == run text for the covered range) ===")
s="steelcraft_com"; text=texts[s]
tot=bad_span=bad_snip=bad_snipspan=0
missed_total=0; missed_by_field=Counter(); missed_examples=[]; occ_total=0
forms_not_sent_by_window=Counter()
for f in FIELDS:
    d=load(NEW,s,f)
    for cb,ch in d["chunks"].items():
        wins={w["window"]:w["sub_bounds"] for w in ch["fold"]["windows"]}
        # per-window: forms with mentions in that window; chunk-wide: all forms of groups
        chunk_forms=set()
        for g in ch["fold"]["groups"]:
            chunk_forms|=set(g["forms"])
        win_forms=defaultdict(set); win_cov=defaultdict(set)
        for g in ch["fold"]["groups"]:
            for m in g["mentions"]:
                tot+=1
                ws,we=map(int,wins[m["window"]].split(":")); a,b=m["span"]
                if text[ws+a:ws+b]!=m["form"]: bad_span+=1
                if m["snippet"] not in text[ws:we]: bad_snip+=1
                win_forms[m["window"]].add(m["form"]); win_forms[m["window"]].add(m.get("sent_form") or m["form"])
                win_cov[m["window"]].update(range(a,b))
        # loss channel: chunk forms occurring in a window but not covered there
        for wi,sb in wins.items():
            ws,we=map(int,sb.split(":")); wt=text[ws:we]
            before=[m for m in URL_RE.finditer(text[:ws])]; pre=before[-1].group().strip() if before else None
            dom=scan_domain(wt,pre)
            for form in chunk_forms:
                for mm in word_re(form).finditer(dom):
                    occ_total+=1
                    if not (set(range(mm.start(),mm.end())) & win_cov[wi]):
                        missed_total+=1; missed_by_field[f]+=1
                        forms_not_sent_by_window[(f,form)]+=1
                        if len(missed_examples)<10: missed_examples.append((f,sb,form,dom[max(0,mm.start()-50):mm.end()+40].replace("\n","⏎")))
print(f"mentions {tot}: span text != form: {bad_span}; snippet not in window: {bad_snip}")
print(f"LOSS CHANNEL (form has mentions somewhere in the chunk, occurs in window W's scan domain, but no mention covers it in W): {missed_total} of {occ_total} chunk-form occurrences ({missed_total/occ_total:.1%})")
print("  by field:", dict(missed_by_field))
print("  top (field, form):", forms_not_sent_by_window.most_common(12))
for e in missed_examples: print("   ",e)

print("\n=== ALECMFG approximate (local text drifts 44 chars): snippet containment in the local text ===")
s="alecmfg_com"; text=texts[s]; tot=miss=0; ex=[]
for f in FIELDS:
    d=load(NEW,s,f)
    for cb,ch in d["chunks"].items():
        for g in ch["fold"]["groups"]:
            for m in g["mentions"]:
                tot+=1
                if m["snippet"] not in text:
                    miss+=1
                    if len(ex)<5: ex.append(m["snippet"][:100])
print(f"alecmfg mentions {tot}; snippet not found anywhere in local text: {miss}; examples: {ex}")

print("\n=== LOCATION COMPOSITION (distinct (subject,field,chunk,window,mention_id)) ===")
items=[]
for s in SUBJ:
    for f in FIELDS:
        d=load(NEW,s,f)
        for cb,ch in d["chunks"].items():
            wins={w["window"]:w["sub_bounds"] for w in ch["fold"]["windows"]}
            seen=set()
            for g in ch["fold"]["groups"]:
                for m in g["mentions"]:
                    k=(m["window"],m["mention_id"])
                    if k in seen: continue
                    seen.add(k); items.append((s,f,cb,wins[m["window"]],m))
L=[m["location"] for *_,m in items if m["location_source"]=="llm"]
total_chars=sum(len(x) for x in L)
url_chars=sum(len(u) for x in L for u in re.findall(r"\(?https?://\S+\)?", x))
prefix_chars=sum(len(mm.group()) for x in L for mm in [re.match(r"^(This|The) (passage|phrase|line|sentence|text|word|heading|item|entry|mention|statement|title|bullet)( (is|appears|is found|is located|occurs|is part of|is listed|is present|sits))?( (as|in|on|at|under|within))?\s*", x)] if mm)
print(f"LLM locations {len(L)}: total chars {total_chars:,}; URL chars {url_chars:,} ({url_chars/total_chars:.0%}); 'This passage appears as' prefix chars {prefix_chars:,} ({prefix_chars/total_chars:.0%})")
print(f"snippet chars (same items) {sum(len(m['snippet']) for *_,m in items):,}  → location/snippet char ratio {total_chars/sum(len(m['snippet']) for *_,m in items):.2f}")
rel=re.compile(r"previous mention|preceding mention|the same (testimonial|quotation|quote|list|section|page|paragraph|bullet|table|post|article|case study|heading)|another (item|entry|bullet|line)|as (the|in) (previous|above)|mentioned above|described above|above mention|next mention|following mention|also (in|on|from) the same|continu(es|ing|ation of) (the|from)|immediately (follows|after|before|preceding)|second sentence of the same|first sentence of the same|the earlier mention|like the previous", re.I)
relhits=[x for x in L if rel.search(x)]
print(f"locations with RELATIVE references to other mentions in the request: {len(relhits)} ({len(relhits)/len(L):.1%})")
for x in relhits[:10]: print("   -", x[:230])
hedge=re.compile(r"before any URL|before the first URL|no URL|beginning of the (scraped )?text|start of the (scraped )?text|initial (section|part)|top of the (scraped )?text|very beginning|not (possible|clear) to (determine|tell)|cannot be determined|unclear (which|what) page", re.I)
hh=[(s,f,sb,m) for s,f,cb,sb,m in items if m["location_source"]=="llm" and hedge.search(m["location"])]
print(f"hedged / 'before any URL' locations: {len(hh)}; by window: {Counter((s,sb) for s,f,sb,m in hh).most_common(8)}")
# items whose first occurrence precedes the first URL line in its window (head-of-window, page inherited by code)
head_items=0; head_hedged=0; head_named_page=0; head_wrong=[]
for s,f,cb,sb,m in items:
    ws,we=map(int,sb.split(":")); wt=texts[s][ws:we]
    first=URL_RE.search(wt); cut=first.start() if first else len(wt)
    if m["span"][0] < cut:   # approx for alecmfg
        head_items+=1
        if m["location_source"]!="llm": continue
        if hedge.search(m["location"]): head_hedged+=1
        elif "http" in m["location"] or re.search(r"page", m["location"], re.I): head_named_page+=1
        if len(head_wrong)<6 and "http" in m["location"] and not hedge.search(m["location"]): head_wrong.append((s,f,sb,m["page"],m["location"][:200]))
print(f"items whose occurrence sits in the window HEAD (before the window's first URL line; page known only to code): {head_items}; hedged {head_hedged}; named a page anyway {head_named_page}")
for w in head_wrong: print("   head item naming a page (code page vs location):", w)

print("\n=== SYNTHESIS INPUT VOLUME (entries = distinct snippets per group, first-occurrence location) ===")
ent=0; schars=0; lchars=0; per_group=[]
for s in SUBJ:
    for f in FIELDS:
        d=load(NEW,s,f)
        for cb,ch in d["chunks"].items():
            for g in ch["fold"]["groups"]:
                seen=set(); n=0
                for m in g["mentions"]:
                    if m["snippet"] in seen: continue
                    seen.add(m["snippet"]); n+=1; schars+=len(m["snippet"]); lchars+=len(m["location"])
                if n: per_group.append(n); ent+=n
print(f"non-empty groups {len(per_group)}; entries {ent:,}; entries/group median {st.median(per_group)}, p90 {sorted(per_group)[int(.9*len(per_group))]}, max {max(per_group)}")
print(f"entry snippet chars {schars:,} + location chars {lchars:,} = {schars+lchars:,} chars (~{(schars+lchars)//4:,} tokens) of synthesis entry payload (6 fields x 2 subjects)")
# prev run equivalents (prev fold: mentions had location + snippet; entries were per mention)
pent=0; pschars=0; plchars=0
for s in SUBJ:
    for f in FIELDS:
        p=load(PREV,s,f)
        for cb,ch in p["chunks"].items():
            for g in ch["fold"]["groups"]:
                seen=set()
                for m in g["mentions"]:
                    if m["snippet"] in seen: continue
                    seen.add(m["snippet"]); pent+=1; pschars+=len(m["snippet"]); plchars+=len(m.get("location") or "")
print(f"PREV run (distinct snippets per group): entries {pent:,}; snippet chars {pschars:,} + location chars {plchars:,} = {pschars+plchars:,}")
# which groups will be biggest synthesis records
big=[]
for s in SUBJ:
    for f in FIELDS:
        d=load(NEW,s,f)
        for cb,ch in d["chunks"].items():
            for g in ch["fold"]["groups"]:
                seen=set(); c=0
                for m in g["mentions"]:
                    if m["snippet"] in seen: continue
                    seen.add(m["snippet"]); c+=len(m["snippet"])+len(m["location"])
                big.append((c,s,f,g["key"],g["distinct_snippets"]))
print("largest synthesis records (chars, subject, field, key, distinct snippets):")
for b in sorted(big,reverse=True)[:8]: print("   ",b)
