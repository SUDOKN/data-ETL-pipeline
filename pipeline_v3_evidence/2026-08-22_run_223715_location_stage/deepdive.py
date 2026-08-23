import json, os, re, random, statistics as st
from collections import Counter, defaultdict
ROOT="/Users/amaryadav/Documents/ASU_PhD/SUDOKN/data-ETL-pipeline"
NEW=f"{ROOT}/packages/logs/extraction_dumps/20260822T223715"; PREV=f"{ROOT}/packages/logs/extraction_dumps/20260822T195947"
TXT=f"{ROOT}/apps/data_etl_app/src/data_etl_app/knowledge/sample_scraped_texts"
FIELDS=["conformity_attestations","equipments","industries","material_caps","process_caps","products"]
SUBJ={"alecmfg_com":"alecmfg.com","steelcraft_com":"steelcraft.com"}
texts={s:open(f"{TXT}/{SUBJ[s]}.txt").read() for s in SUBJ}
def load(run,s,f):
    p=f"{run}/{s}__{f}__partial.json"; return json.load(open(p)) if os.path.exists(p) else None
random.seed(7)

print("=== A. not_described windows ===")
for s in SUBJ:
    for f in FIELDS:
        d=load(NEW,s,f)
        for cb,ch in d["chunks"].items():
            for w in ch["fold"]["windows"]:
                if w["not_described"]:
                    reqs=ch["requests"]["llm_phrase_mention_collection"].get(w["sub_bounds"],[])
                    print(f"{s} {f} chunk {cb} sub {w['sub_bounds']}: items {w['distinct_snippets']} described {w['described']} not_described {len(w['not_described'])}; requests:")
                    for r in reqs: print("    ", r["custom_id"].split(">")[-1][:60], "in",r["input_tokens"],"out",r["output_tokens"])
                    # which snippets are not described
                    nd=set(w["not_described"]); shown=0
                    for g in ch["fold"]["groups"]:
                        for m in g["mentions"]:
                            if m["window"]==w["window"] and m["mention_id"] in nd and shown<8:
                                print("     ND:", m["mention_id"], repr(m["snippet"][:110])); shown+=1; nd.discard(m["mention_id"])

print("\n=== B. search replay vs re-run (identical custom ids: compare tokens) ===")
same_tok=diff_tok=0; ex=[]
for s in SUBJ:
    for f in FIELDS:
        d=load(NEW,s,f); p=load(PREV,s,f)
        pm={r["custom_id"]:r for cb,ch in p["chunks"].items() for r in ch["requests"]["llm_phrase_search"]}
        for cb,ch in d["chunks"].items():
            for r in ch["requests"]["llm_phrase_search"]:
                if r["custom_id"] in pm:
                    o=pm[r["custom_id"]]
                    if (o["input_tokens"],o["output_tokens"])==(r["input_tokens"],r["output_tokens"]): same_tok+=1
                    else: diff_tok+=1; ex.append((s,f,r["custom_id"].split(">")[5],o["input_tokens"],r["input_tokens"],o["output_tokens"],r["output_tokens"]))
print(f"identical ids with identical token counts: {same_tok}; with different counts: {diff_tok}")
for e in ex[:6]: print("  ",e)
# search time spans per subject
for s in SUBJ:
    d=load(NEW,s,"material_caps"); print(s, "search time_span", d["run"]["time_span"]["by_stage"].get("llm_phrase_search"))

print("\n=== D. span verification against subject text ===")
bad_span=0; bad_snip=0; total=0; in_steelcraft=0; in_excluded=0; bad_page=0
from bisect import bisect_right
URL_RE=re.compile(r"^[ \t]*https?://\S+[ \t]*$", re.M)
EXCL=re.compile(r"privacy|cookie|terms|legal|disclaimer|gdpr|imprint|impressum", re.I)
def path(u):
    w=u.split("://",1)[-1]; i=w.find("/"); return "" if i<0 else w[i:]
for s in SUBJ:
    text=texts[s]
    urls=[(m.start(), m.group().strip()) for m in URL_RE.finditer(text)]
    starts=[u[0] for u in urls]
    def page_at(off):
        i=bisect_right(starts,off)-1; return urls[i][1] if i>=0 else None
    for f in FIELDS:
        d=load(NEW,s,f)
        for cb,ch in d["chunks"].items():
            wins={w["window"]:w["sub_bounds"] for w in ch["fold"]["windows"]}
            for g in ch["groups"] if "groups" in ch else ch["fold"]["groups"]:
                for m in g["mentions"]:
                    total+=1
                    ws=int(wins[m["window"]].split(":")[0]); a,b=m["span"]
                    occ=text[ws+a:ws+b]
                    if occ!=m["form"]: bad_span+=1
                    if m["snippet"] not in text[ws:ws+int(wins[m["window"]].split(":")[1])-ws]: bad_snip+=1
                    ctx=text[max(0,ws+a-1):ws+b+6]
                    if re.match(r"Steel", occ) and text[ws+b:ws+b+5].lower()=="craft": in_steelcraft+=1
                    pg=page_at(ws+a)
                    if pg and EXCL.search(path(pg)): in_excluded+=1
                    if pg!=m["page"] and not (m["page"] is None and pg is None): bad_page+=1
print(f"mentions {total}: span!=form {bad_span}, snippet not in window {bad_snip}, page mismatch vs my page_at {bad_page}, 'Steel' spans followed by 'craft' {in_steelcraft}, spans inside excluded pages {in_excluded}")

print("\n=== D2. independent mechanical recount per window (tier-2 whole-word over text minus URL lines & excluded pages) ===")
# reconstruct per-window sent forms from groups' forms that have mentions in the window? Not available; use union of chunk forms (upper bound) -> count distinct snippets of mentions already in dump instead:
# Here: verify every tier-2 occurrence of every form that HAS a mention in that window is covered by a mention (coverage check).
def word_re(form):
    l=r"(?<!\w)" if re.match(r"\w",form[0]) else ""; r=r"(?!\w)" if re.match(r"\w",form[-1]) else ""
    return re.compile(l+re.escape(form)+r, re.I if len(form)>3 else 0)
def mask(text):
    out=list(text)
    for m in URL_RE.finditer(text):
        for i in range(m.start(),m.end()):
            if out[i]!="\n": out[i]=" "
    # excluded pages
    urls=[(m.start(),m.end(),m.group().strip()) for m in URL_RE.finditer(text)]
    for i,(a,b,u) in enumerate(urls):
        if EXCL.search(path(u)):
            e=urls[i+1][0] if i+1<len(urls) else len(text)
            for j in range(a,e):
                if out[j]!="\n": out[j]=" "
    return "".join(out)
uncovered=0; checked=0; examples=[]
for s in SUBJ:
    text=texts[s]
    for f in FIELDS:
        d=load(NEW,s,f)
        for cb,ch in d["chunks"].items():
            wins={w["window"]:w["sub_bounds"] for w in ch["fold"]["windows"]}
            for wi,sb in wins.items():
                ws,we=map(int,sb.split(":")); wt=text[ws:we]
                # preceding page: if window starts mid excluded page, mask head
                urls_before=[m for m in URL_RE.finditer(text[:ws])]
                pre=urls_before[-1].group().strip() if urls_before else None
                mt=mask(wt)
                if pre and EXCL.search(path(pre)):
                    first=URL_RE.search(wt); cut=first.start() if first else len(wt)
                    mt="".join(" " if c!="\n" else c for c in mt[:cut])+mt[cut:]
                spans_in_dump=set()
                forms_here=set()
                for g in ch["fold"]["groups"]:
                    for m in g["mentions"]:
                        if m["window"]==wi: spans_in_dump.add(tuple(m["span"])); forms_here.add(m["form"]); forms_here.add(m.get("sent_form") or m["form"])
                covered_pos=set()
                for a,b in spans_in_dump: covered_pos.update(range(a,b))
                for form in forms_here:
                    for mm in word_re(form).finditer(mt):
                        checked+=1
                        if not (set(range(mm.start(),mm.end())) & covered_pos):
                            uncovered+=1
                            if len(examples)<8: examples.append((s,f,sb,form,mt[max(0,mm.start()-40):mm.end()+40].replace("\n","⏎")))
print(f"tier-2 occurrences of collected forms: {checked}; NOT covered by any dump span: {uncovered}")
for e in examples: print("  ",e)

print("\n=== F. zero-hit forms + short forms ===")
for s in SUBJ:
    for f in FIELDS:
        d=load(NEW,s,f)
        for cb,ch in d["chunks"].items():
            for w in ch["fold"]["windows"]:
                if w["zero_hit_forms"]: print(f"  {s} {f} {w['sub_bounds']}: {w['zero_hit_forms']}")
# where do 'gap','N5','E6','cnc' come from
for s in SUBJ:
    d=load(NEW,s,"products")
    for cb,ch in d["chunks"].items():
        for g in ch["fold"]["groups"]:
            if g["key"] in ("gap","n5","e6","cnc","lcn","tgp","vip"):
                ms=g["mentions"][:2]
                print("  ",s,g["key"],g["forms"],g["mention_count"],[ (m["snippet"][:80]) for m in ms])

print("\n=== G. empty groups classification ===")
swallowed=0; zero=0; other=[]
for s in SUBJ:
    text=texts[s]
    for f in FIELDS:
        d=load(NEW,s,f)
        for cb,ch in d["chunks"].items():
            zh=set(); 
            for w in ch["fold"]["windows"]: zh|=set(w["zero_hit_forms"])
            allspans=[]
            for g in ch["fold"]["groups"]:
                for m in g["mentions"]: allspans.append((m["window"],m["span"][0],m["span"][1],m["form"]))
            for g in ch["fold"]["groups"]:
                if g["status"]=="ok": continue
                if any(fm in zh for fm in g["forms"]): zero+=1; continue
                # swallowed: some mention span text contains the form
                cs,ce=map(int,cb.split(":")); ct=text[cs:ce]
                hit=False
                for fm in g["forms"]:
                    for mm in word_re(fm).finditer(ct):
                        # is there a longer mention covering it? approximate: any span in allspans whose window+span covers — skip precision; check that a mention's form contains fm
                        hit=True; break
                    if hit: break
                cont=any(fm.lower() in m2[3].lower() and fm.lower()!=m2[3].lower() for fm in g["forms"] for m2 in allspans)
                if cont: swallowed+=1
                else: other.append((s,f,g["forms"],hit))
print(f"empty groups: swallowed-by-longer-owner {swallowed}, zero-hit {zero}, other {len(other)}")
for o in other[:15]: print("  ",o)

print("\n=== C. LOCATION SAMPLES ===")
items=[]
for s in SUBJ:
    for f in FIELDS:
        d=load(NEW,s,f)
        for cb,ch in d["chunks"].items():
            seen=set()
            for g in ch["fold"]["groups"]:
                for m in g["mentions"]:
                    k=(m["window"],m["mention_id"])
                    if k in seen: continue
                    seen.add(k); items.append((s,f,cb,g["key"],m))
def show(label, sel, n=8):
    print(f"\n--- {label} ({len(sel)}) ---")
    for s,f,cb,key,m in sel[:n]:
        print(f"[{s[:5]} {f[:10]} w{m['window']} key={key[:25]}] SNIP: {m['snippet'][:140]!r}")
        print(f"      LOC: {m['location'][:420]}")
random.shuffle(items)
show("random", items, 22)
show("location_source=none", [x for x in items if x[4]["location_source"]=="none"], 5)
show("snippet == form (bare)", [x for x in items if x[4]["snippet"].strip()==x[4]["form"]], 8)
show("testimonial/quote words", [x for x in items if re.search(r"testimonial|quot", x[4]["location"], re.I)], 6)
show("job/career", [x for x in items if re.search(r"job|career|posting|hiring", x[4]["location"], re.I)], 4)
show("hedge", [x for x in items if re.search(r"cannot|unclear|mid-page|before any URL|not (shown|clear|possible|evident)|unknown", x[4]["location"], re.I)], 6)
show("longest locations", sorted(items,key=lambda x:-len(x[4]["location"])), 5)
show("shortest locations", sorted(items,key=lambda x:len(x[4]["location"])), 8)
show("alecmfg case-study listing titles (🇫🇷 etc)", [x for x in items if x[0]=="alecmfg_com" and ("|" in x[4]["snippet"]) ], 6)
show("steelcraft footer/menu repeats (Steel group)", [x for x in items if x[0]=="steelcraft_com" and x[3]=="steel"], 10)
show("restatement-heavy (overlap>=0.8)", [x for x in items if (lambda sw,lw: sw and len(sw&lw)/len(sw)>=0.8)({w for w in re.findall(r"[a-z0-9]+",x[4]["snippet"].lower()) if len(w)>3},{w for w in re.findall(r"[a-z0-9]+",x[4]["location"].lower()) if len(w)>3})], 8)
