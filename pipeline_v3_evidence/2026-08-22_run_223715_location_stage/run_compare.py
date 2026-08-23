"""Run 20260822T223715 (mechanical collection + Location stage) vs 20260822T195947."""
import json, glob, os, re, statistics as st, math
from collections import Counter, defaultdict

ROOT = "/Users/amaryadav/Documents/ASU_PhD/SUDOKN/data-ETL-pipeline"
NEW = f"{ROOT}/packages/logs/extraction_dumps/20260822T223715"
PREV = f"{ROOT}/packages/logs/extraction_dumps/20260822T195947"
TXT = f"{ROOT}/apps/data_etl_app/src/data_etl_app/knowledge/sample_scraped_texts"
PHRASE_FIELDS = ["conformity_attestations","contract_products","equipments","industries","material_caps","process_caps","products"]
SUBJECTS = ["alecmfg_com","steelcraft_com"]

def load(run, subj, field):
    p = f"{run}/{subj}__{field}__partial.json"
    return json.load(open(p)) if os.path.exists(p) else None

def stage_tokens(d, stage):
    t = d["run"]["token_usage"]["by_stage"].get(stage, {})
    return t.get("input_tokens",0), t.get("output_tokens",0)

def mention_requests(d):
    out=[]
    for cb,ch in d["chunks"].items():
        mr = ch["requests"].get("llm_phrase_mention_collection",{})
        if isinstance(mr, dict):
            for sb, reqs in mr.items():
                for r in reqs: out.append((cb,sb,r))
        else:
            for r in mr: out.append((cb,None,r))
    return out

def search_requests(d):
    out=[]
    for cb,ch in d["chunks"].items():
        for r in ch["requests"].get("llm_phrase_search",[]): out.append((cb,r))
    return out

print("="*100)
print("PER FIELD-SUBJECT: new run 223715  (prev 195947 in parentheses where comparable)")
print("="*100)
hdr = f"{'subject':10} {'field':24} {'win':>3} {'mreq':>4} {'dum':>3} {'s_in':>7} {'s_out':>5} {'m_in':>7} {'m_out':>6} {'wall':>5} | {'forms':>5} {'0hit':>4} {'grp':>4} {'emp':>3} {'ment':>5} {'dsnip':>5} {'desc':>5} {'nd':>3} {'disc':>4} {'excl':>4}"
print(hdr)
tot = Counter(); ptot=Counter()
rows_new = {}
all_mentions = []   # (subj, field, chunk, mention dict, group)
all_windows = []
all_groups = []
all_excluded = Counter()
prev_windows_unacc = Counter()
req_rows = []
for subj in SUBJECTS:
    for field in PHRASE_FIELDS:
        d = load(NEW, subj, field); p = load(PREV, subj, field)
        if d is None: print("MISSING", subj, field); continue
        s_in,s_out = stage_tokens(d,"llm_phrase_search"); m_in,m_out = stage_tokens(d,"llm_phrase_mention_collection")
        wall = d["run"]["time_span"]["seconds"]
        mreqs = mention_requests(d)
        dummies = sum(1 for _,_,r in mreqs if (r.get("output_tokens") or 0)<=5 or "dummy" in r.get("custom_id",""))
        nwin=0; forms=0; zerohit=0; groups=0; empties=0; ments=0; dsnip=0; desc=0; nd=0; disc=0; excl=0
        for cb,ch in d["chunks"].items():
            f = ch["fold"]; s=f["summary"]
            groups+=s["groups"]; empties+=s["empty_groups"]; ments+=s["mentions"]; dsnip+=s["distinct_snippets"]
            desc+=s["described"]; nd+=s["not_described"]; disc+=s["discovered_casings"]
            nwin+=s["windows"]
            for w in f["windows"]:
                forms+=w["sent_forms"]; zerohit+=len(w["zero_hit_forms"]); excl+=len(w["excluded_pages"])
                for u in w["excluded_pages"]: all_excluded[(subj,u)]+=1
                all_windows.append((subj,field,cb,w))
            for g in f["groups"]:
                all_groups.append((subj,field,cb,g))
                for m in g["mentions"]:
                    all_mentions.append((subj,field,cb,m,g))
        for cb,sb,r in mreqs: req_rows.append((subj,field,cb,sb,r))
        # prev
        ps = ""
        if p is not None:
            pm_in,pm_out = stage_tokens(p,"llm_phrase_mention_collection"); ps_in,ps_out=stage_tokens(p,"llm_phrase_search")
            pg=pe=pm=pun=pob=0
            for cb,ch in p["chunks"].items():
                s=ch["fold"]["summary"]; pg+=s.get("groups",0); pe+=s.get("empty_groups",0); pm+=s.get("mentions",0)
                for w in ch["fold"].get("windows",[]):
                    pun+=len(w.get("unaccounted",[])) if isinstance(w.get("unaccounted"),list) else w.get("unaccounted",0) if isinstance(w.get("unaccounted"),int) else 0
            ptot.update(dict(s_in=ps_in,s_out=ps_out,m_in=pm_in,m_out=pm_out,groups=pg,empties=pe,ments=pm,wall=p["run"]["time_span"]["seconds"], mreq=len(mention_requests(p))))
            ps = f"(prev m_in {pm_in:,} m_out {pm_out:,} grp {pg} emp {pe} ment {pm} wall {p['run']['time_span']['seconds']})"
        print(f"{subj:10} {field:24} {nwin:3} {len(mreqs):4} {dummies:3} {s_in:7,} {s_out:5,} {m_in:7,} {m_out:6,} {wall:5} | {forms:5} {zerohit:4} {groups:4} {empties:3} {ments:5} {dsnip:5} {desc:5} {nd:3} {disc:4} {excl:4}  {ps}")
        if field != "contract_products":  # shares identity w/ products: don't double count
            tot.update(dict(s_in=s_in,s_out=s_out,m_in=m_in,m_out=m_out,wall=wall,mreq=len(mreqs),dum=dummies,win=nwin,forms=forms,zerohit=zerohit,groups=groups,empties=empties,ments=ments,dsnip=dsnip,desc=desc,nd=nd,disc=disc))
print()
print("TOTALS (6 fields x 2 subjects; contract_products excluded as a products duplicate):")
print(json.dumps(dict(tot), indent=0))
print("PREV TOTALS (all 14 files incl. contract_products — see per-row prev for exact):", json.dumps(dict(ptot)))

# Prev tokens for 6 fields only
ptot6=Counter()
for subj in SUBJECTS:
    for field in PHRASE_FIELDS:
        if field=="contract_products": continue
        p=load(PREV,subj,field)
        if p is None: continue
        pm_in,pm_out=stage_tokens(p,"llm_phrase_mention_collection"); ps_in,ps_out=stage_tokens(p,"llm_phrase_search")
        pg=pe=pm=0
        for cb,ch in p["chunks"].items():
            s=ch["fold"]["summary"]; pg+=s.get("groups",0); pe+=s.get("empty_groups",0); pm+=s.get("mentions",0)
        ptot6.update(dict(s_in=ps_in,s_out=ps_out,m_in=pm_in,m_out=pm_out,groups=pg,empties=pe,ments=pm,wall=p["run"]["time_span"]["seconds"],mreq=len(mention_requests(p))))
print("PREV TOTALS 6 fields:", json.dumps(dict(ptot6)))
print(f"mention stage tokens: prev in {ptot6['m_in']:,} → new {tot['m_in']:,} ({tot['m_in']/ptot6['m_in']:.0%});  out {ptot6['m_out']:,} → {tot['m_out']:,} ({tot['m_out']/ptot6['m_out']:.0%})   [estimate was 85% / 41%]")
print(f"search tokens: prev in {ptot6['s_in']:,} → new {tot['s_in']:,};  out {ptot6['s_out']:,} → {tot['s_out']:,}")
print(f"wall sum prev {ptot6['wall']} → new {tot['wall']} s; mention requests prev {ptot6['mreq']} → new {tot['mreq']} ({tot['dum']} dummies)")
print(f"mentions prev {ptot6['ments']:,} → new {tot['ments']:,}; groups {ptot6['groups']} → {tot['groups']}; empty {ptot6['empties']} → {tot['empties']}")

# ---------------- search replay check ----------------
print("\n=== SEARCH identity vs prev (same custom_id = replay-able) ===")
same=diff=0; pv_new=set(); pv_prev=set()
for subj in SUBJECTS:
    for field in PHRASE_FIELDS:
        d=load(NEW,subj,field); p=load(PREV,subj,field)
        if d is None or p is None: continue
        n={r["custom_id"] for _,r in search_requests(d)}; o={r["custom_id"] for _,r in search_requests(p)}
        same+=len(n&o); diff+=len(n-o)
        pv_new|={re.search(r"pv=([^|]+)",c).group(1) for c in n}; pv_prev|={re.search(r"pv=([^|]+)",c).group(1) for c in o}
print(f"search custom ids identical to prev: {same}, new: {diff}; pv new {pv_new} prev {pv_prev}")
# search windows in new run: sub-window bounds
print("\n=== WINDOW GEOMETRY (new) ===")
for subj in SUBJECTS:
    d=load(NEW,subj,"material_caps")
    for cb,ch in d["chunks"].items():
        subs=[w["sub_bounds"] for w in ch["fold"]["windows"]]
        sizes=[(int(b.split(':')[1])-int(b.split(':')[0])) for b in subs]
        print(f"  {subj} chunk {cb}: sub-windows {subs} sizes {sizes}")

# ---------------- excluded pages ----------------
print("\n=== EXCLUDED PAGES (distinct URL per subject; count = windows x fields seen) ===")
for (subj,u),c in sorted(all_excluded.items()):
    print(f"  {subj:10} {u}  x{c}")

# ---------------- per-request output verbosity ----------------
print("\n=== MENTION-STAGE (Location) REQUESTS: tokens per request, items per window ===")
win_items = {}
for subj,field,cb,w in all_windows:
    win_items[(subj,field,cb,w["sub_bounds"])]=w["distinct_snippets"]
by_req=[]
for subj,field,cb,sb,r in req_rows:
    items=win_items.get((subj,field,cb,sb))
    gs=int(re.search(r"gs=(\d+)",r["custom_id"]).group(1)) if "gs=" in r["custom_id"] else None
    by_req.append((subj,field,cb,sb,items,r["input_tokens"] or 0,r["output_tokens"] or 0,r.get("client_latency_ms")))
outs=[b[6] for b in by_req if b[6]>5]; ins=[b[5] for b in by_req]
print(f"requests {len(by_req)}; input tokens median {st.median(ins):.0f} p90 {sorted(ins)[int(.9*len(ins))]} max {max(ins)}; output median {st.median(outs):.0f} p90 {sorted(outs)[int(.9*len(outs))]} max {max(outs)}")
# requests per window
rpw=Counter((s,f,c,sb) for s,f,c,sb,*_ in by_req)
print("requests per window distribution:", Counter(rpw.values()))
# items per window distribution
iw=[w["distinct_snippets"] for _,_,_,w in all_windows]
print(f"items (distinct snippets) per window: median {st.median(iw)}, p90 {sorted(iw)[int(.9*len(iw))]}, max {max(iw)}, zero-item windows {sum(1 for x in iw if x==0)}, windows {len(iw)}")
# output tokens per item (window-level: sum out tokens / items)
wo=defaultdict(int)
for s,f,c,sb,items,i,o,lat in by_req: wo[(s,f,c,sb)]+=o
per_item=[wo[k]/v for k,v in win_items.items() if v>0 and k in wo]
print(f"output tokens per item (per window): median {st.median(per_item):.1f}, p90 {sorted(per_item)[int(.9*len(per_item))]:.1f}, max {max(per_item):.1f}   [estimate assumed 40]")
lat=[b[7] for b in by_req if b[7]]
print(f"client latency ms: median {st.median(lat):.0f}, p90 {sorted(lat)[int(.9*len(lat))]}, max {max(lat)}")
print("top 8 requests by output tokens:")
for b in sorted(by_req,key=lambda x:-x[6])[:8]: print("  ",b[:7])
print("top 5 by input tokens:")
for b in sorted(by_req,key=lambda x:-x[5])[:5]: print("  ",b[:7])

# ---------------- location analysis ----------------
print("\n=== LOCATION TEXT ANALYSIS (distinct (subject, field, chunk, window, mention_id)) ===")
seen=set(); locs=[]
for subj,field,cb,m,g in all_mentions:
    k=(subj,field,cb,m["window"],m["mention_id"])
    if k in seen: continue
    seen.add(k); locs.append((subj,field,cb,m))
L=[m["location"] for *_,m in locs]
src=Counter(m["location_source"] for *_,m in locs)
print("location_source:",dict(src), " distinct described items:",len(L))
ln=[len(x) for x in L]
print(f"location chars: median {st.median(ln)}, p90 {sorted(ln)[int(.9*len(ln))]}, max {max(ln)}, total {sum(ln):,}")
sn=[len(m["snippet"]) for *_,m in locs]
print(f"snippet chars (distinct items): median {st.median(sn)}, p90 {sorted(sn)[int(.9*len(sn))]}, max {max(sn)}, total {sum(sn):,}; multi-line {sum(1 for *_,m in locs if chr(10) in m['snippet'])}")
def share(pred, label):
    c=sum(1 for x in L if pred(x)); print(f"  {label:55} {c:5} ({c/len(L):.0%})")
share(lambda x: "http" in x, "carries a URL")
share(lambda x: re.match(r"^(This|The) (phrase|line|passage|sentence|text|heading|mention|item|entry|statement|quote|bullet|snippet|title|paragraph)", x) is not None, "starts 'This/The phrase|line|passage…'")
share(lambda x: re.search(r"\b(menu|navigation|nav)\b", x, re.I) is not None, "mentions menu/navigation")
share(lambda x: re.search(r"\bheading|header|title\b", x, re.I) is not None, "mentions heading/header/title")
share(lambda x: re.search(r"\b(list|bullet)", x, re.I) is not None, "mentions list/bullet")
share(lambda x: re.search(r"\btable\b", x, re.I) is not None, "mentions table")
share(lambda x: re.search(r"\bprose|paragraph|body text|sentence\b", x, re.I) is not None, "mentions prose/paragraph/sentence")
share(lambda x: re.search(r"testimonial|quot", x, re.I) is not None, "testimonial/quote")
share(lambda x: re.search(r"\bblog|news|article|press", x, re.I) is not None, "blog/news/article")
share(lambda x: re.search(r"case[- ]stud", x, re.I) is not None, "case study")
share(lambda x: re.search(r"job|career|posting|hiring|position", x, re.I) is not None, "job/career")
share(lambda x: re.search(r"footer", x, re.I) is not None, "footer")
share(lambda x: re.search(r"boilerplate|repeat|recur|across (the )?(site|pages|multiple)|every page|each page|several pages|multiple pages", x, re.I) is not None, "recurrence noted")
share(lambda x: re.search(r"site'?s own|company'?s own|own copy|own (description|words)|the (company|site|manufacturer) (itself|describ|state)|first[- ]person|self-description", x, re.I) is not None, "speaker = site's own copy")
share(lambda x: re.search(r"customer|client|reviewer|visitor|user (review|comment)", x, re.I) is not None, "speaker = customer/client")
share(lambda x: re.search(r"publication|magazine|press|third[- ]party|another party|partner", x, re.I) is not None, "speaker = publication/third party")
share(lambda x: re.search(r"cannot|unclear|not (shown|clear|possible|evident|indicated)|mid-page|before any URL|no URL|unknown|does not (show|indicate)|not determinable", x, re.I) is not None, "hedge / can't tell")
share(lambda x: re.search(r"\b(product|service|capabilit|page about|page (for|on|describ))", x, re.I) is not None, "says what the page is about")
share(lambda x: re.search(r"describ(es|ing)|states|claims|explains|highlights|emphasiz|indicat(es|ing) that|showing that|demonstrat", x, re.I) is not None, "restates/summarizes content (describes/states/claims…)")
share(lambda x: re.search(r"\bstating that|which (describes|states|says|lists|highlights)|that (describes|states|lists|explains|highlights)", x, re.I) is not None, "'…stating/that describes…' restatement pattern")
share(lambda x: re.search(r"Steelcraft|Alec|ALEC", x) is not None, "names the company")
share(lambda x: len(x) < 60, "short (<60 chars)")
share(lambda x: len(x) > 250, "long (>250 chars)")
# content-word overlap between location and snippet (restating)
STOP=set("the a an of in on at to for and or is are as by with from this that these those it its their our your we you they be been being was were which who whom where when how what why all any each such into over under about after before between during without within across not no nor so than then too very can will just also more most other some only own same s t".split())
def words(x): return {w for w in re.findall(r"[a-z0-9]+", x.lower()) if w not in STOP and len(w)>2}
ov=[]
for *_,m in locs:
    sw=words(m["snippet"]); lw=words(m["location"])
    if sw: ov.append(len(sw&lw)/len(sw))
print(f"content-word overlap location∩snippet / snippet: median {st.median(ov):.2f}, share ≥0.5: {sum(1 for o in ov if o>=.5)/len(ov):.0%}, share ≥0.8: {sum(1 for o in ov if o>=.8)/len(ov):.0%}")

# same mention_id across windows: differing locations?
print("\n=== same mention_id (same snippet) described in >1 window — location consistency ===")
by_id=defaultdict(list)
for subj,field,cb,m in locs: by_id[(subj,field,m["mention_id"])].append((cb,m["window"],m["location"]))
multi=[(k,v) for k,v in by_id.items() if len(v)>1]
differ=[(k,v) for k,v in multi if len({x[2] for x in v})>1]
print(f"ids seen in >1 window: {len(multi)}; with differing location text: {len(differ)}")
for k,v in differ[:4]:
    print("  ",k)
    for x in v: print("      w",x[0],x[1],"::",x[2][:160])

# ---------------- groups ----------------
print("\n=== GROUPS ===")
mc=[g["mention_count"] for *_,g in all_groups]; ds=[g["distinct_snippets"] for *_,g in all_groups]
print(f"groups {len(all_groups)}; mentions/group median {st.median(mc)}, p90 {sorted(mc)[int(.9*len(mc))]}, max {max(mc)}; distinct snippets/group median {st.median(ds)}, max {max(ds)}")
print("top 12 groups by mention_count:")
for subj,field,cb,g in sorted(all_groups,key=lambda x:-x[3]["mention_count"])[:12]:
    print(f"  {subj:10} {field:22} {cb:14} {g['key'][:35]:35} ments {g['mention_count']:4} dsnip {g['distinct_snippets']:3} forms {g['forms'][:4]}")
print("multi-form groups (≥3 forms) sample:")
mf=[(s,f,c,g) for s,f,c,g in all_groups if len(g["forms"])>=3]
print(f"  count {len(mf)}")
for s,f,c,g in mf[:10]: print(f"  {f:22} {g['key'][:30]:30} {g['forms']}")
print("empty groups sample:")
eg=[(s,f,c,g) for s,f,c,g in all_groups if g["status"]!="ok"]
print(f"  count {len(eg)}, statuses {Counter(g['status'] for *_,g in eg)}")
for s,f,c,g in eg[:12]: print(f"  {s:10} {f:22} {g['key'][:40]} forms {g['forms']}")

# ---------------- sent form provenance ----------------
print("\n=== sent forms provenance (chunk rows) ===")
prov=Counter()
for subj in SUBJECTS:
    for field in PHRASE_FIELDS:
        if field=="contract_products": continue
        d=load(NEW,subj,field)
        for cb,ch in d["chunks"].items():
            for r in ch["rows"]: prov[(r.get("provenance"), r.get("search_round"))]+=1
print(dict(prov))
# discovered casings overall
print("\n=== discovered casings (tier-2 casings no sent form covered; sample) ===")
dc=Counter()
for subj,field,cb,w in all_windows:
    for f,cs in w["discovered_casings"].items(): dc[(field,f,tuple(cs))]+=1
print("distinct (field, form, casings):",len(dc))
for k,c in list(dc.items())[:15]: print("  ",k,c)
# sent_form != form (rescued casing) in mentions
resc=sum(1 for *_,m,g in all_mentions if m.get("sent_form") and m["sent_form"]!=m["form"])
print(f"mentions whose form is a rescued casing (sent_form != form): {resc} of {len(all_mentions)}")
# short forms
sf=Counter()
for subj,field,cb,w in all_windows:
    for f in w["short_forms"]: sf[(field,f)]+=1
print("short forms (≤3 chars) flagged:", dict(sf))
