import json, os, re, statistics as st
from collections import Counter, defaultdict
ROOT="/Users/amaryadav/Documents/ASU_PhD/SUDOKN/data-ETL-pipeline"
NEW=f"{ROOT}/packages/logs/extraction_dumps/20260822T223715"
SP=os.path.dirname(os.path.abspath(__file__))  # expects all_requests_new.json beside this file (regenerate with pull_new.py; NOT committed — it holds the site text)
FIELDS=["conformity_attestations","equipments","industries","material_caps","process_caps","products"]
SUBJ={"alecmfg_com":"alecmfg.com","steelcraft_com":"steelcraft.com"}
docs=json.load(open(f"{SP}/all_requests_new.json"))
wire={}  # (subject, field, sub_bounds) -> wire text
for d in docs:
    c=d["custom_id"]
    if ">llm_phrase_mention_collection>" not in c: continue
    subj=c.split(">")[0]; field=c.split(">")[1]; sb=re.search(r">sub>([0-9:]+)>",c).group(1)
    um=d["user_message"]; i=um.find("text scraped from a manufacturer's website:\n")+len("text scraped from a manufacturer's website:\n"); j=um.rfind("\n\n<<<MENTION_IDS")
    wire[(subj,field,sb)]=um[i:j]
print("wire windows:", len(wire))
URL_RE=re.compile(r"^[ \t]*https?://\S+[ \t]*$", re.M); SEP_RE=re.compile(r"^[ \t]*#{10,}[ \t]*$", re.M)
def word_re(form):
    l=r"(?<!\w)" if re.match(r"\w",form[0]) else ""; r=r"(?!\w)" if re.match(r"\w",form[-1]) else ""
    return re.compile(l+re.escape(form)+r, 0 if len(form)<=3 else re.I)
def mask(wt):
    out=list(wt)
    for rx in (URL_RE,SEP_RE):
        for m in rx.finditer(wt):
            for i in range(m.start(),m.end()):
                if out[i]!="\n": out[i]=" "
    for m in re.finditer(r"\[page content omitted\]", wt):
        for i in range(m.start(),m.end()): out[i]=" "
    return "".join(out)
def load(s,f): return json.load(open(f"{NEW}/{s}__{f}__partial.json"))
G=Counter(); loss_lines_all=set(); loss_by_field=Counter(); loss_fams=Counter(); steelcraft_inside=0
for s,subj in SUBJ.items():
    exact_ok=exact_bad=0; snip_ok=snip_bad=0; cov_tot=cov_miss=0; nomarker=0; cov_ex=[]
    loss_occ=loss_all=0; loss_lines=set()
    for f in FIELDS:
        d=load(s,f)
        for cb,ch in d["chunks"].items():
            wins={w["window"]:w["sub_bounds"] for w in ch["fold"]["windows"]}
            chunk_fams=defaultdict(set)
            for g in ch["fold"]["groups"]:
                for fm in g["forms"]: chunk_fams[fm.casefold()].add(fm)
            win_ments=defaultdict(list)
            for g in ch["fold"]["groups"]:
                for m in g["mentions"]: win_ments[m["window"]].append(m)
            for wi,sb in wins.items():
                wt=wire.get((subj,f,sb))
                if wt is None: continue  # dummy window (no request)
                dom=mask(wt); has_marker="[page content omitted]" in wt
                if not has_marker: nomarker+=1
                fams_here={m["form"].casefold() for m in win_ments[wi]}
                for m in win_ments[wi]:
                    a,b=m["span"]
                    if not has_marker:
                        if wt[a:b]==m["form"]: exact_ok+=1
                        else: exact_bad+=1
                    if m["snippet"] in wt: snip_ok+=1
                    else: snip_bad+=1
                    if m["form"].lower()=="steel" and not has_marker and wt[b:b+5].lower()=="craft": steelcraft_inside+=1
                # occurrences of collected families in the wire scan domain
                pos_collected=set(); occ_collected=[]
                for fam in fams_here:
                    seen=set()
                    for form in chunk_fams.get(fam,{fam}):
                        for mm in word_re(form).finditer(dom):
                            if mm.start() in seen: continue
                            seen.add(mm.start()); occ_collected.append((mm.start(),mm.end())); pos_collected.update(range(mm.start(),mm.end()))
                # coverage: every collected-family occurrence must be covered by a dump span (exact windows only)
                if not has_marker:
                    covpos=set()
                    for m in win_ments[wi]: covpos.update(range(m["span"][0],m["span"][1]))
                    for a,b in occ_collected:
                        cov_tot+=1
                        if not (set(range(a,b)) & covpos):
                            cov_miss+=1
                            if len(cov_ex)<5: cov_ex.append((f,sb,dom[max(0,a-40):b+30].replace("\n","⏎")))
                # loss channel: families of the chunk never collected in this window
                for fam,forms in chunk_fams.items():
                    if fam in fams_here: continue
                    seen=set()
                    for form in forms:
                        for mm in word_re(form).finditer(dom):
                            if mm.start() in seen: continue
                            seen.add(mm.start()); loss_all+=1
                            if set(range(mm.start(),mm.end())) & pos_collected: continue   # inside a longer collected occurrence
                            loss_occ+=1; loss_by_field[f]+=1; loss_fams[(f,fam)]+=1
                            ls=dom.rfind("\n",0,mm.start())+1; le=dom.find("\n",mm.end()); le=len(dom) if le<0 else le
                            loss_lines.add((f,wt[ls:le].strip()))
    print(f"\n=== {s} (wire text from Mongo) ===")
    print(f"exact windows (no excluded-page marker): {nomarker}; spans verified {exact_ok} ok / {exact_bad} bad; snippets in wire window {snip_ok} ok / {snip_bad} bad; 'Steel' inside 'Steelcraft' {steelcraft_inside}")
    print(f"COVERAGE (exact windows): collected-family occurrences {cov_tot}, uncovered {cov_miss}", cov_ex)
    print(f"LOSS CHANNEL (family collected elsewhere in the chunk, never sent for this window): {loss_occ} uncovered occurrences (of {loss_all}); distinct (field, line) = {len(loss_lines)}")
    print("   by field:", dict(loss_by_field)); print("   top:", loss_fams.most_common(12))
    loss_lines_all|={(s,)+x for x in loss_lines}
print(f"\nTOTAL loss-channel: distinct (subject, field, line) = {len(loss_lines_all)}  (≈ extra Location wire items if forms were chunk-wide)")
import random; random.seed(3)
for x in random.sample(sorted(loss_lines_all), 12): print("   ", x[0][:5], x[1][:12], "|", x[2][:110])
