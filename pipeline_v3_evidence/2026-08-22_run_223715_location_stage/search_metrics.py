"""Search per-window consistency metric (Q3) + products search outputs for the self-evaluation (finding 8)."""
import json, re, os, statistics as st
from collections import Counter, defaultdict
SP=os.path.dirname(os.path.abspath(__file__))
ROOT="/Users/amaryadav/Documents/ASU_PhD/SUDOKN/data-ETL-pipeline"
NEW=f"{ROOT}/packages/logs/extraction_dumps/20260822T223715"
docs=json.load(open(f"{SP}/all_requests_new.json"))
URL_RE=re.compile(r"^[ \t]*https?://\S+[ \t]*$", re.M); SEP_RE=re.compile(r"^[ \t]*#{10,}[ \t]*$", re.M)
def word_re(form):
    l=r"(?<!\w)" if re.match(r"\w",form[0]) else ""; r=r"(?!\w)" if re.match(r"\w",form[-1]) else ""
    return re.compile(l+re.escape(form)+r, 0 if len(form)<=3 else re.I)
def mask(wt):
    out=list(wt)
    for rx in (URL_RE,SEP_RE,re.compile(r"\[page content omitted\]")):
        for m in rx.finditer(wt):
            for i in range(m.start(),m.end()):
                if out[i]!="\n": out[i]=" "
    return "".join(out)
# search vocab per (subject, field, sub_bounds) from the search docs; wire text per window from the Location docs (fallback: search doc text)
vocab={}; wire={}
for d in docs:
    c=d["custom_id"]; subj=c.split(">")[0]; field=c.split(">")[1]
    if ">llm_search>" in c:
        sb=re.search(r">sub>([0-9:]+)",c).group(1)
        try:
            o=json.loads(d["content"]); ph=next((v for v in o.values() if isinstance(v,list)),[])
        except Exception: ph=[]
        forms=[p if isinstance(p,str) else (p.get("phrase") or p.get("form") or json.dumps(p)) for p in ph]
        vocab[(subj,field,sb)]=forms
        um=d["user_message"]; i=um.find("\n\n",0)+2  # after nonce
        wire.setdefault((subj,sb), um[i:])
    elif ">llm_phrase_mention_collection>" in c:
        sb=re.search(r">sub>([0-9:]+)>",c).group(1)
        um=d["user_message"]; i=um.find("text scraped from a manufacturer's website:\n")+len("text scraped from a manufacturer's website:\n"); j=um.rfind("\n\n<<<MENTION_IDS")
        wire[(subj,sb)]=um[i:j]
print("search windows:", len(vocab), "wire windows:", len(set(wire)))
FIELDS=sorted({k[1] for k in vocab})
print("\n=== Q3: SEARCH PER-WINDOW CONSISTENCY on its own vocabulary (family = casefold form; occurrence = whole-word tier-2 in the window's scan domain) ===")
print("scope: DOC = the subject's whole search vocabulary for the field (both chunks); CHUNK = the chunk's 4 sub-windows")
print(f"{'subject':10} {'field':24} {'fam':>4} {'occ':>5} {'listed':>6} {'inLonger':>8} {'missed':>6} {'rate':>6} | {'missedLines':>11}")
agg=Counter(); miss_examples=defaultdict(list)
for subj in ["alecmfg.com","steelcraft.com"]:
    for field in FIELDS:
        wins=sorted({k[2] for k in vocab if k[0]==subj and k[1]==field}, key=lambda x:int(x.split(":")[0]))
        docvocab=set(); 
        for sb in wins: docvocab|={f.casefold() for f in vocab[(subj,field,sb)] if f.strip()}
        fam_forms=defaultdict(set)
        for sb in wins:
            for f in vocab[(subj,field,sb)]:
                if f.strip(): fam_forms[f.casefold()].add(f)
        occ=listed=inlonger=missed=0; mlines=set()
        for sb in wins:
            wt=wire.get((subj,sb)); 
            if wt is None: continue
            dom=mask(wt); local={f.casefold() for f in vocab[(subj,field,sb)] if f.strip()}
            # positions of LISTED families' occurrences (for the inside-longer test)
            listed_pos=set()
            for fam in local:
                for form in fam_forms[fam]:
                    for mm in word_re(form).finditer(dom): listed_pos.update(range(mm.start(),mm.end()))
            for fam in docvocab:
                seen=set()
                for form in fam_forms[fam]:
                    for mm in word_re(form).finditer(dom):
                        if mm.start() in seen: continue
                        seen.add(mm.start()); occ+=1
                        if fam in local: listed+=1
                        elif set(range(mm.start(),mm.end())) & listed_pos: inlonger+=1
                        else:
                            missed+=1
                            ls=dom.rfind("\n",0,mm.start())+1; le=dom.find("\n",mm.end()); le=len(dom) if le<0 else le
                            mlines.add(wt[ls:le].strip()[:100])
                            if len(miss_examples[(subj,field)])<4: miss_examples[(subj,field)].append((fam, sb, wt[max(0,mm.start()-40):mm.end()+30].replace("\n","⏎")))
        rate=(listed+inlonger)/occ if occ else float('nan')
        print(f"{subj:10} {field:24} {len(docvocab):4} {occ:5} {listed:6} {inlonger:8} {missed:6} {rate:6.0%} | {len(mlines):11}")
        agg.update(dict(occ=occ,listed=listed,inlonger=inlonger,missed=missed))
print(f"TOTAL: occurrences {agg['occ']}, listed {agg['listed']}, inside a longer listed form {agg['inlonger']}, MISSED {agg['missed']} → consistency {(agg['listed']+agg['inlonger'])/agg['occ']:.0%}")
print("\nexamples of missed occurrences (family, window, context):")
for k,v in miss_examples.items():
    for e in v[:2]: print("  ",k, e)

# ---- products: search outputs with one snippet of context per form, for the precision judgment; window texts to files
print("\n=== PRODUCTS search outputs per window (written to products_search_lists.txt; window texts to products_window_<subject>_<bounds>.txt) ===")
snip={}
for subj,fn in [("alecmfg.com","alecmfg_com"),("steelcraft.com","steelcraft_com")]:
    d=json.load(open(f"{NEW}/{fn}__products__partial.json"))
    for cb,ch in d["chunks"].items():
        for g in ch["fold"]["groups"]:
            for m in g["mentions"]:
                snip.setdefault((subj,m.get("sent_form") or m["form"]), m["snippet"][:150]); snip.setdefault((subj,m["form"]), m["snippet"][:150])
out=[]
for subj in ["alecmfg.com","steelcraft.com"]:
    wins=sorted({k[2] for k in vocab if k[0]==subj and k[1]=="products"}, key=lambda x:int(x.split(":")[0]))
    for sb in wins:
        forms=vocab[(subj,"products",sb)]
        out.append(f"\n##### {subj} products window {sb}: {len(forms)} forms")
        for f in forms: out.append(f"  - {f!r}   <<{snip.get((subj,f),'(no snippet: zero-hit or swallowed)')}>>")
        wt=wire.get((subj,sb))
        if wt: open(f"{SP}/products_window_{subj.split('.')[0]}_{sb.replace(':','-')}.txt","w").write(wt)
open(f"{SP}/products_search_lists.txt","w").write("\n".join(out))
print("products windows per subject:", {s: len([k for k in vocab if k[0]==s and k[1]=='products']) for s in ['alecmfg.com','steelcraft.com']})
for subj in ["alecmfg.com","steelcraft.com"]:
    for sb in sorted({k[2] for k in vocab if k[0]==subj and k[1]=="products"}, key=lambda x:int(x.split(":")[0])):
        print(f"  {subj} {sb}: {len(vocab[(subj,'products',sb)])} forms; wire chars {len(wire.get((subj,sb),''))}")
