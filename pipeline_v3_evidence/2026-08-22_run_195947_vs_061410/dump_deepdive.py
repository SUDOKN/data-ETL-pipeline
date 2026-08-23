"""Deep-dive: unanchored composition, pages/junk, line-type miss rates, location style. Both runs."""
import json, re, sys, os, glob, collections, statistics
sys.path.insert(0, "/Users/amaryadav/Documents/ASU_PhD/SUDOKN/data-ETL-pipeline/packages/core/src")
from core.utils.floor_scan import find_form_occurrences
S=os.path.dirname(os.path.abspath(__file__)); ROOT="/Users/amaryadav/Documents/ASU_PhD/SUDOKN/data-ETL-pipeline"
RUNS={"prev":("20260822T061410",f"{S}/mention_requests_prev.json"),"new":("20260822T195947",f"{S}/all_requests.json")}
MARK="text scraped from a manufacturer's website:\n"
full={s: open(f"{ROOT}/apps/data_etl_app/src/data_etl_app/knowledge/sample_scraped_texts/{s}.txt",encoding="utf-8").read() for s in ["alecmfg.com","steelcraft.com"]}
def load_windows(path):
    W={}
    for r in json.load(open(path)):
        cid=r.get("custom_id") or ""; um=r.get("user_message") or ""
        if ">llm_phrase_mention_collection>" not in cid or MARK not in um: continue
        m=re.search(r"^([^>]+)>([^>]+)>llm_phrase_mention_collection>chunk>([^>]+)>sub>([^>]+)>",cid)
        W[(m.group(1),m.group(2),m.group(4))]=um.split(MARK,1)[1].rsplit("<<<PHRASES",1)[0]
    return W
def line_type(text, start):
    ls=text.rfind("\n",0,start)+1; le=text.find("\n",start); le=len(text) if le<0 else le; line=text[ls:le]; w=len(line.split())
    if re.match(r"^\s*(https?://|#{5,})", line): return "url/header"
    rep=text.count(line.strip()) if len(line.strip())>=12 else 1
    if w>=12 and re.search(r"[.!?]", line): return "prose"+(" (repeated)" if rep>=2 else "")
    return ("short/heading/list"+(" (repeated)" if rep>=2 else ""))
for label,(run,path) in RUNS.items():
    print("="*20, label, run)
    W=load_windows(path)
    unanch=collections.Counter(); unanch_ex=collections.defaultdict(collections.Counter); un_keys=None
    lt_unacc=collections.Counter(); lt_acc=collections.Counter(); lt_unacc_mat=collections.Counter(); lt_acc_mat=collections.Counter()
    page_groups=collections.defaultdict(set); page_mentions=collections.Counter(); flagged_only=collections.Counter(); forms_total=collections.Counter(); loc_samples=[]
    for p in sorted(glob.glob(f"{ROOT}/packages/logs/extraction_dumps/{run}/*__partial.json")):
        d=json.load(open(p)); subj=d["subject_unique_id"]; field=d["field_type"]
        if field in ("addresses","is_contract_manufacturer","is_product_manufacturer","contract_products"): continue
        for cb,ch in d["chunks"].items():
            fold=ch.get("fold")
            if not fold: continue
            wins=fold["windows"]
            for w in wins:
                text=W.get((subj,field,w["sub_bounds"]))
                for u in (w.get("unanchored") or []):
                    if un_keys is None: un_keys=list(u.keys())
                    kind="snippet==form" if u["snippet"].strip()==u["reported_form"].strip() else "other"
                    unanch[(field,kind)]+=1
                    if kind=="snippet==form": unanch_ex[(subj,field)][u["reported_form"]]+=1
                if text is None: continue
                for f,spans in (w.get("unaccounted") or {}).items():
                    for sp in spans:
                        lt=line_type(text, sp[0]); lt_unacc[lt]+=1
                        if subj=="alecmfg.com" and field=="material_caps": lt_unacc_mat[lt]+=1
            for g in fold["groups"]:
                forms_total[field]+=len(g["forms"])
                pages=set()
                for m in g["mentions"]:
                    pg=m.get("page") or "(none)"; pages.add(pg); page_groups[(subj,pg)].add((field,g["key"])); page_mentions[(subj,pg)]+=1
                    w=wins[m["window"]]; text=W.get((subj,field,w["sub_bounds"]))
                    if text is not None and m.get("span"):
                        lt=line_type(text, m["span"][0]); lt_acc[lt]+=1
                        if subj=="alecmfg.com" and field=="material_caps": lt_acc_mat[lt]+=1
                    if len(loc_samples)<8 and label=="new" and m.get("location"): loc_samples.append(m["location"][:140])
                if pages and all(re.search(r"privacy|terms|legal|cookie|policy|career|job|login|account|cart", pg, re.I) for pg in pages): flagged_only[field]+=len(g["forms"])
    print("unanchored record keys:", un_keys)
    print("UNANCHORED by field (snippet==form / other):", {f: (unanch[(f,'snippet==form')], unanch[(f,'other')]) for f in sorted({k[0] for k in unanch})})
    for (subj,field),c in sorted(unanch_ex.items(), key=lambda x:-sum(x[1].values()))[:6]:
        top=c.most_common(4); W_=None
        # for the top bare form: substring vs whole-word occurrences in the subject text
        info=[]
        for form,n in top[:2]:
            sub_occ=len(re.findall(re.escape(form), full[subj])); ww=len(find_form_occurrences(full[subj], form, case_sensitive=True))
            info.append((form, n, f"substr {sub_occ} / whole-word {ww} in subject text"))
        print(f"   {subj[:9]} {field[:14]} bare-form unanchored total {sum(c.values())}: top {top}; {info}")
    def show(c, cacc):
        keys=sorted(set(c)|set(cacc))
        return {k: f"miss {c[k]}/{c[k]+cacc[k]} ({c[k]/max(c[k]+cacc[k],1):.0%})" for k in keys}
    print("LINE-TYPE MISS RATES, all fields (unaccounted / (unaccounted+accounted mentions) by line type):", show(lt_unacc, lt_acc))
    print("LINE-TYPE MISS RATES, alecmfg material_caps:", show(lt_unacc_mat, lt_acc_mat))
    print("TOP PAGES by distinct (field,group) — per subject:")
    for subj in ["alecmfg.com","steelcraft.com"]:
        top=sorted([(pg,len(gs),page_mentions[(s,pg)]) for (s,pg),gs in page_groups.items() if s==subj], key=lambda x:-x[1])[:10]
        print(f"   {subj}: "+"; ".join(f"{pg[-60:]} g={n} m={mm}" for pg,n,mm in top))
    print("forms whose groups' mentions are ONLY on privacy/legal/careers/cart pages, by field:", dict(flagged_only), "| forms total by field:", dict(forms_total))
    if label=="new":
        print("LOCATION samples (new):"); [print("    ", l) for l in loc_samples]
# steelcraft newly-reached region 178148:192276 — which pages
print("\nsteelcraft region 178148:192276 URL lines:", re.findall(r"^https?://\S+$", full["steelcraft.com"][178148:192276], re.M)[:20])
print("alecmfg region 142870:? — new chunk2 ends at", 98612+44258, "; prev chunk2 ended at 142870")
