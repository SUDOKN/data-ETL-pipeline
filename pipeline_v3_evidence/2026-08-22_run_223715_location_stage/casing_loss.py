import json, os, re, statistics as st
from collections import Counter, defaultdict
ROOT="/Users/amaryadav/Documents/ASU_PhD/SUDOKN/data-ETL-pipeline"
NEW=f"{ROOT}/packages/logs/extraction_dumps/20260822T223715"
FIELDS=["conformity_attestations","equipments","industries","material_caps","process_caps","products"]
SUBJ=["alecmfg_com","steelcraft_com"]
def load(s,f): return json.load(open(f"{NEW}/{s}__{f}__partial.json"))
print("=== B. rescued casings: top (sent_form -> form) pairs ===")
pairs=Counter(); 
for s in SUBJ:
    for f in FIELDS:
        d=load(s,f)
        for cb,ch in d["chunks"].items():
            for g in ch["fold"]["groups"]:
                for m in g["mentions"]:
                    sf=m.get("sent_form") or m["form"]
                    if sf!=m["form"]: pairs[(f,sf,m["form"])]+=1
print("distinct pairs", len(pairs), "mentions", sum(pairs.values()))
for k,c in pairs.most_common(20): print("  ",k,c)
print("\n=== B2. polyseme groups: 'lead' composition; 'production'; 'part' ===")
for s in SUBJ:
    for f in FIELDS:
        d=load(s,f)
        for cb,ch in d["chunks"].items():
            for g in ch["fold"]["groups"]:
                if g["key"] in ("lead","production","part","gap","steel") and g["mentions"]:
                    kinds=Counter()
                    for m in g["mentions"]:
                        sn=m["snippet"]
                        if re.search(r"lead[- ]?time", sn, re.I): kinds["lead time"]+=1
                        elif re.search(r"\blead(s|ing)? (to|the|in)\b|\bleads\b|lead (engineer|designer|manager|product|project)|Project Lead|Team Lead|\blead (by|up|with)\b|, Lead\b|Lead,", sn): kinds["lead (verb/role)"]+=1
                        elif re.search(r"\bLead\b|\blead\b", sn): kinds["lead (other)"]+=1
                        else: kinds["n/a"]+=1
                    if g["key"]=="lead": print(f"  {s} {f} {cb} key=lead forms={g['forms']} mentions={g['mention_count']} kinds={dict(kinds)}")
                    if g["key"]=="lead":
                        for m in g["mentions"][:6]: print("      ", m["form"], "|", m["snippet"][:90])
                    if g["key"] in ("production","part","gap") and f in ("process_caps","products"): print(f"  {s} {f} {cb} key={g['key']} forms={g['forms']} mentions={g['mention_count']} dsnip={g['distinct_snippets']}")
