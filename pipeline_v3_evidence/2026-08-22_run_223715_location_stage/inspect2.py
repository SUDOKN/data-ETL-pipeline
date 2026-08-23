import json, re, os
from collections import Counter, defaultdict
SP=os.path.dirname(os.path.abspath(__file__))  # expects all_requests_new.json beside this file (regenerate with pull_new.py; NOT committed — it holds the site text)
ROOT="/Users/amaryadav/Documents/ASU_PhD/SUDOKN/data-ETL-pipeline"
NEW=f"{ROOT}/packages/logs/extraction_dumps/20260822T223715"
docs=json.load(open(f"{SP}/all_requests_new.json"))
ment=[d for d in docs if ">llm_phrase_mention_collection>" in d["custom_id"]]
print("=== 1. the 0/47 response (alecmfg industries 98612:123329) ===")
for d in ment:
    if "alecmfg.com>industries>" in d["custom_id"] and ">sub>98612:123329>" in d["custom_id"]:
        print("usage:", d["usage"]); print("content:", d["content"])
        um=d["user_message"]; i=um.rfind("<<<MENTIONS"); print("sent MENTIONS block head:", um[i:i+700])
        print("user msg len", len(um), "; count of '[page content omitted]':", um.count("[page content omitted]"))
        # where does the window start? show head
        print("window head:", um[:400].replace("\n","⏎"))
print("\n=== 1b. the 35/36 response (alecmfg material_caps 98612:123329): returned ids vs sent for 'Lead Time (Pilot Lot)' ===")
for d in ment:
    if "alecmfg.com>material_caps>" in d["custom_id"] and ">sub>98612:123329>" in d["custom_id"]:
        um=d["user_message"]; 
        for m in re.finditer(r'\{"mention_id":"(m[0-9a-z]{7})",\n"mention":"(.*?)"\}', um):
            if "Lead Time" in m.group(2): print("  sent:", m.group(1), m.group(2)[:100])
        obj=json.loads(d["content"]); ids=[x["mention_id"] for x in obj["mentions"]]
        print("  returned", len(ids), "; 'mlvaqsfl' in returned:", "mlvaqsfl" in ids)
        # the neighbours in the answer
        for x in obj["mentions"]:
            if "Lead" in x["location"] or "lead" in x["location"]: print("   loc mentioning lead:", x["mention_id"], x["location"][:200])

print("\n=== 2. alecmfg homepage cookie-policy text ===")
t=open(f"{ROOT}/apps/data_etl_app/src/data_etl_app/knowledge/sample_scraped_texts/alecmfg.com.txt").read()
URL_RE=re.compile(r"^[ \t]*https?://\S+[ \t]*$", re.M)
urls=[(m.start(),m.group().strip()) for m in URL_RE.finditer(t)]
for i,(a,u) in enumerate(urls[:6]):
    e=urls[i+1][0] if i+1<len(urls) else len(t)
    body=t[a:e]
    print(f"  page {u}: {e-a} chars; head: {body[len(u):len(u)+160]!r}")
    if i==0:
        # find where cookie text ends: look for typical markers
        for kw in ["Cookie Policy","consent","personal data","Complianz","Last updated","WordPress"]:
            print("     ",kw, body.count(kw))
# mentions on page https://alecmfg.com/ with legal-ish snippets
legal=re.compile(r"cookie|consent|personal data|gdpr|privacy|third[- ]part|browser|tracking", re.I)
hits=Counter(); ex=[]
for f in ["conformity_attestations","equipments","industries","material_caps","process_caps","products"]:
    d=json.load(open(f"{NEW}/alecmfg_com__{f}__partial.json"))
    for cb,ch in d["chunks"].items():
        for g in ch["fold"]["groups"]:
            for m in g["mentions"]:
                if m["page"]=="https://alecmfg.com/" and legal.search(m["snippet"]):
                    hits[(f,g["key"])]+=1
                    if len(ex)<8: ex.append((f,g["key"],m["snippet"][:100],m["location"][:120]))
print("  legal-ish mentions on the homepage URL by (field,key):", sum(hits.values()), "groups", len(hits))
for k,c in hits.most_common(12): print("    ",k,c)
for e in ex: print("    EX:",e)
# the search: what did search extract on the first window (0:24296) of alecmfg — forms that only occur in the cookie text
print("\n=== 2b. alecmfg forms whose ALL mentions are on the homepage cookie text (page https://alecmfg.com/ and offset < end of page 0) ===")
end0=urls[1][0]
for f in ["conformity_attestations","equipments","industries","material_caps","process_caps","products"]:
    d=json.load(open(f"{NEW}/alecmfg_com__{f}__partial.json"))
    only=[]
    for cb,ch in d["chunks"].items():
        if not cb.startswith("0:"): continue
        for g in ch["fold"]["groups"]:
            if not g["mentions"]: continue
            if all(m["window"]==0 and m["span"][0] < end0 for m in g["mentions"]):
                only.append((g["key"], g["mention_count"], g["mentions"][0]["snippet"][:60]))
    print(f"  {f}: {len(only)} groups only in page-0 text (first {end0} chars):", only[:12])
