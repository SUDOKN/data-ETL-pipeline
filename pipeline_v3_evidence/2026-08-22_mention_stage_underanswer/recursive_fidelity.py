import re, json, collections, sys
sys.path.insert(0, "/Users/amaryadav/Documents/ASU_PhD/SUDOKN/data-ETL-pipeline/packages/core/src")
from core.utils.floor_scan import find_form_occurrences, mask_page_headers
from pymongo import MongoClient
ROOT="/Users/amaryadav/Documents/ASU_PhD/SUDOKN/data-ETL-pipeline"
uri=[l.split("=",1)[1].strip() for l in open(f"{ROOT}/.env") if l.startswith("MONGO_DB_URI=")][0].strip('"').strip("'")
coll=MongoClient(uri).get_default_database()["gpt_batch_requests"]
subs=["alecmfg.com","steelcraft.com"]
def content(d):
    r=d.get("response"); return r["chat_completion_result"]["choices"][0]["message"]["content"] if r and r.get("chat_completion_result") else None
def window_text(um):
    body=um.split("\n",1)[1] if re.match(r"^[0-9a-f]{32}\n", um) else um
    marker="text scraped from a manufacturer's website:\n"
    if marker in body: body=body.split(marker,1)[1]
    return body.rsplit("<<<",1)[0] if "<<<" in body else body
tot=collections.Counter(); byfield=collections.defaultdict(collections.Counter); ex=collections.defaultdict(list)
for stage,regex in [("search",">llm_search>chunk>"),("recursive",">llm_recursive_search>round>")]:
    for d in coll.find({"subject_unique_id":{"$in":subs},"request.custom_id":{"$regex":regex}}):
        cid=d["request"]["custom_id"]; field=cid.split(">")[1]
        um=d["request"]["body"]["messages"][1]["content"]; text=window_text(um); dom=mask_page_headers(text)
        try: phrases=json.loads(content(d))["phrases"]
        except Exception: tot[(stage,"unparsed")]+=1; continue
        for p in phrases:
            if not isinstance(p,str): continue
            if find_form_occurrences(dom,p,case_sensitive=True): c="exact"
            elif find_form_occurrences(dom,p,case_sensitive=False): c="casing-variant only"
            elif find_form_occurrences(dom,p.strip(),case_sensitive=False): c="whitespace"
            elif p.lower() in dom.lower(): c="substring (not whole-word)"
            else: c="NOT IN WINDOW"
            tot[(stage,c)]+=1; byfield[(stage,field)][c]+=1
            if c=="NOT IN WINDOW" and len(ex[(stage,field)])<3: ex[(stage,field)].append(p)
for stage in ["search","recursive"]:
    n=sum(v for (s,c),v in tot.items() if s==stage); print(f"\n{stage.upper()} phrases returned: {n}")
    for (s,c),v in sorted(tot.items()):
        if s==stage: print(f"   {c:28s} {v:5d} ({v/n:.0%})")
print("\nNOT-IN-WINDOW share by field:")
for (stage,field),cnt in sorted(byfield.items()):
    n=sum(cnt.values()); print(f"   {stage:9s} {field:24s} {cnt['NOT IN WINDOW']:4d}/{n:4d} ({cnt['NOT IN WINDOW']/n:.0%})  e.g. {ex[(stage,field)]}")
