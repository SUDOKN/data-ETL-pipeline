import re, json, collections
from pymongo import MongoClient
ROOT="/Users/amaryadav/Documents/ASU_PhD/SUDOKN/data-ETL-pipeline"
S="/private/tmp/claude-501/-Users-amaryadav-Documents-ASU-PhD-SUDOKN-data-ETL-pipeline/b0b8a829-bab9-4333-b43c-60e0b05cf84b/scratchpad"
uri=[l.split("=",1)[1].strip() for l in open(f"{ROOT}/.env") if l.startswith("MONGO_DB_URI=")][0].strip('"').strip("'")
coll=MongoClient(uri).get_default_database()["gpt_batch_requests"]
subs=["alecmfg.com","steelcraft.com"]
def content(d):
    r=d.get("response"); 
    return r["chat_completion_result"]["choices"][0]["message"]["content"] if r and r.get("chat_completion_result") else None
def usage(d):
    r=d.get("response"); u=(r or {}).get("chat_completion_result",{}).get("usage",{}) or {}
    return u.get("prompt_tokens",0), u.get("completion_tokens",0)
search={}; rec=collections.defaultdict(dict)
for d in coll.find({"subject_unique_id":{"$in":subs},"request.custom_id":{"$regex":">llm_search>chunk>"}}):
    cid=d["request"]["custom_id"]; m=re.search(r"^([^>]+)>([^>]+)>llm_search>chunk>([^>]+)>sub>([^>]+)>",cid); k=(m.group(1),m.group(2),m.group(4))
    try: search[k]=len(json.loads(content(d))["phrases"])
    except Exception: search[k]=None
for d in coll.find({"subject_unique_id":{"$in":subs},"request.custom_id":{"$regex":">llm_recursive_search>round>"}}):
    cid=d["request"]["custom_id"]; m=re.search(r"^([^>]+)>([^>]+)>llm_recursive_search>round>(\d+)>chunk>([^>]+)>sub>([^>]+)>",cid); k=(m.group(1),m.group(2),m.group(5)); rnd=int(m.group(3))
    um=d["request"]["body"]["messages"][1]["content"]; mm=re.search(r"<<<ALREADY_EXTRACTED_PHRASES(.*?)ALREADY_EXTRACTED_PHRASES>>>", um, re.S)
    try: listed=len(json.loads(mm.group(1).strip()))
    except Exception: listed=None
    try: ret=len(json.loads(content(d))["phrases"])
    except Exception: ret=None
    p,c=usage(d); rec[k][rnd]=dict(listed=listed, returned=ret, ptok=p, ctok=c)
print("search windows:", len(search), "| recursive windows:", len(rec))
empty=[(k,v[0]) for k,v in rec.items() if 0 in v and v[0]["listed"]==0]
nonempty=[(k,v[0]) for k,v in rec.items() if 0 in v and v[0]["listed"]]
print(f"round-0 with EMPTY listed: {len(empty)} -> returned phrases total {sum(v['returned'] or 0 for _,v in empty)}, tokens {sum(v['ptok'] for _,v in empty):,} in / {sum(v['ctok'] for _,v in empty):,} out")
print(f"round-0 with listed>0   : {len(nonempty)} -> listed {sum(v['listed'] for _,v in nonempty)}, returned {sum(v['returned'] or 0 for _,v in nonempty)}")
print("by field (empty round-0 windows / returned phrases):", {f: (sum(1 for (s,ff,w),v in empty if ff==f), sum(v['returned'] or 0 for (s,ff,w),v in empty if ff==f)) for f in sorted({k[1] for k in rec})})
# later rounds?
later=collections.Counter(); 
for k,v in rec.items():
    for rnd,x in v.items():
        if rnd>0: later[(x["listed"]==0)]+=1
print("rounds>0 (listed empty?):", dict(later))
# mention-stage cost for windows whose search returned []
mreqs=[r for r in json.load(open(f"{S}/mention_requests.json")) if r["user_message"] and "<<<PHRASES" in r["user_message"]]
cost=collections.Counter(); forms=0; groups=0
for r in mreqs:
    m=re.search(r"^([^>]+)>([^>]+)>llm_phrase_mention_collection>chunk>([^>]+)>sub>([^>]+)>group>(\d+)>",r["custom_id"]); k=(m.group(1),m.group(2),m.group(4))
    if search.get(k)==0:
        u=r["usage"] or {}; cost["ptok"]+=u.get("prompt_tokens",0); cost["ctok"]+=u.get("completion_tokens",0); groups+=1
        um=r["user_message"]; a=um.index("<<<PHRASES")+len("<<<PHRASES"); b=um.index("PHRASES>>>"); forms+=len(json.loads(um[a:b].strip()))
tot_p=sum((r["usage"] or {}).get("prompt_tokens",0) for r in mreqs); tot_c=sum((r["usage"] or {}).get("completion_tokens",0) for r in mreqs)
print(f"\nMENTION stage spent on windows whose SEARCH found nothing: {groups} requests, {forms} forms, {cost['ptok']:,} prompt ({cost['ptok']/tot_p:.0%}) / {cost['ctok']:,} completion ({cost['ctok']/tot_c:.0%}) tokens")
print("examples of empty-listed round-0 returns (top):", sorted([(k[1],k[0][:9],k[2],v["returned"]) for k,v in empty], key=lambda x:-(x[3] or 0))[:8])
