"""Pull every search / recursive-search / mention-collection request for alecmfg + steelcraft out of Mongo
(custom_id, subject, created_at, user message, raw response content, usage, finish_reason) -> all_requests.json"""
import json, os, collections
from pymongo import MongoClient
ROOT="/Users/amaryadav/Documents/ASU_PhD/SUDOKN/data-ETL-pipeline"
uri=[l.split("=",1)[1].strip().strip('"').strip("'") for l in open(f"{ROOT}/.env") if l.strip().startswith("MONGO_DB_URI=")][0]
coll=MongoClient(uri, serverSelectionTimeoutMS=20000).get_default_database()["gpt_batch_requests"]
q={"request.custom_id":{"$regex":">llm_phrase_mention_collection>|>llm_search>chunk>|>llm_recursive_search>round>"},
   "subject_unique_id":{"$in":["alecmfg.com","steelcraft.com"]}}
out=[]
for doc in coll.find(q):
    req=doc.get("request",{}) or {}; body=req.get("body",{}) or {}; msgs=body.get("messages") or []
    um=next((m.get("content") for m in msgs if m.get("role")=="user"), None)
    resp=doc.get("response"); content=None; finish=None; usage=None; err=None
    if isinstance(resp, dict):
        rb=resp.get("chat_completion_result") or resp.get("body") or resp
        try:
            ch=(rb.get("choices") or [])
            if ch: content=ch[0].get("message",{}).get("content"); finish=ch[0].get("finish_reason")
            usage=rb.get("usage")
        except Exception as e: content=f"<<unparsed: {e}>>"
        err=resp.get("error")
    out.append({"custom_id":req.get("custom_id"),"subject":doc.get("subject_unique_id"),"created_at":str(doc.get("created_at")),
                "batch_id":doc.get("batch_id"),"user_message":um,"content":content,"finish_reason":finish,"usage":usage,"error":err})
here=os.path.dirname(os.path.abspath(__file__))
json.dump(out, open(os.path.join(here,"all_requests.json"),"w"), ensure_ascii=False)
def stage(c):
    return "mention" if ">llm_phrase_mention_collection>" in c else "search" if ">llm_search>" in c else "recursive"
print("docs:", len(out))
cnt=collections.Counter((o["created_at"][:13], stage(o["custom_id"] or "")) for o in out)
for k in sorted(cnt): print("  ", k, cnt[k])
print("with content:", sum(1 for o in out if o["content"]), "| errors:", sum(1 for o in out if o["error"]))
