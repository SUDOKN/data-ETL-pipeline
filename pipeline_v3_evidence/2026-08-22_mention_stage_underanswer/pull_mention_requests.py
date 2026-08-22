"""Pull every mention-collection request of run 20260822T061410 from Mongo into a local JSON
(custom_id, subject, created_at, the user message, the raw response content, usage/finish_reason)."""
import json, os, re, sys
from pymongo import MongoClient
uri = None
for line in open("/Users/amaryadav/Documents/ASU_PhD/SUDOKN/data-ETL-pipeline/.env"):
    line=line.strip()
    if line.startswith("MONGO_DB_URI="):
        uri=line.split("=",1)[1].strip().strip('"').strip("'")
assert uri
client = MongoClient(uri, serverSelectionTimeoutMS=20000)
db = client.get_default_database()
coll = db["gpt_batch_requests"]
q = {"request.custom_id": {"$regex": ">llm_phrase_mention_collection>"}, "subject_unique_id": {"$in": ["alecmfg.com","steelcraft.com"]}}
out=[]
for doc in coll.find(q):
    req = doc.get("request", {})
    body = req.get("body", {}) or {}
    msgs = body.get("messages") or []
    user_msg = next((m.get("content") for m in msgs if m.get("role")=="user"), None)
    resp = doc.get("response")
    content=None; finish=None; usage=None
    if resp:
        try:
            rb = resp.get("chat_completion_result") or resp.get("body") or resp
            choices = rb.get("choices") or []
            if choices:
                content = choices[0].get("message",{}).get("content")
                finish = choices[0].get("finish_reason")
            usage = rb.get("usage")
        except Exception as e:
            content=f"<<unparsed: {e}>>"
    out.append({"custom_id": req.get("custom_id"), "subject": doc.get("subject_unique_id"),
                "created_at": str(doc.get("created_at")), "batch_id": doc.get("batch_id"),
                "user_message": user_msg, "content": content, "finish_reason": finish, "usage": usage,
                "error": (resp or {}).get("error") if isinstance(resp, dict) else None})
print("docs:", len(out))
import collections
print(collections.Counter(o["created_at"][:10] for o in out))
json.dump(out, open(os.path.join(os.path.dirname(__file__), "mention_requests.json"),"w"), ensure_ascii=False)
print("with content:", sum(1 for o in out if o["content"]), "| response_keys sample:", next((o["response_keys"] for o in out if o["response_keys"]), None))
