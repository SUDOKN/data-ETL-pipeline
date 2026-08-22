"""Replay the two worst mention-collection groups of run 20260822T061410 with controlled variants.
Deterministic: temperature 0, seed 12345, gpt-4.1, identical response_format. Through the LiteLLM proxy."""
import asyncio, json, re, sys, copy, collections
from pymongo import MongoClient
from openai import AsyncOpenAI
sys.path.insert(0, "/Users/amaryadav/Documents/ASU_PhD/SUDOKN/data-ETL-pipeline/packages/core/src")
from core.utils.form_normalizer import normalize
from core.utils.floor_scan import floor_scan
ROOT="/Users/amaryadav/Documents/ASU_PhD/SUDOKN/data-ETL-pipeline"
env={}
for l in open(f"{ROOT}/.env"):
    l=l.strip()
    if l and not l.startswith("#") and "=" in l:
        k,v=l.split("=",1); env[k]=v.strip().strip('"').strip("'")
db=MongoClient(env["MONGO_DB_URI"]).get_default_database()["gpt_batch_requests"]
client=AsyncOpenAI(base_url=env["LITELLM_PROXY_URL"], api_key=env["LITELLM_VIRTUAL_KEY"])

def load(prefix):
    doc=db.find_one({"request.custom_id": {"$regex": "^"+re.escape(prefix)}})
    body=doc["request"]["body"]; body={k:v for k,v in body.items() if v is not None}
    um=body["messages"][1]["content"]
    a=um.index("<<<PHRASES")+len("<<<PHRASES"); b=um.index("PHRASES>>>")
    forms=json.loads(um[a:b].strip()); pre=um[:a]; post=um[b:]
    text=um.split("text scraped from a manufacturer's website:\n",1)[1].rsplit("<<<PHRASES",1)[0]
    return body, forms, pre, post, text

def with_forms(body, pre, post, forms):
    b=copy.deepcopy(body); b["messages"][1]["content"]=pre+"\n"+json.dumps(forms, ensure_ascii=False)+"\n"+post; return b

async def run(name, body, forms, text):
    r=await client.chat.completions.create(**body)
    ch=r.choices[0]; content=ch.message.content or ""
    try: parsed=json.loads(content)["forms"]
    except Exception as e: parsed=[]; print("   PARSE FAIL", e, content[-120:])
    scan=floor_scan(text, forms); with_hits=[f for f in forms if scan.tier1[f]]
    answered={f["form"] for f in parsed if f.get("mentions")}
    print(f"{name:42s} sent {len(forms):2d} with-hits {len(with_hits):2d} returned {len(parsed):2d} answered(with hits) {len(answered & set(with_hits)):2d} mentions {sum(len(f['mentions']) for f in parsed):3d} out_tok {r.usage.completion_tokens:5d} finish={ch.finish_reason}")
    return parsed

async def main():
    # --- steelcraft process_caps 2/30 ---
    body, forms, pre, post, text = load("steelcraft.com>process_caps>llm_phrase_mention_collection>chunk>81331:178148>sub>102672:126084>group>0>")
    print("STEELCRAFT process_caps window 102672:126084 group 0")
    p=await run("A exact replay", body, forms, text)
    answered_A={f["form"] for f in p if f.get("mentions")}
    await run("B forms reversed", with_forms(body,pre,post,list(reversed(forms))), list(reversed(forms)), text)
    seen=set(); dedup=[]
    for f in forms:
        k=normalize(f, verb_fold=True)
        if k in seen: continue
        seen.add(k); dedup.append(f)
    await run(f"C one form per normalize key ({len(dedup)})", with_forms(body,pre,post,dedup), dedup, text)
    skipped=[f for f in forms if f not in answered_A]
    await run(f"D only the forms A skipped ({len(skipped)})", with_forms(body,pre,post,skipped), skipped, text)
    h=len(forms)//2
    await run("E1 first half", with_forms(body,pre,post,forms[:h]), forms[:h], text)
    await run("E2 second half", with_forms(body,pre,post,forms[h:]), forms[h:], text)
    # --- alecmfg material_caps 3/21 ---
    body, forms, pre, post, text = load("alecmfg.com>material_caps>llm_phrase_mention_collection>chunk>0:98612>sub>82700:98612>group>0>")
    print("\nALECMFG material_caps window 82700:98612 group 0")
    await run("A exact replay", body, forms, text)
    seen=set(); dedup=[]
    for f in forms:
        k=normalize(f, verb_fold=True)
        if k in seen: continue
        seen.add(k); dedup.append(f)
    await run(f"C one form per normalize key ({len(dedup)})", with_forms(body,pre,post,dedup), dedup, text)
    caps=[f for f in forms if f[:1].isupper()]; lows=[f for f in forms if not f[:1].isupper()]
    await run(f"F capitalized forms only ({len(caps)})", with_forms(body,pre,post,caps), caps, text)
asyncio.run(main())
