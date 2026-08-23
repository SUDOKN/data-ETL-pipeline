import os
import json, re, statistics as st
from collections import Counter, defaultdict
SP=os.path.dirname(os.path.abspath(__file__))  # expects all_requests_new.json beside this file (regenerate with pull_new.py; NOT committed — it holds the site text)
docs=json.load(open(f"{SP}/all_requests_new.json"))
ment=[d for d in docs if ">llm_phrase_mention_collection>" in d["custom_id"]]
srch=[d for d in docs if ">llm_search>" in d["custom_id"]]
print("mention docs",len(ment),"search docs",len(srch))
# --- wire shape of one request
d0=[d for d in ment if "steelcraft.com>material_caps>" in d["custom_id"] and ">sub>0:23757>" in d["custom_id"]][0]
um=d0["user_message"]; sm=d0["system_message"]
print("\n=== system message head (Location static?) ===\n", (sm or "")[:300].replace("\n","⏎"))
print("\n=== user message: length", len(um), "chars; head ===\n", um[:600].replace("\n","⏎"))
i=um.rfind("<<<MENTION_IDS"); print("\n=== user message tail from <<<MENTION_IDS (first 1500 chars) ===\n", um[i:i+1500])
print("\n=== response head ===\n", (d0["content"] or "")[:700])
print("marker count in all user messages:", sum((d["user_message"] or "").count("[page content omitted]") for d in ment))
# --- hold audit: sent ids vs returned ids
def sent_ids(um):
    i=um.rfind("<<<MENTION_IDS"); j=um.rfind("MENTION_IDS>>>")
    block=um[i:j]
    return re.findall(r'"?(m[0-9a-z]{7})"?', block)
tot_sent=tot_ret=unknown=missing=dups=0; bad=[]; empty_resp=0; per_item_out=[]; usage_tot=Counter()
for d in ment:
    um=d["user_message"]; s=sent_ids(um)
    try: obj=json.loads(d["content"]); ret=[m.get("mention_id") for m in obj.get("mentions",[])]
    except Exception as e: ret=[]; bad.append((d["custom_id"],str(e)[:80],(d["content"] or "")[:200]))
    ss=set(s); rs=Counter(ret)
    tot_sent+=len(s); tot_ret+=len(ret)
    unknown+=sum(1 for r in rs if r not in ss); missing+=sum(1 for x in ss if x not in rs); dups+=sum(c-1 for c in rs.values() if c>1)
    if not ret: empty_resp+=1; print("EMPTY/short response:", d["custom_id"].split(">")[1:3], d["custom_id"].split(">")[5:7], "usage", d["usage"], "content:", (d["content"] or "")[:300])
    u=d["usage"] or {}; usage_tot.update({k:v for k,v in u.items() if isinstance(v,int)})
    if ret and u.get("completion_tokens"): per_item_out.append(u["completion_tokens"]/len(ret))
    if missing and len(ss)-len([x for x in ss if x in rs]) and len(s)!=len(ret) and len(ret)>0:
        pass
print(f"\nHOLD AUDIT: sent ids {tot_sent}, returned {tot_ret}, unknown {unknown}, missing {missing}, duplicate {dups}, unparseable {len(bad)}, empty responses {empty_resp}")
for b in bad: print("  BAD:",b)
# missing details
for d in ment:
    s=set(sent_ids(d["user_message"]))
    try: ret={m.get("mention_id") for m in json.loads(d["content"]).get("mentions",[])}
    except: continue
    miss=s-ret
    if miss and ret:
        print("  partial miss:", d["custom_id"].split(">")[1], d["custom_id"].split(">")[5:7], "sent",len(s),"ret",len(ret),"missing",list(miss)[:5])
print("usage totals:", dict(usage_tot))
print(f"completion tokens per returned item: median {st.median(per_item_out):.1f}, p90 {sorted(per_item_out)[int(.9*len(per_item_out))]:.1f}")
# locations mentioning the omitted marker
n=sum(1 for d in ment if d["content"] and "omitted" in d["content"]); print("responses mentioning 'omitted':", n)
# search: sample of output forms for the privacy windows? just count forms per search doc + the 12 zero-hit
print("\n=== search docs: output phrase counts ===")
cnts=[]
for d in srch:
    try: o=json.loads(d["content"]); 
    except: print("  unparsable search", d["custom_id"]); continue
    # find list
    ph=None
    for k,v in o.items():
        if isinstance(v,list): ph=v; break
    cnts.append(len(ph) if ph is not None else -1)
print("phrases per search request: ", sorted(cnts))
print("search user message head:", (srch[0]["user_message"] or "")[:200].replace("\n","⏎"))
