import json, re, collections, hashlib, random
SP="<scratchpad>"
docs=json.load(open(f"{SP}/all_requests_034518.json"))
def stage(c): return "mention" if ">llm_phrase_mention_collection>" in c else "search" if ">llm_search>" in c else "other"
cnt=collections.Counter((d["subject"],stage(d["custom_id"]),"answered" if d["content"] else "unanswered") for d in docs)
for k in sorted(cnt): print(k,cnt[k])
CONT="[this page began before the text shown"
NONCE="request nonce (ignore): "
print("\n== mention requests: header/nonce checks ==")
rows=[]
for d in docs:
    if stage(d["custom_id"])!="mention": continue
    um=d["user_message"] or ""
    first=um.split("\n",1)[0]
    rows.append((d["subject"], d["custom_id"].split(">")[1], re.search(r"sub>([^>]+)>",d["custom_id"]).group(1), CONT in um, first.startswith(NONCE), NONCE in um, um.find(NONCE), len(um), "[page content omitted]" in um, d["usage"]["prompt_tokens"] if d["usage"] else None, d["usage"]["completion_tokens"] if d["usage"] else None, d["finish_reason"]))
print("n mention docs",len(rows))
print("continued header present:",sum(r[3] for r in rows),"| nonce is FIRST line:",sum(r[4] for r in rows),"| nonce anywhere:",sum(r[5] for r in rows), "| page-omitted marker present:",sum(r[8] for r in rows))
print("windows with continued header:",sorted(set((r[0],r[2]) for r in rows if r[3])))
print("windows WITHOUT continued header:",sorted(set((r[0],r[2]) for r in rows if not r[3])))
print("\n== search requests: header checks ==")
srows=[]
for d in docs:
    if stage(d["custom_id"])!="search": continue
    um=d["user_message"] or ""
    srows.append((d["subject"], d["custom_id"].split(">")[1], re.search(r"sub>([^>|]+)",d["custom_id"]).group(1), CONT in um, um.split("\n",1)[0].startswith(NONCE), "[page content omitted]" in um))
print("n search docs",len(srows),"continued:",sum(r[3] for r in srows),"nonce first:",sum(r[4] for r in srows),"omitted marker:",sum(r[5] for r in srows))
print("search windows with continued header:",sorted(set((r[0],r[2]) for r in srows if r[3])))
print("\n== steelcraft products: search answers per window + empty mention windows ==")
for d in docs:
    if d["subject"]!="steelcraft.com": continue
    c=d["custom_id"]
    if stage(c)=="search":
        try: ph=json.loads(d["content"]).get("phrases")
        except Exception as e: ph=f"<unparsed {e}>"
        um=d["user_message"] or ""
        urls=re.findall(r"^https?://\S+$", um, re.M)
        print(" SEARCH",re.search(r"sub>([^>|]+)",c).group(1),"phrases:",len(ph) if isinstance(ph,list) else ph, "| urls in window:",len(urls), "| omitted markers:",um.count("[page content omitted]"), "| sample urls:",urls[:4])
for d in docs:
    if d["subject"]!="steelcraft.com": continue
    c=d["custom_id"]
    if stage(c)=="mention":
        um=d["user_message"] or ""
        print(" MENTION",re.search(r"sub>([^>]+)>group>(\d+)",c).groups(),"answered" if d["content"] else "UNANSWERED","| user msg len",len(um),"| batch_id",d["batch_id"],"| first line:",repr(um[:90]))
print("\n== steelcraft window texts for 63780:90445 and 136253:157472 (search request): URL lines + first 600 chars ==")
for d in docs:
    c=d["custom_id"]
    if d["subject"]=="steelcraft.com" and stage(c)=="search" and ("sub>63780:90445" in c or "sub>136253:157472" in c):
        um=d["user_message"] or ""
        urls=re.findall(r"^https?://\S+$", um, re.M)
        print("---",c.split(">")[5], "len",len(um)); print(" URLS:",urls); print(" HEAD:",repr(um[:700])); print(" TAIL:",repr(um[-400:]))
        try: print(" PHRASES:",json.loads(d["content"]).get("phrases")[:40])
        except Exception as e: print(" content:",repr((d["content"] or "")[:300]))
print("\n== one mention wire sample (alecmfg industries, first window) ==")
for d in docs:
    c=d["custom_id"]
    if d["subject"]=="alecmfg.com" and ">industries>" in c and stage(c)=="mention" and "sub>0:24071>group>0" in c:
        um=d["user_message"]; print(repr(um[:1500])); print("..."); i=um.find("<<<MENTION_IDS"); print(repr(um[i:i+800])); print("SYSTEM (first 1200):"); print((d["system_message"] or "")[:1200]); print("CONTENT head:",repr((d["content"] or "")[:600]))
        break
print("\n== system message digests per field (mention) ==")
sm=collections.defaultdict(set)
for d in docs:
    if stage(d["custom_id"])=="mention": sm[d["custom_id"].split(">")[1]].add(hashlib.md5((d["system_message"] or "").encode()).hexdigest()[:8])
print(dict(sm))
# stand-alone sentence present?
for d in docs:
    if stage(d["custom_id"])=="mention" and d["system_message"]:
        s=d["system_message"]
        for kw in ("stand alone","stand-alone","reference to other","other mention","began before","continued","nonce","URL","url"):
            j=s.lower().find(kw.lower())
            if j>=0: print(f" sys has {kw!r} at {j}: ...{s[max(0,j-120):j+160]!r}")
        break
