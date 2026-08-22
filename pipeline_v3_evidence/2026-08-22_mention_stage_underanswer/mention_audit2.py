import json, re, sys, collections, statistics, glob
sys.path.insert(0, "/Users/amaryadav/Documents/ASU_PhD/SUDOKN/data-ETL-pipeline/packages/core/src")
from core.utils.floor_scan import floor_scan, find_form_occurrences
S="/private/tmp/claude-501/-Users-amaryadav-Documents-ASU-PhD-SUDOKN-data-ETL-pipeline/b0b8a829-bab9-4333-b43c-60e0b05cf84b/scratchpad"
ROOT="/Users/amaryadav/Documents/ASU_PhD/SUDOKN/data-ETL-pipeline"
reqs=[r for r in json.load(open(f"{S}/mention_requests.json")) if r["user_message"] and "<<<PHRASES" in r["user_message"]]
full={s: open(f"{ROOT}/apps/data_etl_app/src/data_etl_app/knowledge/sample_scraped_texts/{s}.txt",encoding="utf-8").read() for s in ["alecmfg.com","steelcraft.com"]}
# --- dump rows: phrase provenance ---
dumps={}
for p in glob.glob(f"{ROOT}/packages/logs/extraction_dumps/20260822T061410/*__partial.json"):
    d=json.load(open(p)); dumps[(d["subject_unique_id"], d["field_type"])]=d
k0=next(iter(dumps)); ch0=next(iter(dumps[k0]["chunks"].values())); row0=ch0["rows"][0] if ch0["rows"] else {}
print("dump row keys:", list(row0.keys())[:12]); 
sp=row0.get("search") or row0.get("search_provenance") or {}
print("row sample:", json.dumps(row0)[:400])
# build phrase -> provenance per (subj, field, chunk)
prov={}
for (subj,field),d in dumps.items():
    for cb,ch in d["chunks"].items():
        for row in ch.get("rows",[]):
            ph=row.get("phrase") or row.get("form") or row.get("record_id")
            prov[(subj,field,cb,ph)]=row
# --- zero-hit classification ---
cls=C=collections.Counter(); ex=collections.defaultdict(list); zero_by_field=collections.Counter(); sent_by_field=collections.Counter()
field_forms=collections.defaultdict(set)
for r in reqs:
    cid=r["custom_id"]; m=re.search(r"^([^>]+)>([^>]+)>llm_phrase_mention_collection>chunk>([^>]+)>sub>([^>]+)>group>(\d+)>",cid); subj,field,chunk,sub,gi=m.groups()
    um=r["user_message"]; a=um.index("<<<PHRASES")+len("<<<PHRASES"); b=um.index("PHRASES>>>")
    sent=json.loads(um[a:b].strip()); text=um.split("text scraped from a manufacturer's website:\n",1)[1].rsplit("<<<PHRASES",1)[0]
    cs,ce=map(int,chunk.split(":")); chunk_text=full[subj][cs:ce]
    scan=floor_scan(text, sent)
    for f in sent:
        sent_by_field[field]+=1; field_forms[field].add(f)
        if scan.tier1[f]: continue
        zero_by_field[field]+=1
        row=prov.get((subj,field,chunk,f))
        src="?"
        if row:
            src=str({k:v for k,v in row.items() if k in ("round","rounds","source","provenance","search_round","sub_bounds","origin")})[:80]
        if scan.tier2[f]: c="A casing/punct variant occurs in window"
        elif find_form_occurrences(chunk_text, f, case_sensitive=False): c="B occurs elsewhere in the CHUNK (not this window)"
        elif find_form_occurrences(full[subj], f, case_sensitive=False): c="C occurs elsewhere in the SUBJECT text"
        else: c="D occurs NOWHERE in the subject text (non-verbatim search output)"
        cls[c]+=1
        if len(ex[c])<4: ex[c].append((field, f, src))
print("\nZERO-HIT FORMS (406) classified:")
for c,n in sorted(cls.items()): print(f"  {n:4d} {c}"); [print("        ", e) for e in ex[c]]
print("zero-hit share by field:", {f: f"{zero_by_field[f]}/{sent_by_field[f]}" for f in sent_by_field})
# --- conformity leak check ---
print("\nCONFORMITY forms sample (alecmfg):", sorted(field_forms["conformity_attestations"])[:40])
d=dumps.get(("alecmfg.com","conformity_attestations"))
if d:
    ch=d["chunks"]["0:98612"]; phrases=[(row.get("phrase") or row.get("form")) for row in ch.get("rows",[])]
    print("conformity dump rows for chunk 0:98612:", len(phrases), "sample:", phrases[:25])
    g=[x["key"] for x in ch["fold"]["groups"]][:25]; print("conformity fold group keys sample:", g)
