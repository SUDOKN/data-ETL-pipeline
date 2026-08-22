"""Per-group analysis of run 20260822T061410 mention-collection requests: answered ratio vs
near-duplicate density; worst groups; finish reasons."""
import json, re, statistics, difflib, itertools, collections, sys
sys.path.insert(0, "/Users/amaryadav/Documents/ASU_PhD/SUDOKN/data-ETL-pipeline/packages/core/src")
from core.utils.floor_scan import floor_scan
from core.utils.form_normalizer import normalize
ROOT="/Users/amaryadav/Documents/ASU_PhD/SUDOKN/data-ETL-pipeline"
texts={s: open(f"{ROOT}/apps/data_etl_app/src/data_etl_app/knowledge/sample_scraped_texts/{s}.txt",encoding="utf-8").read() for s in ["alecmfg.com","steelcraft.com"]}
reqs=json.load(open("/private/tmp/claude-501/-Users-amaryadav-Documents-ASU-PhD-SUDOKN-data-ETL-pipeline/b0b8a829-bab9-4333-b43c-60e0b05cf84b/scratchpad/mention_requests.json"))
rows=[]
fin=collections.Counter()
for r in reqs:
    cid=r["custom_id"]; m=re.search(r"^([^>]+)>([^>]+)>llm_phrase_mention_collection>chunk>([^>]+)>sub>([^>]+)>group>(\d+)>",cid)
    subj,field,chunk,sub,gi=m.groups()
    um=r["user_message"] or ""
    if "<<<PHRASES" not in um: continue
    a=um.index("<<<PHRASES")+len("<<<PHRASES"); b=um.index("PHRASES>>>")
    sent=json.loads(um[a:b].strip())
    s,e=map(int,sub.split(":")); wt=texts[subj][s:e]
    # alignment check: window text must equal the text in the user message
    um_text=um.split("text scraped from a manufacturer's website:\n",1)[1].rsplit("\n\n<<<PHRASES",1)[0]
    aligned = (um_text==wt)
    scan=floor_scan(wt, sent)
    with_hits=[f for f in sent if scan.tier1[f]]
    try:
        resp=json.loads(r["content"]); forms=resp["forms"]
    except Exception as ex:
        forms=[]; 
    answered={f["form"] for f in forms if f.get("mentions")}
    returned={f["form"] for f in forms}
    n_m=sum(len(f.get("mentions") or []) for f in forms)
    # near-dup metrics
    keys=collections.Counter(normalize(f) for f in sent)
    same_key_pairs=sum(c*(c-1)//2 for c in keys.values())
    sub_pairs=sum(1 for x,y in itertools.permutations(sent,2) if x!=y and x in y)
    sim_pairs=sum(1 for x,y in itertools.combinations(sent,2) if difflib.SequenceMatcher(None,x.lower(),y.lower()).ratio()>=0.8)
    fin[str(r["finish_reason"])]+=1
    rows.append(dict(subj=subj,field=field,chunk=chunk,sub=sub,gi=int(gi),n_sent=len(sent),n_hits=len(with_hits),
        n_answered=len(answered), n_answered_with_hits=len(answered & set(with_hits)), n_returned=len(returned), n_m=n_m,
        skipped=[f for f in with_hits if f not in answered],
        out_tok=(r["usage"] or {}).get("completion_tokens"), in_tok=(r["usage"] or {}).get("prompt_tokens"),
        same_key_pairs=same_key_pairs, sub_pairs=sub_pairs, sim_pairs=sim_pairs, aligned=aligned, sent=sent, answered=sorted(answered)))
print("requests", len(rows), "aligned windows:", sum(r["aligned"] for r in rows), "finish_reason values:", dict(fin))
tot_hits=sum(r["n_hits"] for r in rows); tot_ans=sum(r["n_answered_with_hits"] for r in rows)
print(f"forms with >=1 exact hit in their window: {tot_hits}; of those answered with >=1 mention: {tot_ans} ({tot_ans/tot_hits:.0%}); returned-all-forms: {sum(r['n_returned']==r['n_sent'] for r in rows)}/{len(rows)}")
# skip ratio per group
for r in rows: r["skip_ratio"]=(1-r["n_answered_with_hits"]/r["n_hits"]) if r["n_hits"] else None
valid=[r for r in rows if r["skip_ratio"] is not None and r["n_hits"]>=3]
print("groups with >=3 forms-with-hits:", len(valid))
print("skip-ratio distribution:", collections.Counter(("0%" if r["skip_ratio"]==0 else "<20%" if r["skip_ratio"]<.2 else "<50%" if r["skip_ratio"]<.5 else "<80%" if r["skip_ratio"]<.8 else ">=80%") for r in valid))
# correlation skip ratio vs near-dup density (pairs per form)
def corr(xs,ys):
    mx,my=statistics.mean(xs),statistics.mean(ys); sx=statistics.pstdev(xs); sy=statistics.pstdev(ys)
    return sum((x-mx)*(y-my) for x,y in zip(xs,ys))/(len(xs)*sx*sy) if sx and sy else float('nan')
for name in ["same_key_pairs","sub_pairs","sim_pairs","n_sent","n_hits"]:
    xs=[r[name]/max(r["n_sent"],1) if "pairs" in name else r[name] for r in valid]; ys=[r["skip_ratio"] for r in valid]
    print(f"corr(skip_ratio, {name}{'/n_sent' if 'pairs' in name else ''}) = {corr(xs,ys):+.2f}")
# bucket by sim density
print("\nskip ratio by near-dup density (sim_pairs/n_sent):")
for lo,hi in [(0,0.001),(0.001,0.15),(0.15,0.4),(0.4,99)]:
    g=[r for r in valid if lo<=r["sim_pairs"]/r["n_sent"]<hi]
    if g: print(f"  [{lo},{hi}): n={len(g):3d} mean skip={statistics.mean(r['skip_ratio'] for r in g):.0%}  mean out_tok={statistics.mean(r['out_tok'] for r in g):.0f}")
print("\nskip ratio by group size:")
for lo,hi in [(1,10),(10,20),(20,30),(30,31)]:
    g=[r for r in valid if lo<=r["n_sent"]<hi]
    if g: print(f"  size [{lo},{hi}): n={len(g):3d} mean skip={statistics.mean(r['skip_ratio'] for r in g):.0%}")
print("\nWORST groups (skip >= 50%):")
for r in sorted(valid,key=lambda r:-r["skip_ratio"]):
    if r["skip_ratio"]<.5: break
    print(f"  {r['subj']} {r['field']} sub {r['sub']} g{r['gi']}: sent {r['n_sent']}, with-hits {r['n_hits']}, answered {r['n_answered_with_hits']}, mentions {r['n_m']}, out_tok {r['out_tok']}, sim_pairs {r['sim_pairs']}, sub_pairs {r['sub_pairs']}")
json.dump(rows, open("/private/tmp/claude-501/-Users-amaryadav-Documents-ASU-PhD-SUDOKN-data-ETL-pipeline/b0b8a829-bab9-4333-b43c-60e0b05cf84b/scratchpad/group_rows.json","w"), ensure_ascii=False)
