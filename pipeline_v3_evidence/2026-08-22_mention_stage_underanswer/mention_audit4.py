import json, glob, collections
ROOT="/Users/amaryadav/Documents/ASU_PhD/SUDOKN/data-ETL-pipeline"
tot=collections.Counter(); rekey_cat=collections.Counter(); disc_ex=[]; dup_in_window_overlap=0; dup_across_chunks=0; groups_total=0; empty_total=0
overlap_chars_windows=0; overlap_chars_chunks=0; mention_keys_by_field=collections.defaultdict(list)
for p in sorted(glob.glob(f"{ROOT}/packages/logs/extraction_dumps/20260822T061410/*__partial.json")):
    d=json.load(open(p)); subj=d["subject_unique_id"]; field=d["field_type"]
    chunk_bounds=[tuple(map(int,cb.split(":"))) for cb in d["chunks"]]
    for (a,b),(c,e) in zip(chunk_bounds, chunk_bounds[1:]): overlap_chars_chunks+=max(0,b-c)
    seen_abs=collections.defaultdict(set)  # (group key) -> abs starts, across chunks
    for cb,ch in d["chunks"].items():
        fold=ch.get("fold"); 
        if not fold: continue
        s=fold["summary"]; 
        for k in ["groups","empty_groups","mentions","candidates","obligations","unaccounted","unlocated","unanchored","rekeyed","discovered_casings"]: tot[k]+=s.get(k,0)
        wins=fold["windows"]; wb=[tuple(map(int,w["sub_bounds"].split(":"))) for w in wins]
        for (a,b),(c,e) in zip(wb, wb[1:]): overlap_chars_windows+=max(0,b-c)
        for w in wins:
            for rk in (w.get("rekeyed_reports") or w.get("rekeys") or []):
                rekey_cat[(rk.get("reason") or rk.get("kind") or "?")]+=1
            dc=w.get("discovered_casings") or {}
            if isinstance(dc, dict):
                for form,cas in list(dc.items())[:2]:
                    if len(disc_ex)<8: disc_ex.append((field, form, cas[:3] if isinstance(cas,list) else cas))
        for g in fold["groups"]:
            groups_total+=1
            if not g["mentions"]: empty_total+=1
            starts_this_chunk=collections.Counter()
            for mnt in g["mentions"]:
                wi=mnt.get("window_index"); ws=wb[wi][0] if wi is not None and wi<len(wb) else None
                st=mnt.get("start"); absst=(ws+st) if (ws is not None and st is not None) else None
                if absst is None: continue
                key=(g["key"], absst)
                starts_this_chunk[key]+=1
                if absst in seen_abs[g["key"]]: dup_across_chunks+=1
                seen_abs[g["key"]].add(absst)
            dup_in_window_overlap+=sum(c-1 for c in starts_this_chunk.values() if c>1)
print("fold totals over 14 dumps:", dict(tot))
print(f"groups {groups_total}, empty {empty_total} ({empty_total/groups_total:.0%})")
print(f"sub-window overlap chars (within chunks) {overlap_chars_windows:,}; chunk overlap chars {overlap_chars_chunks:,}")
print(f"DUPLICATE occurrences: same group+absolute start twice within one chunk fold (window overlaps) {dup_in_window_overlap}; same across the two chunk folds (chunk overlap) {dup_across_chunks}")
print("rekey categories:", dict(rekey_cat) or "(no per-report reasons in dump windows)")
print("discovered casings examples:", disc_ex)
# what keys do window dicts carry?
w0=next(iter(json.load(open(glob.glob(f"{ROOT}/packages/logs/extraction_dumps/20260822T061410/*__partial.json")[0]))["chunks"].values()))["fold"]["windows"][0]
print("window keys:", list(w0.keys())); m0=next(iter(json.load(open(glob.glob(f"{ROOT}/packages/logs/extraction_dumps/20260822T061410/*__partial.json")[0]))["chunks"].values()))["fold"]["groups"][0]["mentions"][:1]; print("mention keys:", list(m0[0].keys()) if m0 else None)
