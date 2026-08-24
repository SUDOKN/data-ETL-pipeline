"""Deep supplement on run 20260824T020729 — a second, independent pass.

Re-verifies the committed A/B's structural claims from the dumps, then measures
what the first pass did not: mention-stage delivery, own-name identification
stability across the three post-ban runs, the document-listing rule under the
new locations, a census of confusable "twin" focal forms sharing a request, an
identical-synthesis (copy-paste) collapse census across the last four runs,
party survival over the FULL bounds-keyed pairing, and corrected paired
synthesis lengths.

Corrections this pass makes to the first pass's record:
  * mention stage: 93 shared request ids (not 37), 0 with a changed ud=.
  * pairing by (subject, field, group_id) drops 114 records — 242 group_ids
    appear in BOTH chunks of a field; pair with the chunk bounds in the key.
  * on the full 1,360-pair set, 7 records DID lose a party name (all
    steelcraft/conformity_attestations, all the Falcon page-enumeration class);
    survival still improved 47% -> 66%.

Section 10 (added after the first supplement pass) sizes the collapse signature
and kills the "separate near-identical focal forms in the packer" fix as stated:
every candidate PRE-emptive filter is either ~1-3% precise or ~7% recalling.
The exact POST-hoc tripwire (identical synthesis strings within one request) is
the only cheap instrument, and the harmful subclass is already covered by the
existing focal-form lint.

Run from the repo root:
    python3 pipeline_v3_evidence/2026-08-24_run_020729_heading_verbatim/deep_supplement.py
"""

from __future__ import annotations

import glob
import json
import os
import re
import statistics
import sys
from collections import Counter, defaultdict

sys.path.insert(0, "packages/core/src")
from core.utils.focal_form_lint import focal_form_absent  # noqa: E402

DUMPS = "packages/logs/extraction_dumps"
NEW = "20260824T020729"   # tenth run (heading verbatim)
OLD = "20260824T012721"   # ninth run (Location rewrite; 17 dumps)
DOCRUN = "20260824T010654"  # eighth run (document rule; last full 20-dump run)
BASE = "20260824T002404"  # seventh run (published synthesis)
RUNS = [BASE, DOCRUN, OLD, NEW]
SHARED_DUPLICATE = "contract_products"


def dumps_of(run, include_shared_duplicate=False):
    for path in sorted(glob.glob(f"{DUMPS}/{run}/*.json")):
        base = os.path.basename(path)
        subject, field = base.split("__")[0], base.split("__")[1]
        if field == SHARED_DUPLICATE and not include_shared_duplicate:
            continue
        yield subject, field, json.load(open(path))


def stage_requests(chunk, stage):
    block = chunk.get("requests", {}).get(stage)
    if isinstance(block, dict):
        for reqs in block.values():
            yield from reqs
    elif block:
        yield from block


def synth_rows(run):
    """Keyed WITH chunk bounds — group_ids repeat across chunks (242 do)."""
    out = {}
    for subject, field, doc in dumps_of(run):
        for bounds, chunk in doc.get("chunks", {}).items():
            block = chunk.get("synthesis")
            if not block:
                continue
            fold = {g["group_id"]: g for g in chunk["fold"]["groups"]}
            for row in block["records"]:
                out[(subject, field, bounds, row["group_id"])] = (row, fold.get(row["group_id"]))
    return out


def packed_requests(recs):
    """Reconstruct pack_records: sequential, soft cap 50 entries, never split."""
    reqs, cur, cnt = [], [], 0
    for r in recs:
        size = r["entries"]
        if cur and cnt + size > 50:
            reqs.append(cur)
            cur, cnt = [], 0
        cur.append(r)
        cnt += size
    if cur:
        reqs.append(cur)
    return reqs


def section(t):
    print(f"\n{'=' * 78}\n{t}\n{'=' * 78}")


R = {run: synth_rows(run) for run in RUNS}
R_OLD, R_NEW = R[OLD], R[NEW]
shared = [k for k in R_NEW if k in R_OLD]

# --------------------------------------------------------------------------- 1
section("1. Structural claims re-verified from the dumps")
bad = []
for s, f, d in dumps_of(NEW):
    st = d["run"]["time_span"]["by_stage"]
    ss = st.get("llm_phrase_search", {}).get("started_at", "")
    if ss and not ss.startswith("2026-08-23T04:45:00"):
        bad.append((s, f, ss))
print(f"search stages not replaying from 2026-08-23T04:45:00 -> {bad or 'none'}")

def req_ids(run, stage):
    out = {}
    for s, f, d in dumps_of(run, include_shared_duplicate=True):
        for bounds, chunk in d.get("chunks", {}).items():
            for r in stage_requests(chunk, stage):
                cid = r["custom_id"]
                key = re.sub(r"pv=[^|]+", "pv=*", cid)
                ud = re.search(r"ud=([0-9a-f]+)", cid)
                out[key] = ud.group(1) if ud else None
    return out

a, b = req_ids(OLD, "llm_phrase_mention_collection"), req_ids(NEW, "llm_phrase_mention_collection")
both = [k for k in a if k in b]
print(f"mention collection: {len(a)} -> {len(b)} request ids; shared {len(both)}, "
      f"with a changed ud= {sum(1 for k in both if a[k] != b[k])} "
      f"(the first pass's '37' should read '{len(both)}')")
search_ids = req_ids(NEW, "llm_phrase_search")
print(f"llm_search requests in the dumps: {len(search_ids)} (claim: 96)")

tot = Counter()
for s, f, d in dumps_of(NEW):
    for bounds, chunk in d.get("chunks", {}).items():
        blk = chunk.get("synthesis")
        if blk:
            for k, v in blk["summary"].items():
                tot[k] += v if isinstance(v, int) else len(v)
print(f"synthesis: records={tot['records']} synthesized={tot['synthesized']} "
      f"not_synth={tot['not_synthesized']} retried={tot['retried']} "
      f"unknown={tot['unknown_answer_ids']} focal_form_absent={tot['focal_form_absent_records']}")

# --------------------------------------------------------------------------- 2
section("2. Mention-stage delivery (the first pass never checked this run's)")
for run in (OLD, NEW):
    t = Counter()
    for s, f, d in dumps_of(run):
        for bounds, chunk in d.get("chunks", {}).items():
            fs = (chunk.get("fold") or {}).get("summary")
            if fs:
                for k, v in fs.items():
                    t[k] += v if isinstance(v, int) else len(v)
    print(f"{run}: mentions={t['mentions']} snippets={t['distinct_snippets']} "
          f"described={t['described']} not_described={t['not_described']} retried={t['retried']} "
          f"unknown={t['unknown_answer_ids']} zero_hit_forms={t['zero_hit_forms']} "
          f"groups={t['groups']} empty={t['empty_groups']}")

# --------------------------------------------------------------------------- 3
section("3. Latency and turnaround (fresh stages of 020729)")
for stage in ("llm_phrase_mention_collection", "llm_phrase_synthesis"):
    lat, turn, seen = [], [], set()
    for s, f, d in dumps_of(NEW, include_shared_duplicate=True):
        for bounds, chunk in d.get("chunks", {}).items():
            for r in stage_requests(chunk, stage):
                if r["custom_id"] in seen:
                    continue
                seen.add(r["custom_id"])
                if r.get("client_latency_ms"):
                    lat.append(r["client_latency_ms"])
                if r.get("turnaround_seconds") is not None:
                    turn.append(r["turnaround_seconds"])
    lat.sort()
    turn.sort()

    def q(xs, p):
        return xs[min(len(xs) - 1, int(p * len(xs)))]

    print(f"{stage}: n={len(lat)} client_latency p50={q(lat, .5)/1000:.1f}s "
          f"p90={q(lat, .9)/1000:.1f}s max={max(lat)/1000:.1f}s | "
          f"turnaround p50={q(turn, .5)}s p90={q(turn, .9)}s")

# --------------------------------------------------------------------------- 4
section("4. Own-name identification across the three post-ban runs")
for run in (BASE, DOCRUN, NEW):
    per, recs, withhit = Counter(), Counter(), Counter()
    for (s, f, bounds, g), (row, grp) in R[run].items():
        per[s] += row.get("own_name_hits_in_synthesis") or 0
        recs[s] += 1
        if row.get("own_name_hits_in_synthesis"):
            withhit[s] += 1
    print(f"{run}: " + "  ".join(
        f"{s}: hits={per[s]} on {withhit[s]}/{recs[s]} records" for s in sorted(per)))

# --------------------------------------------------------------------------- 5
section("5. Document-listing rule under the new locations")
DOCUMENT = re.compile(r"downloadable|literature|brochure|data sheet|sell sheet|stock sheet|catalog|downloads", re.I)
SCOPED = re.compile(r"offers? a document|document titled|a document bearing|no dealing with it beyond"
                    r"|beyond a document|offering a document|only a document", re.I)
UNSCOPED = re.compile(r"provides? this (resource|data sheet|brochure|portfolio|nomenclature)"
                      r"|provides? (a|the) (data sheet|brochure|resource)", re.I)

def evidence_text(pair):
    row, grp = pair
    return " ".join(m.get("snippet") or "" for m in (grp or {}).get("mentions", []))

common = [k for k in R_NEW if k in R[DOCRUN]]
doc_keys = [k for k in common if DOCUMENT.search(evidence_text(R[DOCRUN][k]))]
print(f"records whose snippet evidence is a document listing (paired with {DOCRUN}): {len(doc_keys)}")
for run in (DOCRUN, NEW):
    t = R[run]
    sc = sum(1 for k in doc_keys if SCOPED.search(t[k][0]["synthesis"] or ""))
    un = sum(1 for k in doc_keys if UNSCOPED.search(t[k][0]["synthesis"] or ""))
    print(f"  {run}: scoped {sc} ({sc / len(doc_keys):.0%})  unscoped {un}  "
          f"neither {len(doc_keys) - sc - un}")

# --------------------------------------------------------------------------- 6
section("6. Twin census — confusable focal forms sharing one request (020729)")

def editdist(x, y, cap=3):
    if abs(len(x) - len(y)) > cap:
        return cap + 1
    prev = list(range(len(y) + 1))
    for i, cx in enumerate(x, 1):
        cur = [i]
        for j, cy in enumerate(y, 1):
            cur.append(min(prev[j] + 1, cur[-1] + 1, prev[j - 1] + (cx != cy)))
        prev = cur
    return prev[-1]

def confusable(x, y):
    """Multi-token names differing in ONE confusable token, or editdist<=2 overall."""
    lx, ly = x.lower(), y.lower()
    if lx == ly:
        return False
    if editdist(lx, ly, 2) <= 2:
        return True
    tx, ty = lx.split(), ly.split()
    if len(tx) == len(ty) >= 2:
        diff = [(u, v) for u, v in zip(tx, ty) if u != v]
        return len(diff) == 1 and editdist(diff[0][0], diff[0][1], 2) <= 2
    return False

per_chunk = defaultdict(list)
for (s, f, bounds, g), (row, grp) in R_NEW.items():
    per_chunk[(s, f, bounds)].append(row)
twins, by_field = [], Counter()
for (s, f, bounds), recs in per_chunk.items():
    for req in packed_requests(recs):
        for i in range(len(req)):
            for j in range(i + 1, len(req)):
                if confusable(req[i]["focal_form"], req[j]["focal_form"]):
                    twins.append((s, f, req[i]["focal_form"], req[j]["focal_form"]))
                    by_field[(s, f)] += 1
print(f"confusable same-request pairs: {len(twins)}   by field: {dict(by_field)}")
print("the population the FE->DE fix has to protect; 1 invention among them this run")

# --------------------------------------------------------------------------- 7
section("7. Identical-synthesis (copy-paste) collapse census, four runs")
for run in RUNS:
    pc = defaultdict(list)
    for (s, f, bounds, g), (row, grp) in R[run].items():
        pc[(s, f, bounds)].append(row)
    pairs, fields = 0, Counter()
    for (s, f, bounds), recs in pc.items():
        for req in packed_requests(recs):
            for i in range(len(req)):
                for j in range(i + 1, len(req)):
                    if (req[i]["synthesis"] or "") and req[i]["synthesis"] == req[j]["synthesis"]:
                        pairs += 1
                        fields[(s, f)] += 1
    print(f"{run}: {pairs} pairs  {dict(fields)}")
print("Standing behavior, not the heading fix: 12 and 23 in the two full runs before it;\n"
      "012721's 1 is an artifact of its missing steelcraft/products dump. The FE->DE pair\n"
      "is the only one whose shared string is WRONG for a record; the rest are composite\n"
      "sentences naming both entities, reused verbatim across co-packed records.")

# --------------------------------------------------------------------------- 8
section("8. Party survival on the FULL bounds-keyed pairing")
PARTY = re.compile(r"\b(Allegion|Falcon|LCN|Von Duprin|Schlage)\b")

def parties_named(group):
    out = set()
    for m in (group or {}).get("mentions", []):
        out |= set(PARTY.findall(m.get("location") or ""))
    return out

gid_count = Counter((s, f, g) for (s, f, bounds, g) in R_NEW)
print(f"paired records: {len(shared)} (first pass: 1,246 — its (subject,field,group_id) key "
      f"collides for {sum(1 for v in gid_count.values() if v > 1)} group_ids present in both chunks)")
for run, table in ((OLD, R_OLD), (NEW, R_NEW)):
    named = kept = 0
    for k in shared:
        row, grp = table[k]
        p = parties_named(grp)
        if not p:
            continue
        named += 1
        kept += any(x in (row["synthesis"] or "") for x in p)
    print(f"  {run}: {named} records name a party in a location; {kept} carry it into "
          f"the synthesis ({kept / named:.0%})")
lost = [k for k in shared if parties_named(R_OLD[k][1]) and not parties_named(R_NEW[k][1])]
print(f"records whose location named a party in {OLD} but not in {NEW}: {len(lost)}")
for k in lost:
    row_o, grp_o = R_OLD[k]
    parties = set()
    for m in grp_o.get("mentions", []):
        parties |= set(PARTY.findall(m.get("location") or ""))
    print(f"    {k[0]}/{k[1]} {k[3]} focal={row_o['focal_form']!r} lost={sorted(parties)}")
print("All are the Falcon page-enumeration class: the old location listed every product\n"
      "page carrying the bullet ('... and SZ Series Falcon Flush Doors'); the new style\n"
      "names fewer pages, so the sub-brand word rode out of the list. The Allegion class\n"
      "the fix targeted stays fixed.")

# --------------------------------------------------------------------------- 9
section("9. Paired synthesis text, corrected (same records both runs)")
lo = [len(R_OLD[k][0]["synthesis"] or "") for k in shared]
ln = [len(R_NEW[k][0]["synthesis"] or "") for k in shared]
print(f"paired={len(shared)}  {OLD}: mean={statistics.mean(lo):.0f} median={statistics.median(lo):.0f} "
      f"total={sum(lo):,}  {NEW}: mean={statistics.mean(ln):.0f} median={statistics.median(ln):.0f} "
      f"total={sum(ln):,}  ({(sum(ln) - sum(lo)) / sum(lo):+.1%})")
for run, table in ((OLD, R_OLD), (NEW, R_NEW)):
    thin = sum(1 for row, _ in table.values() if row["entries"] == 1)
    print(f"{run}: single-entry records {thin}/{len(table)} ({thin / len(table):.0%})")


# -------------------------------------------------------------------------- 10
section("10. Sizing the collapse — is any PRE-emptive filter affordable?")


def edit_distance(x, y, cap=3):
    if abs(len(x) - len(y)) > cap:
        return cap + 1
    prev = list(range(len(y) + 1))
    for i, cx in enumerate(x, 1):
        cur = [i]
        for j, cy in enumerate(y, 1):
            cur.append(min(prev[j] + 1, cur[-1] + 1, prev[j - 1] + (cx != cy)))
        prev = cur
    return prev[-1]


def confusable_names(x, y):
    lx, ly = x.lower(), y.lower()
    if lx == ly:
        return False
    if edit_distance(lx, ly, 2) <= 2:
        return True
    tx, ty = lx.split(), ly.split()
    if len(tx) == len(ty) >= 2:
        diff = [(u, v) for u, v in zip(tx, ty) if u != v]
        return len(diff) == 1 and edit_distance(diff[0][0], diff[0][1], 2) <= 2
    return False


print(f"{'run':>16}  {'thin-twin':>9} {'+confusable':>11} {'collapses':>9} "
      f"{'thin-twin recall':>16} {'precision':>9} {'confusable recall':>17}")
for run in RUNS:
    table = R[run]
    by_chunk = defaultdict(list)
    for (s, f, bounds, g), (row, grp) in table.items():
        by_chunk[(s, f, bounds)].append((row, grp))
    thin_twin = confus = collapses = hit_thin = hit_confus = 0
    missed = []
    for _, recs in by_chunk.items():
        groups = {id(row): grp for row, grp in recs}
        for req in packed_requests([row for row, _ in recs]):
            for i in range(len(req)):
                for j in range(i + 1, len(req)):
                    a, b = req[i], req[j]
                    la = [m.get("location") for m in (groups[id(a)] or {}).get("mentions", [])]
                    lb = [m.get("location") for m in (groups[id(b)] or {}).get("mentions", [])]
                    # "thin twins": one evidence entry each, sitting in the same place
                    sig = a["entries"] == 1 and b["entries"] == 1 and bool(la) and la == lb
                    cf = confusable_names(a["focal_form"], b["focal_form"])
                    collapsed = bool(a["synthesis"]) and a["synthesis"] == b["synthesis"]
                    thin_twin += sig
                    confus += sig and cf
                    collapses += collapsed
                    hit_thin += sig and collapsed
                    hit_confus += sig and cf and collapsed
                    if collapsed and not (sig and cf):
                        missed.append((a["focal_form"], b["focal_form"]))
    pr = f"{hit_thin / thin_twin:.0%}" if thin_twin else "n/a"
    rc = f"{hit_thin / collapses:.0%}" if collapses else "n/a"
    rc2 = f"{hit_confus / collapses:.0%}" if collapses else "n/a"
    print(f"{run:>16}  {thin_twin:>9} {confus:>11} {collapses:>9} {rc:>16} {pr:>9} {rc2:>17}")

print("""
READ: 'thin twins' = two records in one request with ONE evidence entry each and
byte-identical location lists. It is a NECESSARY condition — 49 of the 50 collapses
across these four runs have it (~100% recall) — but it fires ~840-1,557 times a run,
so its precision is 1-3%. Narrowing it by 'the two names are confusable' drops it to
~75 pairs but catches only 1 of this run's 14 collapses: FE->DE is the ATYPICAL case.
Most collapses are pairs of DIFFERENT things named in one true sentence (Paladin /
Schlage / Von Duprin / latching hardware, standard-weight / heavyweight hinges).

CONSEQUENCE: 'separate near-identical focal forms in the packer' is NOT viable as
recorded — every measured pre-filter either over-treats by ~100x or misses 13 of 14.
The exact post-hoc tripwire (identical synthesis strings within one request) is the
affordable instrument. And the HARMFUL subclass — a record whose own name is absent
from the shared sentence — is already caught by the existing focal-form lint, which
flagged FE->DE both times it occurred. The other 13 are accurate but undifferentiated:
a quality axis (satisficing), not a correctness defect.""")

# ---- the per-chunk contrast that explains the mechanism
section("10b. Same twins, two chunks: evidence thickness decides the outcome")
for (s, f, bounds, g), (row, grp) in sorted(R[NEW].items()):
    if f == "equipments" and "egress" in row["key"]:
        flagged = focal_form_absent(
            row["synthesis"] or "", row["focal_form"] or "", row.get("forms") or []
        )
        print(f"  chunk {bounds:>13}  {row['focal_form']!r:36} entries={row['entries']}  "
              f"lint flags it: {str(flagged):5}  ->  {(row['synthesis'] or '')[:58]}...")
print("""  Given ONE bare navigation-menu entry each and identical locations, the pair fuses
  (FE described as DE). Given 2-3 entries of real prose in the other chunk, the SAME
  pair comes out right. The variable is evidence thickness, not the names.""")
