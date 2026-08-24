"""Mention-stage Location rewrite: run 20260824T012721 against 20260824T010654.

The six `3_phrase_mention_collection` statics were rewritten to ask for ONE COMPACT
SENTENCE carrying three things — what kind of text it is, what it belongs to, whose
words they are — plus an explicit ban on opening with a reference to the mention
itself ("This passage …", "appears", "is found", "is located", "sits").

That changes the mention stage's `pv=`, and therefore the `ud=` digest of every
synthesis request that digests those locations, so BOTH stages re-ran. Search and the
single-stage fields replayed from Mongo.

Every rate below is recomputed with ONE version of the code on BOTH runs.

Run from the repo root:
    python3 pipeline_v3_evidence/2026-08-24_run_012721_location_rewrite/location_rewrite_ab.py
"""

from __future__ import annotations

import glob
import json
import os
import random
import re
import statistics
import sys
from collections import Counter

sys.path.insert(0, "packages/core/src")
from core.utils.focal_form_lint import focal_form_absent, is_entity_shaped

DUMPS = "packages/logs/extraction_dumps"
OLD, NEW = "20260824T010654", "20260824T012721"
# products and contract_products share one synthesis request and their dumps are
# byte-identical apart from field_type; counting both overstates every total.
SHARED_DUPLICATE = "contract_products"

URL = re.compile(r"https?://|www\.|\.com\b|\.html\b", re.I)
# The ban, read literally off the static.
BANNED_OPENER = re.compile(
    r"^\W*(this|the)\s+(passage|phrase|sentence|line|word|entry|mention|text|item)\b", re.I
)
BANNED_VERB = re.compile(r"^\W*\S+(\s+\S+){0,3}\s+(appears|is found|is located|sits)\b", re.I)
# "Whose words they are" — an explicit voice/source attribution.
WHOSE_WORDS = re.compile(
    r"site's own (copy|words)|company's own (copy|words)|manufacturer's own (copy|words)"
    r"|in (its|their) own (copy|words)|own marketing copy"
    r"|from a customer|customer(’|')s? (words|quote|testimonial)|written by"
    r"|quotation from|quoted from|attributed to|authored by"
    r"|words of a (customer|supplier|publication|partner)"
    r"|a (supplier|publication|customer|third party|trade publication)'s"
    r"|does not show whose words|whose words (they are )?(is|are) not|not attributed"
    r"|no indication of whose words|source of the words is not",
    re.I,
)


def dumps_of(run: str, include_shared_duplicate: bool = False):
    for path in sorted(glob.glob(f"{DUMPS}/{run}/*.json")):
        base = os.path.basename(path)
        subject, field = base.split("__")[0], base.split("__")[1]
        if field == SHARED_DUPLICATE and not include_shared_duplicate:
            continue
        yield subject, field, json.load(open(path))


def mentions_of(run):
    """Every fold-group mention, keyed so the two runs can be paired exactly."""
    out = {}
    for subject, field, doc in dumps_of(run):
        for bounds, chunk in doc.get("chunks", {}).items():
            for group in chunk.get("fold", {}).get("groups", []):
                for m in group["mentions"]:
                    key = (subject, field, bounds, m["mention_id"])
                    out[key] = m
    return out


def stage_requests(chunk, stage):
    """Mention-collection requests are a dict keyed by sub-window; the others are lists."""
    block = chunk.get("requests", {}).get(stage)
    if isinstance(block, dict):
        for reqs in block.values():
            yield from reqs
    elif block:
        yield from block


def synthesis_chunks(doc):
    for bounds, chunk in doc.get("chunks", {}).items():
        block = chunk.get("synthesis")
        if block:
            yield bounds, chunk, block


def synth_rows(run):
    out = {}
    for subject, field, doc in dumps_of(run):
        for _, chunk, block in synthesis_chunks(doc):
            fold = {g["group_id"]: g for g in chunk["fold"]["groups"]}
            for row in block["records"]:
                out[(subject, field, row["group_id"])] = (row, fold.get(row["group_id"]))
    return out


def section(title: str) -> None:
    print(f"\n{'=' * 78}\n{title}\n{'=' * 78}")


# --------------------------------------------------------------------------- 0
section("0. Both runs present, and what actually changed")
for run in (OLD, NEW):
    files = sorted(os.path.basename(p) for p in glob.glob(f"{DUMPS}/{run}/*.json"))
    pvs = {}
    for _, _, doc in dumps_of(run):
        md = doc["run"]["extraction_metadata"]
        for stage in ("llm_phrase_mention_collection", "llm_phrase_synthesis"):
            if md.get(stage):
                pvs.setdefault(stage, set()).add(md[stage]["prompt_version_id"][:10])
    print(f"{run}: {len(files)} dumps")
    for stage, ids in sorted(pvs.items()):
        print(f"    {stage:34s} pv={sorted(ids)}")

only_old = {os.path.basename(p) for p in glob.glob(f"{DUMPS}/{OLD}/*.json")} - {
    os.path.basename(p) for p in glob.glob(f"{DUMPS}/{NEW}/*.json")
}
if only_old:
    print(f"!! dumps missing from {NEW}: {sorted(only_old)} — excluded from every pairing below")

OLD_M, NEW_M = mentions_of(OLD), mentions_of(NEW)
paired = [k for k in NEW_M if k in OLD_M]
print(
    f"\nmentions: {OLD} {len(OLD_M):,}  {NEW} {len(NEW_M):,}  paired by mention_id {len(paired):,}"
)
same_span = sum(1 for k in paired if OLD_M[k]["span"] == NEW_M[k]["span"])
same_snip = sum(1 for k in paired if OLD_M[k]["snippet"] == NEW_M[k]["snippet"])
print(f"  of the paired: same span {same_span:,}   same snippet {same_snip:,} (both should be all)")


# --------------------------------------------------------------------------- 1
section("1. The opener ban — target ~0, baseline 94%")
for run, table in ((OLD, OLD_M), (NEW, NEW_M)):
    locs = [(m.get("location") or "") for m in table.values()]
    opener = sum(1 for L in locs if BANNED_OPENER.match(L))
    verb = sum(1 for L in locs if BANNED_VERB.match(L))
    either = sum(1 for L in locs if BANNED_OPENER.match(L) or BANNED_VERB.match(L))
    first2 = Counter(" ".join(L.split()[:2]).lower().strip(",.") for L in locs)
    print(
        f"{run}: n={len(locs):,}  banned self-reference opener {opener:,} ({opener / len(locs):.1%})"
        f"   banned verb early {verb:,} ({verb / len(locs):.1%})   either {either / len(locs):.1%}"
    )
    print(f"    most common opening two words: {first2.most_common(6)}")


# --------------------------------------------------------------------------- 2
section("2. Length and payload share — baseline median 194, p90 272, 64.1% of payload")
for run, table in ((OLD, OLD_M), (NEW, NEW_M)):
    locs = [len(m.get("location") or "") for m in table.values()]
    snips = [len(m.get("snippet") or "") for m in table.values()]
    ordered = sorted(locs)
    print(
        f"{run}: n={len(locs):,} median={statistics.median(locs):.0f} "
        f"p90={ordered[int(len(ordered) * 0.9)]} max={max(locs)} "
        f"total={sum(locs):,} ch   snippet total={sum(snips):,} ch   "
        f"location share of (location+snippet) = {sum(locs) / (sum(locs) + sum(snips)):.1%}"
    )
old_tot = sum(len(OLD_M[k].get("location") or "") for k in paired)
new_tot = sum(len(NEW_M[k].get("location") or "") for k in paired)
print(f"\non the {len(paired):,} paired mentions: {old_tot:,} -> {new_tot:,} ch "
      f"({(new_tot - old_tot) / old_tot:+.1%})   target was -35% to -40%")


# --------------------------------------------------------------------------- 3
section("3. 'Whose words' — baseline 34%, the half of the change that ADDS information")
for run, table in ((OLD, OLD_M), (NEW, NEW_M)):
    locs = [(m.get("location") or "") for m in table.values()]
    hits = sum(1 for L in locs if WHOSE_WORDS.search(L))
    print(f"{run}: {hits:,}/{len(locs):,} = {hits / len(locs):.0%} carry an explicit attribution")
counts = Counter()
for m in NEW_M.values():
    L = m.get("location") or ""
    for phrase in ("site's own", "company's own", "customer", "supplier", "publication",
                   "does not show whose", "not attributed"):
        if re.search(re.escape(phrase), L, re.I):
            counts[phrase] += 1
print(f"  {NEW} attribution vocabulary: {counts.most_common()}")


# --------------------------------------------------------------------------- 4
section("4. Rules that must not have regressed (URLs banned, stand-alone, one sentence)")
for run, table in ((OLD, OLD_M), (NEW, NEW_M)):
    locs = [(m.get("location") or "") for m in table.values()]
    url = sum(1 for L in locs if URL.search(L))
    cross = sum(1 for L in locs if re.search(r"previous mention|same (table|list) as above|another item in this list|as (noted|described) above", L, re.I))
    empty = sum(1 for L in locs if not L.strip())
    sentences = [len(re.findall(r"[.!?](\s|$)", L)) for L in locs]
    print(
        f"{run}: URL-bearing {url} ({url / len(locs):.1%})   cross-references {cross}   "
        f"empty {empty}   sentence-enders median {statistics.median(sentences):.0f} "
        f"max {max(sentences)}   >2 enders {sum(1 for s in sentences if s > 2)}"
    )


# --------------------------------------------------------------------------- 5
section("5. Mention-stage delivery")
for run in (OLD, NEW):
    described = missing = 0
    ids, in_tok, out_tok = set(), 0, 0
    for _, _, doc in dumps_of(run, include_shared_duplicate=True):
        for _, chunk in doc.get("chunks", {}).items():
            for req in stage_requests(chunk, "llm_phrase_mention_collection"):
                if req["custom_id"] in ids:
                    continue
                ids.add(req["custom_id"])
                in_tok += req.get("input_tokens") or 0
                out_tok += req.get("output_tokens") or 0
    for _, _, doc in dumps_of(run):
        for _, chunk in doc.get("chunks", {}).items():
            for g in chunk.get("fold", {}).get("groups", []):
                for m in g["mentions"]:
                    L = m.get("location") or ""
                    if L and "not described" not in L:
                        described += 1
                    else:
                        missing += 1
    print(
        f"{run}: {len(ids)} mention requests  in={in_tok:,} out={out_tok:,} "
        f"est. ${in_tok / 1e6 * 2 + out_tok / 1e6 * 8:.2f} | "
        f"entries with a usable location {described:,}, without {missing}"
    )

# The mention stage reads the same windows and the same forms in both runs, so every
# `ud=` must be identical and only `pv=` may differ; the synthesis `ud=` MUST differ,
# because the locations it digests are what changed.
for stage, expect_same_ud in (
    ("llm_phrase_mention_collection", True),
    ("llm_phrase_synthesis", False),
):
    digests = {}
    for run in (OLD, NEW):
        table = {}
        for _, _, doc in dumps_of(run, include_shared_duplicate=True):
            for _, chunk in doc.get("chunks", {}).items():
                for req in stage_requests(chunk, stage):
                    cid = req["custom_id"]
                    key = re.sub(r"\|(pv|ud)=[^|]*", "", cid)
                    table[key] = re.search(r"ud=([0-9a-f]+)", cid).group(1)
        digests[run] = table
    both = set(digests[OLD]) & set(digests[NEW])
    changed = sum(1 for k in both if digests[OLD][k] != digests[NEW][k])
    verdict = "as expected" if (changed == 0) == expect_same_ud else "!! UNEXPECTED"
    print(
        f"{stage}: {len(both)} shared request ids, {changed} with a changed ud= "
        f"({'all identical' if expect_same_ud else 'all should differ'}) -- {verdict}"
    )


# --------------------------------------------------------------------------- 6
section("6. Synthesis — the input genuinely changed, so re-check delivery")
for run in (OLD, NEW):
    total = Counter()
    for _, _, doc in dumps_of(run):
        for _, _, block in synthesis_chunks(doc):
            for key, value in block["summary"].items():
                total[key] += value if isinstance(value, int) else len(value)
    ids, in_tok, out_tok = set(), 0, 0
    for _, _, doc in dumps_of(run, include_shared_duplicate=True):
        for _, chunk, _ in synthesis_chunks(doc):
            for req in stage_requests(chunk, "llm_phrase_synthesis"):
                if req["custom_id"] in ids:
                    continue
                ids.add(req["custom_id"])
                in_tok += req["input_tokens"] or 0
                out_tok += req["output_tokens"] or 0
    print(
        f"{run}: records={total['records']} synthesized={total['synthesized']} "
        f"not_synthesized={total['not_synthesized']} retried={total['retried']} "
        f"unknown={total['unknown_answer_ids']} | {len(ids)} requests "
        f"in={in_tok:,} out={out_tok:,} est. ${in_tok / 1e6 * 2 + out_tok / 1e6 * 8:.2f}"
    )

OLD_R, NEW_R = synth_rows(OLD), synth_rows(NEW)
shared = [k for k in NEW_R if k in OLD_R]
print(f"\nsynthesis records paired across the two runs: {len(shared):,}")
same_focal = sum(1 for k in shared if OLD_R[k][0]["focal_form"] == NEW_R[k][0]["focal_form"])
print(f"  same focal form: {same_focal:,} (the fold did not move, so this should be all)")
for run, table in ((OLD, OLD_R), (NEW, NEW_R)):
    lengths = [len(r["synthesis"] or "") for r, _ in table.values()]
    print(
        f"  {run}: synthesis text mean={statistics.mean(lengths):.0f} "
        f"median={statistics.median(lengths):.0f} total={sum(lengths):,} ch"
    )


# --------------------------------------------------------------------------- 7
section("7. Focal-form lint, one version of the code on both runs")
for run in (OLD, NEW):
    shaped, flags = 0, []
    for subject, field, doc in dumps_of(run):
        for _, _, block in synthesis_chunks(doc):
            for row in block["records"]:
                if is_entity_shaped(row["focal_form"] or ""):
                    shaped += 1
                if focal_form_absent(
                    row["synthesis"] or "", row["focal_form"] or "", row.get("forms") or []
                ):
                    flags.append((subject, field, row["focal_form"]))
    print(f"{run}: {shaped:,} entity-shaped records, {len(flags)} flagged")
    for f in flags:
        print(f"    {f}")


# --------------------------------------------------------------------------- 8
section("8. Attribution — does the party a location names survive into the synthesis?")
PARTY = re.compile(r"\b(Allegion|Falcon|LCN|Von Duprin|Schlage)\b")


def parties_named(group):
    out = set()
    for m in (group or {}).get("mentions", []):
        out |= set(PARTY.findall(m.get("location") or ""))
    return out


for run, table in ((OLD, OLD_R), (NEW, NEW_R)):
    named = kept = 0
    for key in shared:
        row, group = table[key]
        parties = parties_named(group)
        if not parties:
            continue
        named += 1
        kept += any(p in (row["synthesis"] or "") for p in parties)
    print(
        f"{run}: of the {len(shared):,} shared records, {named} have a location naming a "
        f"party; {kept} carry it into the synthesis"
        + (f" ({kept / named:.0%})" if named else "")
    )
# A location can stop naming a party only if the REWRITE dropped it — worth naming.
lost = [
    k for k in shared
    if parties_named(OLD_R[k][1]) and not parties_named(NEW_R[k][1])
]
print(f"records whose location named a party in {OLD} but not in {NEW}: {len(lost)}")
for k in lost[:6]:
    print(f"    {k[0]}/{k[1]} focal={NEW_R[k][0]['focal_form']!r}")


# --------------------------------------------------------------------------- 9
section("9. Samples — the same mentions, both runs")
random.seed(11)
for key in random.sample(paired, 8):
    print(f"\n[{key[0]}/{key[1]}] snippet={NEW_M[key]['snippet'][:70]!r}")
    print(f"  OLD ({len(OLD_M[key]['location'] or '')} ch): {OLD_M[key]['location']}")
    print(f"  NEW ({len(NEW_M[key]['location'] or '')} ch): {NEW_M[key]['location']}")


# -------------------------------------------------------------------------- 10
section("10. The party names the rewrite dropped — the one regression signal")
for k in lost:
    old_locs = [m.get("location") or "" for m in (OLD_R[k][1] or {}).get("mentions", [])]
    new_locs = [m.get("location") or "" for m in (NEW_R[k][1] or {}).get("mentions", [])]
    print(f"\n{k[0]}/{k[1]}  focal={NEW_R[k][0]['focal_form']!r}")
    for old, new in zip(old_locs, new_locs):
        if PARTY.search(old) and not PARTY.search(new):
            print(f"  OLD: {old}")
            print(f"  NEW: {new}")
            break
    print(f"  OLD synthesis: {(OLD_R[k][0]['synthesis'] or '')[:190]}")
    print(f"  NEW synthesis: {(NEW_R[k][0]['synthesis'] or '')[:190]}")
