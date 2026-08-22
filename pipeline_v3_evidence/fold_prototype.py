"""Prototype of the Phase 2.3 aggregation fold (PIPELINE_V3_PLAN.md D8, D9, D19),
built on the shipped 2.2 matcher, shown to the user 2026-08-21 (see
PIPELINE_V3_WALKTHROUGH_2_2.md §6). NOT production code — the seed for
core/utils/aggregation_fold.py and its tests.

SUPERSEDED 2026-08-21: the module is built and reproduces this output (golden
test in tests/test_utils/test_aggregation_fold.py); kept as the walkthrough's
illustration. Where the module differs: it attributes from the tier-1 SCAN (the
owners inside a located snippet) instead of re-matching forms inside the
snippet string, applies longest-match containment window-wide rather than per
snippet, and breaks D19 ties toward the collector's own filing.

Run from packages/core with the repo venv:
    cd packages/core && ../../.venv/bin/python ../../pipeline_v3_evidence/fold_prototype.py

Steps: A locate each LLM snippet in the window (exact substring) · B re-attribute
from the snippet (every sent form that occurs exactly; longest-match containment
owns a spot) · C same-occurrence dedup keyed (group, occurrence span), longer
snippet kept · D bundles by normalize() key in locked order (window position ==
page order then position), code-derived page, group_id as record_id; empty
bundles kept + marked · E tier-1 hold obligations computed with the SAME
containment rule applied to scan hits.
"""
from collections import defaultdict

from core.utils.floor_scan import find_form_occurrences, floor_scan, page_at, page_spans
from core.utils.form_normalizer import group_id_for_key, normalize

SEP = "#" * 50
W = (f"{SEP}\nhttps://acme.example/materials\n\n"
     "We stock Aluminum and Brass. Sample Lead Time: 2 weeks.\n"
     "aluminum alloys ship daily.\n"
     f"{SEP}\nhttps://acme.example/about\n\n"
     "Lead-free solder only. Aluminum | Brass | Steel\n")
sent = ["Aluminum", "aluminum", "Brass", "Lead", "Lead Time", "Sample Lead Time", "die-casting"]
llm = {  # a deliberately imperfect collector answer
    "Aluminum": [("materials page, intro", "We stock Aluminum and Brass. Sample Lead Time: 2 weeks."),
                 ("about page, footer menu", "Aluminum | Brass | Steel")],
    "aluminum": [("materials page, intro", "We stock Aluminum and Brass. Sample Lead Time: 2 weeks."),  # sloppy casing
                 ("materials page, line 2", "aluminum alloys ship daily.")],
    "Brass": [("materials page, intro", "We stock Aluminum and Brass. Sample Lead Time: 2 weeks.")],
    "Lead": [("materials page, intro", "Sample Lead Time: 2 weeks."), ("about page", "Lead-free solder only.")],
    "Lead Time": [], "Sample Lead Time": [], "die-casting": [],
}
spans = page_spans(W)


def locate(snippet):
    return [i for i in range(len(W)) if W.startswith(snippet, i)]


def attributions(snippet_start, snippet):
    occ = [(snippet_start + o.start, snippet_start + o.end, f)
           for f in sent for o in find_form_occurrences(snippet, f, case_sensitive=True)]
    occ.sort(key=lambda t: (t[0], -(t[1] - t[0])))
    kept = []
    for s, e, f in occ:
        if any(ks <= s and e <= ke and (ke - ks) > (e - s) for ks, ke, _ in kept):
            continue
        kept.append((s, e, f))
    return kept


print("=== A+B: re-attribution + longest-match containment ===")
mentions = []
for reported_under, items in llm.items():
    for loc, snip in items:
        for st in locate(snip):
            att = attributions(st, snip)
            flag = "" if reported_under in [f for _, _, f in att] else f"   <- LLM said {reported_under!r}; re-keyed"
            print(f"  snippet@{st:3} {snip!r}\n      kept: {att}{flag}")
            mentions.extend((f, s, e, st, snip, loc) for s, e, f in att)

print("\n=== C: same-occurrence dedup (group, occurrence span); longer snippet kept ===")
by_occ = {}
for f, s, e, st, snip, loc in mentions:
    key = (normalize(f), s, e)
    prev = by_occ.get(key)
    if prev is None or len(snip) > len(prev[4]):
        by_occ[key] = (f, s, e, st, snip, loc)
print(f"  {len(mentions)} raw rows -> {len(by_occ)} after dedup")

print("\n=== D: bundles in locked order ===")
groups = defaultdict(lambda: {"forms": set(), "mentions": []})
for f in sent:
    groups[normalize(f)]["forms"].add(f)
for (k, s, e), (f, _, _, st, snip, loc) in by_occ.items():
    groups[k]["mentions"].append((s, page_at(spans, s), f, snip))
for k, g in groups.items():
    ms = sorted(g["mentions"])
    tag = "  <- EMPTY BUNDLE: kept, no_mentions, skipped by synthesis, dump-visible" if not ms else ""
    print(f"  record_id={group_id_for_key(k)} key={k!r} forms={sorted(g['forms'])}{tag}")
    for s, p, f, snip in ms:
        print(f"      @{s:3} page={p!r} via {f!r} snippet={snip!r}")

print("\n=== E: tier-1 obligations with the same containment rule ===")
scan = floor_scan(W, sent)
covered = {(f, s, e) for (k, s, e), (f, *_) in by_occ.items()}
all_hits = [(o.start, o.end, g) for g in sent for o in scan.tier1[g]]
for f in sent:
    hits = [(o.start, o.end) for o in scan.tier1[f]]
    obligations = [(s, e) for s, e in hits
                   if not any(ks <= s and e <= ke and (ke - ks) > (e - s) for ks, ke, _ in all_hits)]
    missing = [(s, e) for s, e in obligations if (f, s, e) not in covered]
    print(f"  {f!r:18} hits={len(hits)} obligations={len(obligations)} unaccounted={missing}")
