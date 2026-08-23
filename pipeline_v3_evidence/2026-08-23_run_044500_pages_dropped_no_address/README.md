# Fifth v3 run analyzed: 20260823T044500 — legal pages dropped before chunking + no-address Location prompt + dummy-dispatch fix (2026-08-22 evening run)

First run after the three fixes built off the fourth-run review (plan journal row 'BUILT — dummy dispatch fix,
legal pages dropped before chunking, no-address Location prompt'): (1) the recursive base dispatches only requests
without a stored response, (2) excluded (legal) pages are REMOVED from the text before chunking
(`ChunkingStrategy.drop_excluded_pages`, `PAGE_EXCLUSION_VERSION` "1"), (3) the six Location statics republished with
the user's rewrite + the "never name a page by copying its address" rule. Same two subjects,
`stop_after(mention_collection)`; recursive search off (`max_rounds=0`), overlap 0, page-aligned chunking on.

Compared against **20260823T034518** for alecmfg and **20260822T223715** for steelcraft (the last run in which
steelcraft completed — it crashed in 034518).

Log: `apps/data_etl_app/src/data_etl_app/scripts/latest_extraction_logs.txt` (2,451 lines, 227 dispatched requests,
0 errors, 4 warnings — all one incident, below).

Scripts: `dump_compare.py` / `_output.txt` (per-field new vs prev: bounds, tokens, requests, fold summary, per-window
counters, location stats) and `pass2.py` / `_output.txt` (token coverage, search drift, packing/re-send cost,
location shape, synthesis payload, empty groups, discovered casings, run cost). Both run from the repo root.

## Verdict in one line
All three fixes landed and are verified on live data: steelcraft completes for the first time since 223715 and its
real-content coverage goes **64% → 100%** (mentions +52%, items +41%); locations carry **zero URLs** (was 39–83% per
field) and are shorter; the retry pass silently repaired a mis-echoed mention id. Nothing regressed except alecmfg's
raw item count, and that is search drift, not a stage fault.

## RIGHT

1. **No crash. 227/227 requests answered, 0 errors.** Both subjects finished all 10 fields: steelcraft 118.1 s,
   alecmfg 105.7 s (prev: alecmfg 133 s for the same 10 fields). The two zero-form windows that killed the last run
   (`alecmfg/conformity_attestations 0:23771`, `steelcraft/equipments 154347:158790`) produced `NO_MODEL` dummies that
   were **created and never dispatched** — the dispatch filter works on exactly the case that triggered the crash.
2. **Real-content coverage 100% on both subjects** (tiktoken o200k, `pass2.py` §1):
   - steelcraft: 224,094 ch / 45,596 tok raw → 3 legal pages dropped (privacy 35,057 ch, terms 21,219, cookie 9,028;
     29% of the site) → 158,790 ch / **32,935 tok, all of it chunked** (19,077 + 13,858 against a 40,000 budget).
     Was 64% in 034518, with 8,026 tok idle and 10,906 legal tok blanked. **The predicted number was ~100%; it is 100.0%.**
   - alecmfg: 4 legal pages, 15,134 ch → 25,094 tok, all chunked. (alecmfg was never budget-bound; this only frees slack.)
   - The exclusion list is exactly legal pages on both subjects — no over-removal.
3. **steelcraft yield follows the recovered budget:** mentions 2,352 → **3,579 (+52%)**, items (distinct snippets)
   1,336 → **1,888 (+41%)**, groups 1,123 → 1,297, for +31% input / +13% output tokens. Per field, items:
   products 543→811, process_caps 204→311, material_caps 218→302, conformity 124→207, industries 97→107, equipments 150→150.
4. **The no-address Location prompt did exactly what it was published for.** Across all 6,085 locations in the run:
   **0 contain a URL** (previous run, alecmfg per field: products 49%, equipments 41%, conformity 71%,
   industries 83%, process 39%, material 64%; steelcraft in 223715 ran 42–74%). They still identify the page — 85% name a page in words
   ("the LS Series Stainless Steel Doors page", "the manufacturing services overview page"). Median length
   246 → 208 ch (alecmfg products), 262 → 200 (industries), 216 → 169 (steelcraft products); run-wide median 194 ch.
5. **Synthesis payload down where the content did not grow:** alecmfg 674,557 → 561,259 ch (−17%), location share
   68% → 65%. steelcraft grew 562,517 → 643,583 ch but that is +32% more entries — its location share fell 65% → 60%.
6. **The Location retry pass earned its keep for the first time.** alecmfg/process_caps window `45436:67366`: the model
   echoed mention id `m2iv4vtt` for the sent id `m2iv4ltt` (a single `l`→`v` character slip), so one item came back
   undescribed. The floor-scan hold caught it, the retry request (5,078 in / 82 out) described it, and the fold shows
   `not_described: []`. Run-wide: **1,457 + 1,888 = 3,345 items sent, 3,345 described, 0 not_described, 1 unknown id, 1 retry.**
7. **Case-insensitive collection is doing real work.** `_owning_hits` matches on tier 2 (case-insensitive), so a
   casing search never returned is still collected, as a form in its own right. Measured: **722 of 6,085 mentions
   (11.9%) were harvested under a casing different from the sent form** — heaviest in steelcraft/products (253) and
   alecmfg/industries (148). The `discovered_casings` report (547 window sightings, 311 distinct
   (subject, field, form, casing) — `'welding'`→`'Welding'`, `'offices'`→`'OFFICES'`, `'FE series'`→`'FE Series'`)
   is a record of what the scan added beyond search, NOT a list of misses: 394 of the 547 appear directly as a
   harvested mention form in their window, and the remaining 153 are spans swallowed by a longer overlapping phrase
   under D8 containment (spot-checked: every `'Prototyping'` sits inside `'Rapid Prototyping'`; every window-4
   `'aluminum'` inside `'Custom Aluminum Machining'` / `'three aluminum alloy components'`). 45 short forms
   (≤3 chars) remain case-sensitive by the settled policy.
8. **Fold hygiene is clean:** `forms_with_hits == sent_forms` in every window of every field (0 zero-hit forms, both
   subjects — was 5 + 1 for steelcraft in 223715); **115/115 empty groups explained by D8 containment** (034518 had 1
   unexplained); every mention has `location_source: llm`.

## WRONG / still open

1. **Search remains unstable under re-chunking, and it now drives the headline numbers.** Because collection is
   mechanical, items are a pure function of the forms search returns. New bounds (the dropped pages moved every
   boundary) → distinct phrase sets share only **Jaccard 0.43–0.64** with the previous run. That is the whole
   explanation for alecmfg's apparent regression: mentions 2,723 → 2,506 (−8%), items 1,560 → 1,457 (−7%), with
   equipments 47 → 33 distinct phrases and conformity 56 → 41 the worst cases. alecmfg lost no content this run
   (its legal pages were already blanked in 034518), so this is drift, not loss — but it means **no old-vs-new
   comparison on a re-chunked subject measures anything but search variance**, and a Phase 5 A/B will need bounds held fixed.
2. **26% of mention-stage input tokens are the window text re-sent.** 126 requests, median 24.5 items each; every
   2nd+ group of a window re-sends the full window: 182,720 of 700,193 in-tokens. Worst cases: 1 item at 5,491 tokens
   (alecmfg/industries) and 1 item at 5,272 (steelcraft/products). 30 requests carry <10 items, 9 carry <5.
   Packing items across windows, or a larger `max_mentions_per_request`, is the lever.
3. **Locations are still 60–65% of the synthesis payload and 96% of them open with the same formula.** Median location
   194 ch vs median snippet 106 ch — the description of where a quote sits is 1.8× the quote. 5,848 of 6,085 open with
   "This passage/sentence/phrase/line/word …" (2,640 / 1,928 / 653 / 442 / 86). Accepted until the 3.2 A/B, but it is
   the single biggest lever left on synthesis input cost.
4. **19 client-level OpenAI retries** (8.4% of 227 calls), sub-second backoff, all transparently recovered — visible
   only as latency. Real per-request latency: median 3.3 s, p90 10.8 s, max 14.2 s (`client_latency_ms`).
5. **Read `time_span.seconds` with care.** `created_at` is the run's single `deferred_at`, so per-field and per-stage
   `seconds` (and `turnaround_seconds`, median 104 s here) measure *elapsed since the run began*, not the work's own
   duration — which is why alecmfg's per-field figures (139…215 s) exceed the 105.7 s the log reports for the whole
   subject. This matches the documented deferral→completion semantics; it is not a defect, but it is not a latency
   number either. Use `client_latency_ms`.

## Cost
Whole run, both subjects, all 10 fields (`contract_products` shares the `products` dump): **1,340,287 in / 203,110 out
≈ $4.31** at gpt-4.1 list. Mention stage alone: 700,193 in / 182,239 out.
