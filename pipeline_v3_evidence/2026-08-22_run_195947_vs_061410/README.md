# Second v3 run analyzed: 20260822T195947 vs 20260822T061410 (2026-08-22)

Run 195947 is the first run after the 2026-08-22 decisions: recursive search OFF (`max_rounds=0`), chunk + sub-window
overlap 0, the restored shortness Extent rule (+ line-break ban, parenthetical dropped) in the search statics, and the
reworked mention `location` (page + how it sits). Same two subjects (alecmfg, steelcraft), same 7 phrase fields,
`stop_after(mention_collection)`. Every search/mention request carries a new `pv=` id — nothing was replayed.

Data sources: the partial dumps under `packages/logs/extraction_dumps/<run>/`, the sample texts in
`apps/.../knowledge/sample_scraped_texts/`, and the stored requests in Mongo. **Mongo now holds only run 195947**
(the previous run's docs were scoped-deleted before the re-run); the previous run's request-level baseline came from the
earlier session's local copy (`mention_requests_prev.json`, not committed, unrecoverable) — its numbers live in
`request_audit_prev.txt`. Regenerate `all_requests.json` (not committed: holds the site text) with `pull_all_requests.py`.

- `dump_compare.py` / `dump_compare_output.txt` — side-by-side of the 14 dumps: geometry, per field-subject fold
  totals (forms, groups, empties, mentions, obligations, unaccounted, unlocated, unanchored, rekeyed, discovered casings,
  overlap duplicates), totals, tokens/time, form-length distributions, longest forms, top unaccounted forms, rekey
  categories, multi-form groups. (`rekey other` can go negative: casing and swallowed overlap — cosmetic.)
- `request_audit.py <label> <requests.json> <run_id>` / `request_audit_prev.txt`, `request_audit_new.txt` — the
  mention stage at request level: zero-hit forms (A/B/C/D classes), under-answer (returned-all-forms, with-hit answered,
  skip-ratio, worst groups), satisficing by hit frequency, context coverage, snippet extent/duplicates/order, locations
  and output-char shares, window re-send cost.
- `search_audit.py` / `search_audit_new.txt` — search fidelity (exact / casing / not-in-window), loops, empty and
  sliver windows, phrases per window, output form lengths.
- `dump_deepdive.py` / `dump_deepdive_output.txt` — unanchored composition, line-type miss rates (unaccounted vs
  accounted occurrences by prose / short-heading-list / repeated), top pages per subject, forms whose mentions sit only
  on privacy/legal/careers/cart pages, location samples.
- `deepdive2.py` / `deepdive2_output.txt` — the privacy-policy page's forms per run/field, non-bare unanchored samples,
  the three worst alecmfg material_caps windows dissected (skipped forms, where their hits sit).
- `deepdive3.py` / `deepdive3_output.txt` — unanchored by distinct (form, snippet) pair and casing-only share, URL-bearing
  locations, token cost of the privacy-page windows.

## Findings (prev → new)
- **Bought by the decisions:** unique forms 3,582 → 3,241; ≥6-word forms 731 → 374 (max 147 → 25 words; 9 → 0 across
  line breaks); ≥6-word empty forms 197 → 21; empty groups 657 (18%) → 289 (9%); rekeyed 2,157 → 981 (swallowed
  1,081 → 451); zero-exact-hit sent forms 406 (11%) → 103 (4%) (nowhere-in-subject 230 → 32); loose/hallucinated
  mentions on them 192 → 82; overlap duplicates 297 within / 87 across chunks → 0 / 0; recursive loops 16 → 0; tokens
  in 2.31M → 1.62M (−30%), out 492k → 419k (−15%); wall 2,454 → 2,005 s; re-sent window text 443k (43%) → 328k (40%).
  Mentions flat (5,137 → 5,108) on more obligations (5,928 → 6,130). Search 97% → 96% exact, no loops.
- **Worse — unaccounted 791 (13%) → 1,022 (17%)**, concentrated in alecmfg material_caps (35% → 60%): the shortness
  rule yields short generic names (Aluminum, Copper, Alloy, Steel, Titanium) whose hits sit in repeated case-study
  listing titles; the collector skips those lines (repeated heading/list miss 53% → 86%, prose 26% → 66% in that
  field). No eager stop this run (143/143 answers returned every form). `Lead`/`lead` (= lead time) declines count as
  misses. Frequency shape unchanged (recall 95/61/41/39% for 1/2–4/5–9/10–24 hits; context coverage 56/36/26%).
- **Unanchored 564 → 1,065 positions is not a regression**: distinct (form, snippet) pairs 200 → 156; the top-5
  pairs cover 567 (bare `Steel` snippet at every `Steel` substring incl. `Steelcraft` ×343, three footer-junk
  snippets under `steel` ×56 each, `Products ` ×56). Casing-only (Title-Case occurrence filed under the lowercase
  form) 315 → 222. The counter multiplies by line repetition.
- **New junk source:** steelcraft's privacy-policy page (offsets 63,831–98,888, 35k chars): forms whose mentions sit
  only on legal/careers/cart pages 63 → 103 (process_caps 10 → 62: 'verifying', 'processing', 'delivering orders';
  products 3 → 21); the two windows fully inside it cost ≈ 80k in / 12k out tokens.
- **Overlap-0 side effect:** remainder sub-windows (386 / 775 chars) → 12 search requests (9.4k tokens) + 10 dummies.
- **Location rework:** median 91 → 106 chars, 39% of output chars (was 32%), 69% carry the URL the fold derives anyway.
  Snippets improved: median 147 → 138, max 5,196 → 1,973, multi-line 16.0% → 11.6%, beyond-line 13% → 9%.

Proposals (none built): backfill (dominant loss now), casing rescue at prefill, privacy/legal page exclusion (mask like
URL lines), drop the page from `location`, merge trailing remainder sub-windows, dump `unanchored` by distinct pair,
fold-side snippet clip.

## Addendum 2026-08-22 — mechanical mention collection measured (`mech_collect_measure.py` / `_output.txt`)

User proposal under evaluation: **Search → code (casing-expand, Ctrl+F every form, clip to line/sentence, dedupe) →
LLM Location stage (semantic role of each mention, same 5k window, wire `[{mention_id, mention}]`, no forms) → Synthesis.**
The LLM collector's accepted output is already a subset of the exact-match scan (the fold discards any mention whose
snippet lacks the form), so the LLM contributes only location / extent / declines. Measured on the 86 real mention windows
of run 195947 (6 fields; contract_products shares products' requests, so this is the whole run):

- sent forms 2,904 (33 zero-hit → dropped for free); LLM collector raw mentions 3,851, fold kept 3,636 (unaccounted 840 = 19%).
- mechanical: tier-1 exact 4,476 → tier-2 casing-expanded distinct spans **4,907** (100% of occurrences; longest-span
  containment); dedupe by (casing family, clipped snippet) **3,989** mentions; dedupe by snippet alone **2,880** wire items.
- clipped snippets: median 112 chars, p90 209, max 623 (LLM: median 138, max 1,973). Items per window median 23, p90 81, max 125.
- downstream text (6 fields): today 747,976 snippet chars + 383,882 location chars; mechanical 487,145 per-form snippet
  chars (−35%) / 316,185 distinct — while covering +1,271 more occurrences.
- Location-stage cost estimate (static ≈ 961 tok, window re-sent per request, 40 out-tok/item): K=50 items/request →
  108 requests, ≈ 690k prompt (85% of today's 814k) / ≈ 115k completion (41% of 283k); K=100 → 572k (70%) / 115k.
- steelcraft privacy-page spans: 203 of 4,907 (4%; process_caps 99, products 63) — P3 still applies.

**BUILT 2026-08-22** (same day, after the user's go-ahead): the restructure above is in the code — see the plan's journal row "BUILT — mechanical mention collection + LLM Location stage" for the file list; P3 (legal-page exclusion) and P5 (remainder merge) built with it.
