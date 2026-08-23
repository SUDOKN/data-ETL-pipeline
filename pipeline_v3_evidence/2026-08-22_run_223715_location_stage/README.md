# Third v3 run analyzed: 20260822T223715 — mechanical mention collection + LLM Location stage (2026-08-22)

First run of the restructured mention stage (code Ctrl+F collection, LLM describes LOCATION only, P3 legal-page exclusion,
P5 remainder merge; `max_mentions_per_request=50`). Same two subjects (alecmfg, steelcraft), 7 phrase fields,
`stop_after(mention_collection)`, zero errors. Compared against run 20260822T195947 (the last LLM-collector run).
Mongo holds exactly this run (the notebook's `prepare_manufacturer` drops the deferred doc + every stored request):
106 Location + 84 search docs; search re-ran (identical custom ids, different token counts — no stale replay).

Data: the partial dumps under `packages/logs/extraction_dumps/20260822T223715/`, the stored requests (pull with
`pull_new.py` → `all_requests_new.json`, not committed: holds the site text), and the wire window texts inside them.
NOTE: the local sample texts (and the S3 object at the dump's version id, for alecmfg: 142,826 chars) do NOT equal the
text the pipeline chunked (142,870) — offset checks against them fail; `wire_verify.py` verifies against the wire texts.

- `run_compare.py` / `_output.txt` — per field-subject table new vs prev (tokens, requests, windows, forms, groups,
  empties, mentions, distinct snippets, described/not_described, discovered casings, excluded pages), totals, search identity,
  window geometry, excluded pages, per-request tokens/items, location text statistics, same-id-across-windows, groups, provenance.
- `deepdive.py` / `_output.txt` — not_described windows, search replay check, zero-hit forms, empty-group classification,
  stratified location samples (random, bare-form, testimonial, hedge, longest/shortest, listing titles, Steel group, restatement).
- `mongo_inspect.py` / `_output.txt` — wire shape (system/user message, fences), hold audit (sent/returned/unknown/missing ids),
  empty answers (dummies), completion tokens per item, search output counts. `inspect2.py` — the 0/47 raw response, the 35/36
  skip, the alecmfg homepage cookie text. `casing_loss.py` — rescued-casing pairs, `lead`/`production`/`part` group composition.
- `verify_and_locations.py` / `_output.txt` — location composition (URL / prefix chars, relative references, hedges,
  window-head items), synthesis input volume new vs prev, largest synthesis records.
- `wire_verify.py` / `_output.txt` — EXACT verification on the wire texts: spans, snippet containment, coverage of collected
  forms, `Steel`-in-`Steelcraft`, and the window-local-forms loss channel.

## Findings (prev 195947 → new 223715; 6 fields × 2 subjects, contract_products = products duplicate)
- **Coverage, verified on the exact wire texts:** 2,650 spans exact / 0 bad (exact windows), 4,726 / 4,726 snippets inside their
  window, **0 uncovered occurrences of any collected form** (100% vs 81%), 0 `Steel` inside `Steelcraft`, 0 mentions on excluded
  pages, 0 multi-line snippets. Mentions 3,636 → 4,726 (+30%); distinct snippets (wire items) 2,755 (estimate 2,880); snippet
  median 138 → 103 chars, max 1,973 → 382. Empty groups 227 → 72, all explained (63 swallowed by a longer owner under D8
  containment, 9 zero-hit). Zero-hit sent forms 103 (4%) → 12 (0.4%). Casing rescue live: 308 discovered casings, 461 mentions
  collected under a rescued casing.
- **P3:** 7 legal URLs excluded (alecmfg cookie/privacy/terms/complianz; steelcraft cookie/privacy/terms); the privacy-page
  window 70223:97071 yields 0 items → dummy in 6 fields; 63 `[page content omitted]` markers on the wire; no location mentions
  them. Limitation: alecmfg's cookie policy (6,978 chars) sits under the homepage URL `https://alecmfg.com/` (1 junk mention).
- **P5:** no remainder windows (all sub-windows ≥ 19.5k chars); 14 search ids new (the merged tails).
- **Tokens:** mention stage in 813,996 → 584,140 (72%; estimate 85%), out 282,938 → 179,053 (63%; estimate 41%); search in
  462k → 363k; wall 1,804 → 1,601 s; 106 requests (7 dummies) vs 153; 1/2/3 requests per window = 69/24/5.
- **Hold:** sent ids 2,755, returned 2,708, unknown 1, missing 48, duplicates 0, unparseable 0; described 2,707, defaulted 48.
- **DEFECT — nonce read as a mention id (1 request, 47 items):** alecmfg industries 98612:123329 answered
  `{"mentions":[{"mention_id":"a02b6f52fadc47a99ed7426440cd0701","location":"There is no mention with the id '…' in the provided
  <<<MENTION_IDS>>> list…"}]}` — that hex is the `uuid4().hex` nonce `create_base_gpt_batch_request` prepends to every user
  message; the window starts mid-page so the nonce sat directly above prose. Hold dropped it; 47 locations defaulted (no data
  lost). Plus 1 benign skip (35/36, `Lead Time (Pilot Lot)`).
- **Location verbosity is the new dominant cost:** median 218 chars (LLM collector: 106), 30% > 250, 70 completion tokens/item
  (estimate 40); location chars = 2.13× snippet chars; URLs = 18% of location chars (55% of locations carry one), "This passage
  appears…" prefix 6%, restatement verbs in 27%, 9% with ≥ 80% content-word overlap with the snippet. Synthesis entry payload
  (distinct snippets per group, first-occurrence location): entries 2,854 → 3,581; snippet chars 592k → 429k (−28%) but location
  chars 310k → 806k (+160%) → total 902k → 1,234k chars (+37%, ≈ 308k tokens); locations = 65% of it. Largest record 11.4k chars
  (steelcraft `steel`, 34 entries).
- **Relative references:** 72 locations (2.7%) refer to other mentions of the same request ("immediately following the previous
  mention", "the second sentence of the same testimonial") — meaningless once the entry travels alone.
- **Window heads:** 273 items sit before their window's first URL line (page known to code, not on the wire); the model hedged 59
  ("before any URL") and named a page anyway for 148, sometimes not code's page.
- **Remaining loss channel — window-local sent forms:** 1,144 occurrences of forms that have mentions elsewhere in the same chunk
  sit uncollected in windows where search did not list them (of 2,544 such occurrences; the rest lie inside a longer collected
  span); 491 distinct (subject, field, line) ≈ +18% wire items if forms were chunk-wide. Top: steelcraft equipments `doors` 77,
  `frames` 55, `steel doors and frames` 44; products `steel` 39, `cnc machining` 30; industries `manufacturing` 30, `machining` 29.
- **Polyseme import (D14 burden, as predicted):** alecmfg material_caps `lead` = 17 mentions, 12 "Lead Time", 2 roles, 3 verbs,
  0 metal; steelcraft `lead` = 2 lead-time. Search junk survives as short forms (`gap`, `N5`, `E6`, `TGP`, `LCN`, `cnc`).
- Same snippet in several windows gets a different location per window (352 ids, all differ) — expected; synthesis entries take
  the first occurrence's. `finish_reason` still not persisted. Location quality by eye: role named and accurate in the samples
  (nav item, section heading, Features-and-Benefits bullet, case-study title in 'More Posts', direct client quotation with
  attribution, regulatory disclaimer, "site's own copy"); recurrence handled ("across all pages", "also appears in…").

Proposals (none built; user decision): N1 label/move the nonce or prepend the inherited page URL line as the window header
(fixes the nonce confusion AND the head attribution); N2 retry/under-answer policy for the Location stage (unknown-id or
missing > X%); N3 sent-form scope — window-local vs chunk-wide vs document-wide; N4 location cost — wait for the synthesis
A/B, or strip URLs mechanically (−18%) / prompt "stand alone, no URL, no restatement"; N5 `unknown_ids` on the window row of
the dump; N6 content-cue legal exclusion (cookie text under a non-legal URL).

## Addendum (same day) — user review, search metric (Q3), search self-evaluation (finding 8), provenance resolved
- **Provenance puzzle resolved:** `PipelineContext.subject_text` = `scraped_text_file.text` = the S3 object after
  `pure_utils.text_normalize.normalize_scraped_text` (NFC, removals, line-separator/space/punctuation canonicalization) applied
  at load in `scraper/models/s3/scraped_text_file.py`. Offsets (GT anchoring included) must always be checked against the
  NORMALIZED text; the raw S3 object and the local sample files are not it (alecmfg: 142,826 vs 142,870 chars).
- **Q3 — search per-window consistency on its own vocabulary** (`search_metrics.py` / `_output.txt`): over the subject's
  document-wide search vocabulary per field, 10,279 whole-word occurrences in the windows' scan domains; 5,385 listed in that
  window, 2,188 inside a longer listed form, **2,706 missed (consistency 74%)**; by field alecmfg 58–95% (industries 58%,
  products 74%), steelcraft 53–89% (equipments 53%, process_caps 61%, products 77%). Caveat: the metric counts a family's
  occurrences in any sense — a generic family listed once (industries `production`) inflates the misses, and `Doors` under
  equipments is a precision error in the window that listed it, not a miss elsewhere. Chunk-scope version (the user's
  proposal): 1,144 missed occurrences / 491 distinct lines (`wire_verify.py`).
- **Finding 8 — search self-evaluation without the prompts** (`search_self_eval.py`, `_output.txt`, `_judgments.txt`,
  `products_eval_lists.txt`): four products windows read in full (alecmfg 0:24296 + 98612:123329, steelcraft 0:23757 +
  120661:144569), all 422 search forms judged by the plain meaning (products ∪ contract products = goods made/offered).
  **Precision (valid + borderline): 37% overall — alecmfg 10% (24/236), steelcraft 70% (131/186).** The alecmfg mass is
  process/service 43, material 33, client's product/application 18, spec/attribute/document/equipment/facility 90, generic 23,
  UI 5 (e.g. `1020 mm × 720 mm × 460 mm`, `Weld map`, `welders`, `alcohol`, `over 1,000 square meters`, `nine cutting-edge`,
  `equipment` from "Rail Equipment Manufacturer", `parts` from the cookie text). **Recall of my own product designations:
  79/84 = 94%** (missed: `Pilkington Pyrostop® glass`, `ceramic glass`, `special glazing compounds`, `custom frames`,
  `precision parts`). Verdict: the products search over-generates (precision), it does not miss (recall); the per-window
  inconsistency is largely the over-generation being inconsistent.
- **User decisions (2026-08-22):** nonce is cache-busting → move it to the END of the user message; under-answer policy for the
  Location stage: YES; sent forms: collect across the chunk's sub-windows and send each sub-window only the forms that occur in it
  (chunk-wide, occurrence-filtered); Location descriptions must not use relative references to other mentions; location
  verbosity ACCEPTED (benefit measured later at 3.2); page-aligned chunking is the user's work in another chat; polyseme handling
  waits for synthesis; the cookie-under-homepage-URL miss is accepted; grounding comes next.

**BUILT same day (user go-ahead):** Location retry pass, chunk-wide occurrence-filtered sent forms, stand-alone sentence in the six Location statics (published + pinned). The nonce was left untouched (another agent owns it). Details: the plan's journal row 'BUILT (user go-ahead) — Location retry…'.

## N1 — page-aligned chunking (built 2026-08-22, page-alignment chat)

- `n1_page_vs_window.py` (+ `_output.txt`): real chunker under `wide` on the sample scraped texts — every one of 39 windows opened mid-page; per-window tokens back to the previous URL / ahead to the next; page sizes vs the 5k window (1 of 307 pages over, the steelcraft privacy policy; acimachine by chars: 1 of 1,430).
- `n1_page_packer_sim.py` (+ `_output.txt`): a page-aligned greedy packer (whole pages ≤ 5k, 4 windows per 20k chunk, max 2 chunks) vs today: windows 1/7/6/3/6/8/8 → 1/8/6/4/7/8/8, fill median 4.2–4.7k, one 766-tok runt, coverage under the 2-chunk cap steelcraft 189k → 157k chars, taylordunn 182k → 176k.
- What shipped: `ChunkingStrategy.align_to_page_headers` + `chunk_util.break_before`, `floor_scan` page = separator + URL, `wire_window_text` continued-page header, six Location statics republished. Journal row 'N1 BUILT' in `PIPELINE_V3_PLAN.md`.
