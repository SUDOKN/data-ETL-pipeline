# Fourth v3 run analyzed: 20260823T034518 — Location retry + chunk-wide forms + page-aligned chunking (2026-08-22 night run, analyzed 2026-08-22/23)

First run after THREE builds landed on the same tree: (1) Location retry pass + chunk-wide occurrence-filtered sent forms +
'stand alone' sentence (mention node became a `BaseLLMRecursiveExtractionNode`), (2) N1 page-aligned chunking at both levels +
continued-page wire header + nonce label (kept first), (3) earlier: mechanical collection + Location stage + P3 legal-page
exclusion + P5 remainder merge. Same two subjects, `stop_after(mention_collection)`. Compared against run 20260822T223715
(alecmfg only — steelcraft crashed, see below). Log: `apps/data_etl_app/src/data_etl_app/scripts/latest_extraction_logs.txt`.
Mongo holds exactly this run: 105 docs (alecmfg 36 search + 49 Location; steelcraft 7 search + 13 Location, 11 never answered).

Scripts (paths to the session scratchpad rewritten as `<scratchpad>`; `all_requests_034518.json` = the Mongo pull, NOT committed —
regenerate with `pull_034518.py`):
- `dump_compare.py` / `_output.txt` — per field new vs prev: chunk/sub-window bounds, tokens, requests, fold summary, windows
  (sent/with-hits/zero-hit/mentions/items/described/not_described/retried/unknown/excluded), location length/URL/relative-ref stats, per-request tokens.
- `wire_inspect.py` / `_output.txt` — wire texts: continued-page header presence, nonce position, the two steelcraft legal-only windows, dummy docs, wire sample, system-message digests.
- `dump_pass2.py` / `_output.txt` — search drift (phrase-set overlap old vs new), empty-group explanation, location samples, synthesis payload estimate, out-tokens/item, group packing, steelcraft coverage, legal-window search cost.

## Verdict in one line
alecmfg: every stage behaved (0 not-described, 0 unknown ids, 0 retries needed, 0 zero-hit forms, 0 nonce leaks, no continuation
windows) and chunk-wide forms added mentions; steelcraft: CRASHED in the mention stage on a pre-answered dummy request that the
recursive base class now dispatches; page alignment + the oversize privacy page cost steelcraft 16 points of real-content coverage.

## WRONG
1. **CRASH (steelcraft, products, 26.6 s in):** `ValueError: Cannot dispatch GPT batch request …>sub>63780:90445>group>0…: it was built
   for model 'no_model' but dispatch was handed 'gpt-4.1'`. Mechanism: the mention node is now a `BaseLLMRecursiveExtractionNode`, whose
   eager path (`base_llm_recursive_extraction_node.py` ~L128) dispatches EVERY request it just created, while `BaseLLMExtractionNode`
   (~L370-379) dispatches only `find_incomplete_gpt_batch_requests_by_custom_ids(...)`. A zero-mention window gets
   `create_dummy_completed_mention_location_request` (model `NO_MODEL`, response pre-filled) → dispatched → the model guard raises.
   Run 223715 survived the same dummies because the node was then on the base class; alecmfg survived this run because none of its
   windows was empty. Trigger this run: two steelcraft sub-windows are ENTIRE legal pages blanked by P3 (63780:90445 = privacy policy head,
   26,665 chars; 136253:157472 = terms of use, 21,219 chars; wire = 180 chars each, search returned `[]` at ~765 prompt tokens each).
   Steelcraft lost all 7 phrase fields (only addresses + 2 binaries dumped); alecmfg ran after it because the sweep continues per subject.
   Fix (not applied): filter completed requests before dispatch in the recursive base (mirror the base node), or skip `NO_MODEL` bodies in
   `dispatch_batch_request`; add a test with an empty window on the recursive node.
2. **Steelcraft coverage regression (exact, tiktoken o200k on the sample text, 45,596 tok, legal 12,661):** OLD chunks 0:97071 + 97071:192276 =
   39,048 tok used of 40,000, legal inside 12,661, real 26,387 = **80%** of real content; NEW 0:63780 (13,292 tok, no legal) + 63780:157472
   (18,682, legal 10,906, real 7,776) = 31,974 used, **idle 8,026**, real 21,068 = **64%**. Cause: the privacy page (6,585 tok, oversize) did not
   fit chunk 1 → close-before-header left 33% of chunk 1 idle; it then headed chunk 2, was split across two sub-windows and blanked anyway;
   terms-of-use (4,321) filled a whole sub-window and was blanked. 47% of the 40k budget produced nothing. 'Excluded pages at zero budget'
   was listed NOT BUILT/rare in the N1 row — on steelcraft it is the dominant coverage lever (would also remove the slack).
3. **Location verbosity up again** (alecmfg, distinct locations 1,242 → 1,407): median products 215 → 246 chars, industries 207 → 262;
   URL present industries 25% → 83% (distinct basis 85%, 192/248 cite the code-derived page exactly), conformity 51% → 71%, material 48% → 64%,
   products 45% → 49%; URL chars share industries 9% → 27%; 'This passage/sentence…' prefix products 13% → 34%, industries 2% → 25%.
   Likely drivers: every window now opens on a URL line (page alignment) and the republished intro talks about URLs. Synthesis payload
   (6 fields, entries = distinct (group, snippet), first location): 1,838 → 1,912 entries, snippet 216k → 225k chars, location 403k → 469k,
   total 619k → 694k (+12%), locations 68%. Biggest record equipments `machining` 37 entries / 13.4k chars. (Accepted until the 3.2 A/B, but trending the wrong way.)
4. **Search output is unstable under re-chunking:** same subject, same search statics, new window bounds → distinct phrase sets share only
   ~55% (Jaccard 0.45–0.58); products 432 → 384 distinct, process_caps 316 → 274, conformity 42 → 56. Confounds every old-vs-new mention comparison
   and is the mechanism behind the 74% per-window consistency measured last round.
5. Tokens: mention stage alecmfg in 296,051 → 317,797 (+7%), out 91,107 → 108,363 (+19%); out/item 66→68 (products), 65→73 (industries), 72→74,
   75→82, 63→64, 70→71; in/item flat (~170–200). Search flat (174k in). Group packing still re-sends the window for tiny last groups
   (1-item requests at 6,351 and 5,666 prompt tokens; 7 requests < 10 items).
6. Minor: one unexplained empty group (`high-precision CNC machined aluminum baseplates`, process_caps chunk 1); the alecmfg cookie policy
   still sits under the homepage URL (accepted); `finish_reason` still not persisted.

## RIGHT
- **Location stage:** described 1,560/1,560 items (prev 1,371/1,419 — the 47-item nonce answer + 1 skip); `unknown_answer_ids` 0; retry sets
  created 0 (`retried` empty in all 12 windows — the retry pass was armed and never needed); nonce labelled, first line in all 105 requests,
  0 nonce-as-mention-id responses. Location samples read well (nav 'Next'/'Previous' link, opening paragraph of a news page, bullet under
  'Versatility', 'Client Requirements' table cell, repeated case-study title "on multiple pages, including …", author-bio line at the end of several pages).
- **'Stand alone' sentence:** strict relative references 21 (1.7%) → 0 (0.0%) of distinct locations.
- **Chunk-wide occurrence-filtered forms:** forms_with_hits == sent_forms in every window, zero-hit forms 6 → 0, discovered casings 165 → 251;
  mentions 2,374 → 2,723 (+15%), wire items 1,419 → 1,560 (+10%) on fewer search forms (expected ≈+18% items, confounded by finding 4).
  Empty groups 47 → 67, ALL but 1 explained by D8 containment (a longer group form owns the occurrence).
- **Page-aligned chunking (N1):** all 13 wire windows (both subjects) open on `##########` + URL; 0 continued-page headers needed (the one
  mid-page head, steelcraft 90445:112857, inherits the EXCLUDED privacy page → correctly no header, opens with `[page content omitted]`);
  no runt windows (alecmfg 21–28k chars, steelcraft 20–27k). alecmfg chunk 1 0:98612 → 0:95329 (little slack; pages are small).
- **P3/P5:** 4 alecmfg legal URLs excluded (cookie, privacy, terms, complianz download) in 3 windows; no remainder windows.
- **Wall time:** alecmfg 10 fields 133 s; mention stage per field 46–149 s (prev 151–236 s, queue-inclusive).

## Addendum (2026-08-23) — built after this review
The dispatch filter in the recursive base, legal pages dropped BEFORE chunking (`ChunkingStrategy.drop_excluded_pages` on `wide`, `floor_scan.drop_excluded_pages`, `PAGE_EXCLUSION_VERSION` "1", dump `run.scraped_text.excluded_pages`) and the no-address Location prompt (published) — see the plan's journal row 'BUILT — dummy dispatch fix, legal pages dropped before chunking, no-address Location prompt'. Expected on the next run: steelcraft's two chunks hold real pages only (≈100% of 32,935 real tokens vs 64% here), no legal-only windows, no P3 dummies; the sample-text coverage numbers above are the baseline to compare against.
