# Pipeline v3 — master plan & journal

**This file is the single source of truth for v3 progress.** The user's proposal is
[pipeline_v3.txt](pipeline_v3.txt); the design was settled across chat sessions on
2026-08-21 (this file is the record). It builds ON TOP of the v2 flip (signed off
2026-08-21 — see [PIPELINE_V2_PLAN.md](PIPELINE_V2_PLAN.md) /
[PIPELINE_V2_FLIP_REVIEW.md](PIPELINE_V2_FLIP_REVIEW.md)); v2's Phase 4 measurement
gate was v3's Phase 0 precondition until it was **abandoned by decision 2026-08-21**
(both gate subjects crashed inside stages v3 replaces — see STATE and appendix D);
v2's Phase 5 (GT rebuild) is deferred into v3's Phase 6.

**The redesign in one paragraph:** v2's relationship stage (one LLM pass that finds
mentions AND synthesizes, per wide window) satisfices — measured 45% context
coverage, ~1.7 mentions per phrase regardless of how many contexts exist. v3 splits
it: search emits **flat verbatim surface forms** per 5k window; an LLM **mention
collector** (per 5k window, exact-string contract, held to a mechanical floor scan)
deposits verbatim snippets; a **pure-code aggregation fold** re-attributes mentions,
groups forms by normalization key into entities, and builds global per-entity
bundles; an LLM **synthesis** stage (deposition-only, never sees source text, blind to
forms and groups) writes one faithful aggregation per record (~~per-mention
dispositions~~ removed 2026-08-21 — D15). Downstream
(grounding → OOV → screening → descent → reconcile) re-keys from per-chunk records
to per-group records. Guiding principle, settled explicitly: **LLM judgment only
where text is visible (search, mention collection, synthesis); everything blind is
deterministic code (matching, attribution, grouping, ids); every LLM stage
answerable to a mechanical floor.**

---

## STATE

### RESUME HERE (written 2026-08-24, tenth run; deep supplement + notes reconciliation same day — supersedes every earlier RESUME block)

**NOTHING IS PENDING.** `assemble_prompts.py check` passes; the six Location statics are published
(02:03:41Z) and measured. The next step is **3.3**.

Read this block, then
`pipeline_v3_evidence/2026-08-24_run_020729_heading_verbatim/README.md` for any number.

---

**Run `20260824T020729` = the heading-verbatim fix, and it landed.** 20 of 20 dumps, field set identical
to the last complete run, so the three fields `012721` lost are back. A clean A/B despite the stored batch
requests being deleted beforehand: the delete was scoped and left search alone (all 96 `llm_search` rows
survive from `04:45`, and the run's search stage replays with `started_at = 2026-08-23T04:45:00`), so the
`ud=` is IDENTICAL on all **93** shared mention request ids and only `pv=` differs (the first write-up
said 37; the supplement recounted).

| metric | `012721` | `020729` |
|---|---|---|
| `LEED Credits` / `CalGreen` provenance | "a list of links or resources" | **"the 'More from Allegion' section"** — fixed |
| records losing a party name | 7 | **0** Allegion-class; 7 Falcon page-enum on the full pairing (supplement §8) |
| party survival into synthesis | 9/18 = 50% (full pairing 16/34 = 47%) | **12/20 = 60%** (full pairing **19/29 = 66%**) |
| opener ban / "whose words" / URLs / cross-refs | 0% / 100% / 0 / 0 | **0% / 100% / 0 / 0** — all held |
| location chars (paired) | 289,956 | 309,901 (**+6.9%**, the price of quoting headings) |
| delivery | clean | **2,133/2,133, 0 retries, 0 unknown** |
| cost | — | mention $2.81 + synthesis $2.44 = **$5.25** |

**THE ONE OPEN DEFECT — and it is NOT the heading fix's doing.** The FE→DE entity swap recurred:
`steelcraft/equipments`, focal form and sole snippet both `FE Series Double-Egress Frames`, synthesis says
`DE Series`. The focal-form lint caught it (1 of 700; 0 of 385 last run), and a Series sweep counting
focal form + forms + snippets + **locations** as evidence gives 0/65 vs **1 genuine of 182**.

**This RETIRES the plan's "REPAIRED" verdict on the swap.** Across five observations of the same record:
`195031` right, `200044` wrong, `002404` right, `012721` right, `020729` wrong — **two in five, stochastic
and unfixed.** The `002404` reading was a single lucky sample. The FE and DE records are twins whose
locations are byte-identical to each other in BOTH runs, so the location is not the variable; the model
collapses them and emitted the *same synthesis string* for both. **Fix candidate (not built, not
measured): the packer puts near-identical focal forms in the same request with nothing to tell them
apart — either separate them, or give synthesis an explicit "these two records are different entities"
discriminator. Decide before trusting any per-entity output downstream.**

**THE SAME-DAY DEEP SUPPLEMENT** (`deep_supplement.py` + output, in the evidence folder) **corrected two
numbers and added three measurements.** Corrections: (1) the mention stage shares **93** request ids (not
37), every one with an identical `ud=`; (2) the first pass paired records by (subject, field, group_id),
which collides for **242 group_ids present in BOTH chunks of a field** and silently dropped 114 records —
the **7 party-name losses hid there, all steelcraft/conformity_attestations, all the Falcon
page-enumeration class** (the old location listed every product page carrying the bullet, "… and SZ Series
Falcon Flush Doors"; the compact new style names fewer pages, so the sub-brand rode out of the list). The
Allegion class the fix targeted stays fixed, and full-pairing survival still improved 47% → 66%.
Measurements: (1) **identical-synthesis collapse census** over the four runs — 12 (`002404`) / 23
(`010654`) / 1 (`012721`, the missing-products artifact) / 14 (`020729`) same-request pairs sharing one
byte-identical synthesis string: a STANDING synthesis behavior, not the heading fix's; the FE→DE pair is
the ONLY one whose shared string is wrong for a record — the rest are composite sentences naming both
entities, reused verbatim across co-packed records, concentrated in `steelcraft/products`. (2) **Twin
census**: 130 confusable same-request focal-form pairs (93 in `steelcraft/products`; the `System Set
D1…S4` family alone contributes 66) — the population the FE→DE fix must protect; 1 invention among them
this run. (3) Own-name hits rose 2,154 → 2,477 (steelcraft) with identification steady at ~100% of
records — verbatim headings inject the name; payload, not a defect. Also: document rule HELD under the new
locations (scoped 19 → 20, unscoped 0 → 0), mention delivery clean (3,345/3,345 described, all zeros),
client latency p50 8–10 s / p90 ~16 s, paired synthesis text +1.9%, 17 transparent client-level retries in
the log (~8%, normal).

---

**OWED, in order:**

1. **3.3**, folding in — rather than doing early — the `subject_name` wiring into
   `create_record_screening_batch_request` (it currently gets `render_record_blocks(...)` alone) and the
   deletion of the now-false *"never by name"* sentence from all seven
   `4_phrase_relationship_screening/*.txt`. Screening is in `stages_disabled` today, so there is no risk
   window and no reason to touch those files twice.
2. **The FE→DE twin-record fix**, above — it wants its own decision and its own run. The supplement
   sized the population (130 confusable same-request pairs) and found the cheap tripwire: **flag identical
   synthesis strings on two records of one request** — the focal-form lint cannot see the collapses whose
   shared string names both entities (13 of this run's 14).

**Still unrun:** the `loc=1` vs `loc=0` A/B. Every run so far is the same arm.

**Open, deliberately not chased:**
- The "whose words" element is formulaic — 2,791 of 2,805 locations say "in the site's own copy". It
  carries almost nothing when 99% of a manufacturer's site is its own copy. Revisit only if the payload
  needs trimming again.
- 32 of the 77 document-listing records carry neither the scoped nor the unscoped phrasing; unmeasured,
  never broken. Root cause is upstream — document titles are extracted as `products` at all (products
  precision measured 37% on 2026-08-22).

**The recursive resume fix has NO live verification** — the publish minted new ids, so the 10 stranded
rows were never consulted. Its evidence is the 5 tests in `test_recursive_node_eager_dispatch.py` and the
next genuine mid-run failure. Do not claim otherwise.

**Tree:** committed on `new-ground-truth-v2` — `c8d04fb` (ninth run + the resume fix + the heading change),
`167537d`, `4c7a1fe`, `e32ef0a` (publish recorded), `dad8891` (tenth run analysed), plus the
deep-supplement/journal-backfill commit (hash recorded by its follow-up, per convention). The `ontology` submodule is deliberately left dirty —
it is the user's own. Suites: 1,033 passed, 1 deselected (the known `test_normalize_is_idempotent`
property). Per the evidence-folder convention only `README.md` and the analysis script are tracked;
regenerate the `.txt` by running the script from the repo root.

**Do not re-litigate:**
- **Publish is the user's action, never the agent's.**
- **Never leave two prompt changes pending at once.** `publish` ships EVERY static whose text differs from
  its pin (`_publish_static`, `assemble_prompts.py:277`).
- **A missing dump is not always a crash.** `012721` looked like a stalled run and was a finished one that
  had silently dropped three fields. Check the dump COUNT against the previous run's.
- **A `pv` change orphans every stale row, so it also hides every resume bug.** Resume behaviour is only
  observable across two runs at the SAME prompt state. Do not claim a resume fix is verified by a run that
  changed a prompt.
- **Deleting the stored batch requests is not free and not necessary.** Search and the single-stage fields
  replay from Mongo every run (worth $1.44 on the two-subject run). The scoped delete this run did was
  harmless only because it left search alone — verify that before trusting any post-delete A/B.
- **When sweeping for "invented" entity names, count the LOCATION as evidence, not just the snippet** —
  and beware a regex that captures the word before "Series" ("these Series", "frame Series"). Both
  mistakes were made in this session and produced 19 false hits against 1 real one.
- **Pair dump records with the CHUNK BOUNDS in the key.** 242 group_ids appear in both chunks of a
  field; a (subject, field, group_id) key silently drops 114 records — it hid all 7 party losses.
- **An identical synthesis string on two records of one request is a collapse**, and most collapses pass
  the focal-form lint because the shared string names both entities. 12–23 such pairs per full run.
- The `[the manufacturer]` bracket convention is retired from synthesis only.
- `ruff format` is not enforced on `synthesis_dump_util.py` or on `focal_form_lint.py`'s pre-existing
  signature line — both were already non-conformant at HEAD.
- **A jump in the "asserts a dealing" metric is not evidence the prompt invents facts.** Measure the
  register before concluding anything from that metric.
- The dump's own `focal_form_absent_records` counter is **not comparable across runs** when the lint code
  changed between them. Recompute both sides with one version.
- **The plan's recorded "whose words 34%" baseline is not reproducible** — one consistent regex over both
  runs gives 12% → 100%.
---

- **2026-08-24 (later, notes-reconciliation session) — DEEP SUPPLEMENT ON THE TENTH RUN + THE PLAN'S RECORDS RECONCILED. Evidence: `deep_supplement.py` / `deep_supplement_output.txt` beside the tenth run's README (pyright 0, ruff clean). CORRECTIONS to the tenth-run record: 93 shared mention request ids, not 37; the (subject, field, group_id) pairing collides for 242 group_ids present in both chunks, dropping 114 records — on the full bounds-keyed pairing 7 records DID lose a party name (all steelcraft/conformity_attestations, all the Falcon page-enumeration class; survival still 47% → 66%; the Allegion class stays fixed). NEW MEASUREMENTS: identical-synthesis collapse census 12 / 23 / 1 / 14 across the last four runs — a standing synthesis behavior concentrated in steelcraft/products, NOT the heading fix (012721's 1 is its missing-products artifact); FE→DE is the only pair whose shared string is wrong for a record; twin census 130 confusable same-request focal-form pairs (93 in steelcraft/products) = the population the twin fix must protect; own-name hits 2,154 → 2,477 with identification steady (verbatim headings inject the name — payload, not defect); document rule held (scoped 19 → 20, unscoped 0 → 0); mention delivery clean 3,345/3,345; latency p50 8–10 s. BOOKKEEPING RECONCILED: the append-only journal had stopped at the sixth run — rows backfilled for the seventh through tenth runs and the builds between them (they lived only in these STATE bullets); `dad8891` added to the Tree line; the four unlisted dump folders accounted for (170602 = pre-v3 single-stage smoke; 063104/063147/063808 = deferred-run polls of the overnight 3.2 run later collected by 195031; 012354 = the failed run already in the ninth-run record). Fix candidate NOT built, sharpened: the identical-synthesis-within-a-request tripwire (13 of 14 collapses invisible to the focal-form lint). Next: 3.3, then the twin-record fix.**

- **2026-08-24 (tenth run) — TENTH RUN ANALYZED (20260824T020729 vs 012721) = the heading-verbatim fix, AND IT LANDED. Evidence `pipeline_v3_evidence/2026-08-24_run_020729_heading_verbatim/README.md`. 20 of 20 dumps, field set identical to the last complete run — the three fields `012721` lost are back. CLEAN A/B DESPITE A DELETE: the stored batch requests were deleted beforehand, but the delete was scoped and left search untouched (all 96 `llm_search` rows survive from `04:45`; the run's search stage replays with `started_at = 2026-08-23T04:45:00`), so all 93 shared mention `ud=` are IDENTICAL and only `pv=` differs — verified, not assumed (the bullet first said 37; corrected by the same-day supplement). THE TARGET RECORDS ARE FIXED: `LEED Credits` and `CalGreen Building Standards` read "the 'More from Allegion' section" again in both the location and the synthesis, where `012721` had flattened it to "a list of links or resources". Records losing a party name 7 → 0; party survival 50% → 60%. (SUPERSEDED same day by the deep supplement: the pairing had dropped 114 records; on the full pairing 7 Falcon page-enumeration losses remain and survival is 47% → 66%.) Everything the rewrite won held: opener 0.0% of 2,805, "whose words" 100%, 0 URLs, 0 cross-refs, 0 empties. Price: +6.9% location text on the paired mentions (median 152 → 154, p90 202 → 224) — the cost of quoting headings. Delivery clean (2,133/2,133, 0 retries, 0 unknown); $5.25 against a ~$5.30 estimate. THE ONE OPEN DEFECT, and NOT the fix's doing: the FE→DE entity swap RECURRED (lint 1 of 700, was 0 of 385; Series sweep counting locations as evidence gives 0/65 vs 1 genuine of 182). THIS RETIRES THE "REPAIRED" VERDICT — across five observations of the same record it is right/wrong/right/right/wrong, two in five, STOCHASTIC AND UNFIXED; `002404` was a lucky sample. The FE and DE records are twins whose locations are byte-identical to each other in BOTH runs, so the location is not the variable — the model collapses them and emitted the SAME synthesis string for both. Fix candidate (not built): separate near-identical focal forms across requests, or give synthesis a discriminator. TWO MEASUREMENT MISTAKES MADE AND CORRECTED IN THIS PASS, both mine: the Series sweep first ignored LOCATIONS as evidence (9 and 19 false hits) and the regex captured the word before "Series" ("these Series", "frame Series") — count locations, and anchor the pattern. Next: 3.3, then the twin-record fix.**

- **2026-08-24 (this later session) — NINTH RUN ANALYZED (20260824T012721, the Location rewrite) + THREE THINGS BUILT. Evidence `pipeline_v3_evidence/2026-08-24_run_012721_location_rewrite/README.md` (+ `location_rewrite_ab.py`). THE RUN IS 17 OF 20 DUMPS: `alecmfg/industries`, `steelcraft/products` and `steelcraft/contract_products` vanished — 773 records, 36% — with no exception and no log while the sweep reported success. TWO CAUSES, both traced to the failed run 45 minutes earlier (`20260824T012354`: steelcraft died on `Duplicate record_id 'gn8gmdzc'`, alecmfg on a 429 no-credits part-way through industries). (A) a duplicate `record_id` raises in `parse_synthesis_response`, killing the field and its shared `contract_products` sibling; (B) THE REAL ONE — the recursive base dispatched only requests it had just CREATED, so a stored row with no response was never re-asked; since `record_response_parse_error` nulls `response`/`batch_id` ON PURPOSE to force a re-dispatch (capped at 3 by `RESPONSE_PARSE_ERROR_CAP`), **the parse-error retry had never worked for ANY recursive stage**, and the loop then broke, `are_all_requests_complete` stayed False, and `execute` returned with no `else` branch. THE REWRITE ITSELF WORKED on the 17 shared fields: opener 92.7% → 0.0%, "whose words" 12% → 100%, locations −24.6% (short of the −35/−40% target), 0 URLs / 0 cross-refs / 0 empties, focal-form lint 0 both sides, delivery clean. ONE REGRESSION: 7 records lost a party name, 5 harmlessly, but `LEED Credits` and `CalGreen Building Standards` lost "More from Allegion" — the exact pair fixed in `002404` — because the static said to name the heading "in your own words". BUILT, all three at once on the user's go-ahead: the heading-verbatim fix to the six statics (the one pending prompt change), Defect B fixed properly (converge over UNANSWERED requests via `find_incomplete_gpt_batch_requests_by_custom_ids`, new `get_incomplete_req_ids`, raise instead of silent return, `MAX_UNPRODUCTIVE_PASSES = 3`), and Defect A deliberately LEFT ALONE (both parsers document the raise on purpose; with B fixed it buys 3 re-dispatches before `RepeatedParseFailure`, and B alone would have prevented the whole loss). Suites 1,029 → 1,033 green, 1 deselected; the dispatch test went 1 → 5; pyright 0; ruff check + format clean on written lines. Next: user publishes + re-runs (expect the three fields to return unaided), then 3.3.**

- **2026-08-24 (later, same session) — EIGHTH RUN ANALYZED (20260824T010654 vs 002404, the third clean prompt A/B) = the document-listing rule. Evidence `pipeline_v3_evidence/2026-08-24_run_010654_document_rule/README.md`. THE RULE LANDED: on the 77 records whose only evidence is a document title, the unscoped 'Steelcraft provides this resource' goes 23 → 0 and an explicitly document-scoped claim ('offers a document titled X') goes 1 → 45 (58%). THE RECORDED OVERREACH DID NOT MATERIALISE: 165 conformity-attestation records, 0 newly withheld; the single record that withholds does so in BOTH runs and correctly (its evidence is a line in a list of external links under 'Allegion & Industry Links', not a certificate) — so the rule STAYS on all six statics and that fork is closed. Delivery flat (2,133/2,133, 0 not-synthesized, 0 retries, 0 unknown ids), cost $2.42 → $2.43, mean length unchanged at 365 chars. ONE DEFECT FOUND AND FIXED IN THE SAME PASS, and it was MINE: the focal-form lint mishandled possessives — `Steelcraft's Express Stock program` normalizes to a bare `s` token no synthesis contains, flagging 3 rows; it only surfaced now because the prose moved off the possessive form that had been satisfying the verbatim fast path. Fixed by stripping `'s` before normalizing (+1 test); the lint now reads 1 / 1 / 1 / 0 across 195031 / 200044 / 002404 / 010654, with the 1 on 200044 still the real FE→DE swap. NOTE the dump's own `focal_form_absent_records` showed 13 → 3, but those two numbers came from DIFFERENT versions of the lint and are not comparable — recompute both sides with one version. Also corrected in this pass: the first measurement of the document rule asked 'did the set-aside floor come back' and reported 2 of 23, which was the WRONG QUESTION — the rule never asked for the floor phrasing, it asked for the claim to be scoped to the document, which is what the 23 → 0 / 1 → 45 numbers measure. THEN BUILT (not published): the mention-stage Location rewrite, compact-sentence form per the user's decision. Suites 1,029 green, pyright 0, ruff clean. Next: user publishes the six Location statics + re-runs (≈$5.30, BOTH stages), then 3.3.**
- **2026-08-24 (this session) — SEVENTH RUN ANALYZED (20260824T002404 vs 200044, the second clean prompt A/B): the six synthesis statics as PUBLISHED. Evidence `pipeline_v3_evidence/2026-08-24_run_002404_published_synthesis/README.md`. Same 95 request ids, every `ud=` identical, six new `pv=` — so every difference is the prompt. BOTH FIXES LANDED: the FE→DE entity swap is REPAIRED (the record whose focal form and only snippet read `FE Series Double-Egress Frames` and came back describing `DE Series` now describes FE, in both the equipments and products branches) and the `<X> Series` invention sweep over 1,250 steelcraft records goes 2 → 0; both Allegion records (`LEED Credits`, `CalGreen Building Standards`) now name Steelcraft as publisher, surface the 'More from Allegion' provenance the old run dropped, and explicitly state that Steelcraft does not itself award or certify. Delivery unchanged (2,133/2,133, 0 retries, 0 unknown ids); cost identical at $2.42; output −2.6%; 'Alec Model' verified against the source text as the real company name (62 occurrences), not an invention. THREE ANSWERS: output length does NOT fall with the read-back check removed (−2.6%, so the +28% belongs elsewhere); `own_name_hits_in_syntheses` 0 → 3,896 with 100% of records naming the manufacturer is CORRECT, the counter now measures identification and wants renaming; `focal_form_absent_records` 2 → 13 with ALL 13 FALSE — the new prose re-inflects the form the old prompt quoted and the lint tests a contiguous substring (8 of 13 are `&`/`,` re-read as 'and'). TWO OWED ITEMS OFF THIS RUN: fix the lint (dump-only, cannot contaminate an A/B), and the OPEN FORK — the set-aside floor collapsed on document listings, 23 of 74 → 1 of 74, so `Falcon SZ Series` and `Steelcraft Portfolio` now carry an affirmative sentence in `products` where they carried 'no dealing shown'; literally true, not fabrication, consequence unproven while screening is disabled. The 45% → 64% dealing-assertion shift is MOSTLY REGISTER, not new claims — the old prompt's 'offered by the manufacturer' is a passive the measuring regex misses; 12 sampled records confirm it. Next: the lint fix, then the location prompt rewrite as a SEPARATE run, then 3.3.**
- **2026-08-23 (evening, this session) — SIXTH RUN ANALYZED (20260823T200044 vs 195031, a clean prompt A/B) and the first two fixes off it BUILT: the own-name ban RETIRED from the six synthesis statics (the name now identifies rather than hides), the party-preservation rule added, and a focal-form lint added to the dump. Statics are EDITED + RENDERED + `check`-verified but DELIBERATELY NOT PUBLISHED (user's call — they publish when they are ready to run); `check` names exactly the six and the published ids it cites (`iq_Pmvdu…`, `r8RmjAd5…`) are the ones run 200044 used, so nothing has left the machine. Suites core+app 968 / llm 52 green; pyright 0 errors on all three edited/new files; ruff check clean (ruff *format* is not enforced on `synthesis_dump_util.py` — it was already non-conformant at HEAD, so its pre-existing lines were left alone). NOT in this step, on purpose: the mention-stage location rewrite (it changes `ud=` and re-runs BOTH stages, which would destroy attribution against 200044 — it wants its own run) and the seven screening statics (their false "never by name" sentence and the `subject_name` wiring into `create_record_screening_batch_request` ride with 3.3, which re-keys screening anyway). Next: user publishes + re-runs `stop_after(synthesis)` → A/B vs 200044 → then the location prompt as a SEPARATE run → then 3.3.**

- **2026-08-22 (night, this session) — 3.2 BUILT: the synthesis node, the location A/B arm, the retry pass, the snippet-radius knob, and the idempotence test narrowed (option c) — all on the user's go-ahead (journal row '3.2 BUILT'). Suites core 546 / app 410 / llm_providers 52 green (the idempotence property is back in the suite at verb_fold=False + a pinned example); pyright delta = 8 errors, all in the two PRE-EXISTING families (the node-hierarchy 'overrides incompatibly' set, identical to the mention node's 7; one more `get_result` on `type[BaseNode]` in the partial dump, same as the five above it); ruff clean on every written line. Six synthesis statics REWRITTEN (focal-form Task; 'where given' wording serves both arms) → PUBLISHED + pinned, `check` clean. What the stage does: per chunk, the fold's non-empty bundles become records `{record_id: group_id, focal_form, entries}` (focal form = most frequent member form, ties → earliest; `include_location=False` strips the location from every entry), packed in bundle order under a SOFT cap `max_entries_per_request` = 50 (a record is never split; a monster record travels alone; a chunk with no records gets a pre-answered dummy), id `…>llm_phrase_synthesis>chunk>{b}>[retry>1>]group>{i}>{model|pv}|gs=50|loc=1|ud=digest(records as rendered)`; the hold is exact (unknown ids dropped + reported, never a crash); pass 2 assesses each chunk once and embeds ONE retry set for the records left unsynthesized; the partial dump gains a `synthesis` block (one row per record: key, forms, focal form, entries, status, retried, synthesis, own-name lint; summary with not_synthesized / unknown / retried / request counts). Chain now: search → recursive → mention → SYNTHESIS → (v2 tail, unreachable until 3.3) — run with `StageToggles().stop_after(PipelineStage.synthesis)`. Knobs reach `create_pipelines` as `synthesis_include_location`, `mention_collection_snippet_radius`, `max_synthesis_entries_per_request`. Radius semantics: r sentence units each side of the hit unit(s), line breaks = unit breaks, blank lines skipped, page boundary lines (separator / URL) never crossed or included, r = 0 byte-identical to the old clip (golden) and NOT in the id (`|rad=r` only when r > 0, so run 044500's mention ids still resolve). **Next: the user's re-run with `stop_after(synthesis)` (Mongo: search + Location replay, synthesis fresh), read the `synthesis` dump block, then the location A/B (`synthesis_include_location=False`, distinct ids, both arms coexist), then 3.3.**
- **2026-08-22 (late, this session) — FIFTH v3 RUN ANALYZED (20260823T044500 = first run of the dummy-dispatch fix + legal pages dropped before chunking + no-address Location prompt; evidence `pipeline_v3_evidence/2026-08-23_run_044500_pages_dropped_no_address/README.md`; journal row 'FIFTH RUN'). All three fixes verified on live data: 227/227 requests, 0 errors, the two zero-mention windows' dummies created and never dispatched; steelcraft completes and its real-content coverage goes 64% → 100% (3 legal pages = 29% of the site dropped; mentions +52%, items +41%); 0 of 6,085 locations carry a URL (85% still name the page in words; median 194 ch); the retry pass repaired one mis-echoed id (3,345/3,345 items described); 0 zero-hit forms; 115/115 empty groups = D8 containment; ≈$4.31 for the whole run. Open: search drift under re-chunking (Jaccard 0.43–0.64 — a Phase 5 A/B must hold bounds fixed), the window re-sent for every 2nd+ group = 26% of mention-stage input tokens, locations 60–65% of the synthesis payload (96% open 'This passage…'). **USER DECISIONS (this session, 2026-08-22):** (1) 3.2 synthesis receives ONE FOCAL FORM per record and is asked to 'describe the focal entity using these entries as evidence' — D15's faithful-aggregation Task is SUPERSEDED (row amended); (2) synthesis runs as TWO ARMS, with and without `location` on the wire (a stage-metadata flag → distinct request ids); (3) NEW KNOB on the mechanical collector: snippet RADIUS (widen the clipped snippet beyond today's sentence-within-line; default = today's behaviour; lives in mention-stage metadata because the snippet hash is the mention id); (4) the tree is COMMITTED (everything from the restructure through the fifth run); (5) cost levers (re-send packing, location verbosity, `max_mentions_per_request`) DEFERRED until v3 has run end-to-end — user: execute the whole pipeline first, then adjust knobs; (6) the idempotence-test fix is a pending user decision (explained in chat). Next: the 3.2 build plan in chat, then build.**
- **2026-08-23 — BUILT after the fourth-run review (user go-ahead; journal row 'BUILT — dummy dispatch fix, legal pages dropped before chunking, no-address Location prompt'): (1) the recursive base dispatches only requests without a response (the mention stage's NO_MODEL dummy is stored, never sent) + test; (2) excluded (legal) pages are REMOVED from the text before chunking in the phrase pipelines — `floor_scan.drop_excluded_pages` / `PAGE_EXCLUSION_VERSION` "1", knob `ChunkingStrategy.drop_excluded_pages` + `page_exclusion_version` on `wide` (drift = re-defer), applied by `PrefillNode.apply_page_exclusion` on every run, trimmed text handed down the chain + `PipelineContext.subject_text`, `PipelineContext.page_exclusion` → dump `run.scraped_text.excluded_pages`; single-stage fields keep the full text; (3) six Location statics = the user's rewrite + 'address' for URL + 'Name a page by what it is about … never by copying its address' — PUBLISHED + pinned (new pv). Suites core 518 / app 406 / llm_providers 52 green (+ the known idempotence test deselected); pyright delta 0; ruff clean on written lines; db_schemas regenerated (additive only). Next: the user's scoped delete + re-run (new bounds, new pv → everything replays fresh), then 3.2.**
- **2026-08-22/23 — FOURTH v3 RUN ANALYZED (20260823T034518 = first run with Location retry + chunk-wide forms + page-aligned chunking; evidence `pipeline_v3_evidence/2026-08-23_run_034518_retry_chunkwide_pagealigned/`, journal row 'FOURTH RUN'). alecmfg CLEAN: 1,560/1,560 items described, 0 unknown ids, 0 retries needed, 0 zero-hit forms, 0 nonce leaks, 0 continuation windows, strict relative refs 1.7% → 0; mentions +15%, items +10%. steelcraft CRASHED: the mention node (now `BaseLLMRecursiveExtractionNode`) eagerly dispatches the pre-answered `NO_MODEL` dummy of a zero-mention window (two sub-windows were whole legal pages blanked by P3) → `dispatch_gpt_batch_request` model guard raises; the base node filters to incomplete requests, the recursive base does not. NOT FIXED (analysis only). Page alignment + the oversize privacy page cost steelcraft real-content coverage 80% → 64% (8,026 of 40k tokens idle + 10,906 legal tokens blanked = 47% of the budget produced nothing) — 'excluded pages at zero budget' is the lever. Locations longer again (median +14%, URL-bearing industries 25% → 83%, synthesis payload +12%, locations 68%). Search phrase sets share only ~55% across the two window layouts. Next: fix the dummy dispatch, decide zero-budget exclusion, re-run steelcraft; then 3.2.**
- **2026-08-22 (night) — N1 BUILT in the page-alignment chat (this tree is SHARED with the chat that owns decisions (a)–(h) above; both append here — re-read before editing): page-aligned chunking at BOTH levels (`ChunkingStrategy.align_to_page_headers`, on for `wide`; splitter `break_before`; predicate = the `##########` separator line), `floor_scan` page = separator + URL, `wire_window_text` announces a mid-page head with `continued_page_header` (search + Location), six Location statics REPUBLISHED + pinned, nonce labelled and kept FIRST (`NONCE_LABEL`). Measured before: 39/39 sample windows opened mid-page; after: +3 windows of 39, one oversize page (excluded) left. Suites: llm_providers 52, app 406, core green except the OTHER chat's in-flight tests. Next run: new bounds → new ids (search + Location replay fresh). See the journal row 'N1 BUILT'.**
- **2026-08-22 (night) — BUILT on the user's go-ahead: Location RETRY pass (node is now recursive; one retry
  set per window for the ids the first pass left undescribed), CHUNK-WIDE occurrence-filtered sent forms,
  'stand alone' sentence in the six Location statics (published + pinned). Nonce untouched (another agent).
  Suites green (core 509 / app 406), pyright/ruff/py_compile clean. See the LAST journal row. Next: re-run, then 3.2.**
- **2026-08-22 (evening, later) — USER DECISIONS on run 223715 + search metric + search self-eval: see the LAST
  journal row. To build on go-ahead: nonce → end of user message (all stages); Location retry on missing ids;
  chunk-wide occurrence-filtered sent forms; 'stand alone' sentence in the six Location statics (+publish/pin).
  Accepted as-is: location verbosity (measure at 3.2), polyseme import, cookie-under-homepage miss. Page-aligned
  chunking is the user's separate work. Search products precision measured 37% (alecmfg 10%) at 94% recall.**
- **2026-08-22 (evening) — THIRD v3 RUN ANALYZED (20260822T223715: mechanical collection + Location
  stage + P3 + P5) — see the LAST journal row + `pipeline_v3_evidence/2026-08-22_run_223715_location_stage/`.
  The restructure delivered: coverage verified 100% on the exact wire texts (0 uncovered occurrences; was 81%),
  mentions +30%, empties 227 → 72, tokens −28% in / −37% out on the stage. New problems: ONE request answered with
  the request nonce as a mention id (47 items defaulted; nonce is `create_base_gpt_batch_request`'s `uuid4().hex`
  user-message header); location verbosity (218 chars median, 70 tok/item) makes locations 65% of the synthesis
  payload (+37% vs prev); 2.7% of locations refer to other mentions; window-local sent forms leave ~1,144
  occurrences (+18% items) uncollected. Proposals N1–N6 in the journal row await the user's decision; 3.2 still next.**
- **HANDOFF 2026-08-22 (end of session): the whole restructure is UNCOMMITTED on
  `new-ground-truth-v2` (36 paths). One pre-existing, unrelated failing test:
  `test_form_normalizer.py::test_normalize_is_idempotent` (hypothesis: `Grounded` → `ground`
  → `grind`; the L2 verb fold is not idempotent on irregular participles; normalizer untouched
  today) — fix is a user decision. The new stage has NOT been run end-to-end yet.**
- **2026-08-22 (latest) — BUILT: mechanical mention collection + LLM Location stage,
  legal-page exclusion (P3), remainder-window merge (P5); statics published + pinned;
  902 tests green. See the LAST journal row. Deferred docs from earlier runs will not
  load (`max_mentions_per_request` rename) — re-defer before the next run. Next: the
  user's re-run, then 3.2 (synthesis node; focal-form ask to be decided there).**
- **2026-08-22 (later) — USER PROPOSAL (now built, above): mechanical mention collection + LLM
  Location stage (see the LAST journal row; measured in
  `pipeline_v3_evidence/2026-08-22_run_195947_vs_061410/mech_collect_measure_output.txt`): 100%
  occurrence coverage, −35% downstream snippet text, Location stage ≈85%/41% of today's
  mention-stage prompt/completion tokens. Subsumes P1/P2/P7. Decision pending; 3.2 still next.**
- **2026-08-22 — second v3 run (20260822T195947: recursive OFF, overlap 0, shortness
  Extent, reworked location) ANALYZED vs 20260822T061410 — see the LAST journal row +
  `pipeline_v3_evidence/2026-08-22_run_195947_vs_061410/`. Decisions bought what they
  promised (empties 18% → 9%, ≥6-word forms 731 → 374, zero-hit forms 11% → 4%, overlap
  duplicates 0, tokens −30% in); the open loss is now satisficing (unaccounted 13% → 17%,
  listing-title skips) + a privacy-page junk source + remainder sub-windows + URL-heavy
  locations. Proposals P1–P7 in that row await the user's decision; 3.2 is still next.**
- **Phase:** 3 — node services + DAG. **3.1 DONE 2026-08-21 — commit
  `617af74`.** **Next substep: 3.2** (synthesis node). **Phase 2 REVIEW gate PASSED 2026-08-21** (user:
  "You can start with the next step" — the fold rule set as built, incl. the
  three refinements, approved). **RESUME HERE:** the 3.1 bullet below, then
  3.2. Commits on `new-ground-truth-v2`: `ca9a82e` = Phase 1 + 2.1;
  `e729a81` = 2.2 + walkthrough + evidence; `c1209a1` = STATE pointers;
  `0f308c9` = 2.3 + the Phase 1.2–1.4 static amendments (the 12 modified
  statics were v3 work, not v2-flip — corrected); `617af74` = 3.1. Only the
  `ontology` submodule's dirty content is not v3.
- **3.1 DONE 2026-08-21 (`617af74`) — the mention node + fold are IN THE CHAIN;
  relationship is OUT (user decision: replace now; full runs wait for
  3.2/3.3).** Core suite 472, app suite 406 (878 green); pyright delta = only
  the pre-existing families (node-hierarchy "overrides incompatibly",
  prefill `next_node`, `object`-typed dump util); ruff clean on new modules.
  **Decisions taken with the user (2026-08-21):** (a) **fold scope = per
  CHUNK** over the chunk's `search_divisor` sub-windows — the 20k macro chunk
  relationship used to see; one `FoldResult` per chunk, one synthesis per
  group per chunk; content-derived `group_id` keeps the same group joinable
  across chunks (as `record_id` did) and reconcile merges. (b) **brute
  survivors enter the mention stage as the exact casings found in each
  sub-window** (case-insensitive whole-word scan; `brute_by_sub_bounds` on
  the concept bundle, computed at prefill where the text is). (c) **fold
  identity in metadata**: `AggregationFoldMetadata{normalizer_version,
  verb_fold}` + `BatchedMentionCollectionNodeMetadata{max_forms_per_request}`,
  both Optional on `LLMPhraseExtractionMetadataV2` (stored v2 docs still
  load; staleness check turns None→set into the standard re-defer). Editing
  fold RULES without a version bump changes no metadata and no request id —
  the cheap loop; a bump hard-fails resume, and the null-and-re-run remedy
  still replays every LLM stage. (d) **24 statics PUBLISHED + pinned**
  (`assemble_prompts.py publish`, 2026-08-21): 6 reworked search, 6 reworked
  recursive, 6 mention, 6 synthesis (the latter are pinned but not yet
  registered in PromptService — 3.2). `check` is clean. The live search
  stage now speaks v3 (surface forms, casings).
  **Built:** `PipelineStage.mention_collection` (token
  `llm_phrase_mention_collection`, rank 3; relationship → 4, grounding 5,
  oov 6, screening 7, descent 8, reconcile 9 — `relationship` stays an enum
  member so stored ids can be scoped/deleted until 3.3);
  `core/services/pipeline_nodes/multi_stage/llm_phrase_mention_collection_node_service.py`
  (window-local forms = that sub-window's first search ∪ its recursive rounds
  ∪ its brute casings, exact-string dedup, sorted; `split_into_form_groups`;
  context = window text + `<<<PHRASES` fence; strict schema; dummy for an
  empty window; parse + EXACT warn-only hold per group; `get_chunk_fold` reads
  each window's sent forms back off its requests' own fences, locates windows
  by `search_sub_bounds`, inherits `preceding_page_of(text, start)`, and calls
  `fold_document`); `LLMPhraseMentionCollectionNode` (+ `Concept…`,
  `Keyword…` in core; `ContractProduct…` (shares the `products` identity like
  search), `PureProduct…`, `Equipment…` in the app) — custom id
  `…>llm_phrase_mention_collection>chunk>{c}>sub>{s}>group>{i}>{model|params|pv}|gs=N|ud=digest(group forms)`;
  bundle field `llm_phrase_mention_req_ids: {sub_bounds: [group ids]}`;
  `get_result(..., subject_text, verb_fold) -> FoldResult`; `validate_own_responses`
  opt-in. **Partial dump:** each chunk gains a `fold` block
  (`core/utils/fold_dump_util.py`: summary, groups with member forms inline +
  mentions in locked order, per-window hold — obligations, unaccounted,
  unlocated, unanchored, rekeyed, `discovered_casings` = tier-2 casings no sent
  form covered, short forms) and the request witness lists mention groups per
  sub-window. `PipelineContext[...]` now raises a NAMED error when a node's
  map is absent (a v2 tail node reading relationship under the v3 chain lands
  there; the message says to run with `stop_after(mention_collection)`).
  Factory: `DEFAULT_MENTION_COLLECTION_MAX_FORMS_PER_REQUEST = 30`,
  `VERB_FOLD_FIELDS = {material_caps, process_caps}`, mention node after
  recursive search in all 7 phrase pipelines; the v2 relationship metadata is
  still built (required field) until 3.3. **2.1 finding (hypothesis, at
  3.1):** `normalize` is idempotent only for forms without a code-token-guarded
  token (`AAAaS` → `aaaas` → `aaaa`: the guard reads casing the key no longer
  has); keys are computed once per form, so this is a documented property
  boundary, not a defect — docstring + property test narrowed, no normalizer
  change, golden digest untouched.
  **3.1 FOLLOW-UP (2026-08-21) — first v3 run analyzed (run 20260822T061410,
  alecmfg + steelcraft, all 7 phrase fields, stop_after(mention_collection), no
  errors).** What went right: every stage held (878 tests' worth of contract
  in production: exact hold, fold per chunk, code-derived pages, witness +
  fold blocks in every dump); the tier-1 floor scan caught the one real
  collector failure — steelcraft process_caps window 102672:126084 answered 2
  of 30 forms (clean JSON, `finish_reason=stop`: the UNDER-ANSWER family,
  now observed in the mention stage too) → 35 unaccounted obligations flagged,
  nothing raised; the fold repaired 448 casing mis-filings in one field alone
  and merged case/inflection variants (`STEEL/Steel/steel`, `prototype(s)/
  prototyping`, `Paladin™ PW Series …` ×3) with no wrong merge seen. What went
  wrong: (1) LONG FORMS — "fullest span" (D2) produced claims, not
  designations (mean 3.4–5.4 words, p90 up to 10, max 147 with 9 forms
  crossing line breaks, all from recursive round 2 under the "fuller span"
  clause); 65% of ≥6-word process forms are empty bundles, and long forms
  swallow their sub-forms' obligations (the dominant rekey category) → **D2
  Extent reverted to the pre-v3 shortness rule + line-break ban in all 12
  statics, published + pinned 2026-08-21** (new pv ids → next run is a new
  identity; old requests stay in Mongo). (2) Satisficing measured: unaccounted
  obligations 4–35% per field (material_caps worst: `Steel` 58 missed of 100+
  hits; menu/footer repeats), almost all on forms that DID get some mentions.
  (3) Recursive search still loops: 16 repetition/truncation events (alecmfg
  industries/process_caps, steelcraft industries/conformity), salvaged by the
  2026-08-16 fix — D3's repurpose verdict gets its Phase 5 data. (4)
  `unanchored` is mostly the model treating `Steelcraft` as containing `Steel`
  (207 in one field — the boundary guard rejecting it is correct) and
  fragment snippets of swallowed sub-forms. Unexpected: `rekeyed` casing
  variants dominate steelcraft material_caps (448) — harmless by design;
  per-window discovery surfaced 12–94 uncovered casings per field. Cost: the
  mention stage is ~40–180k input / 8–65k output tokens per field-subject,
  ~6 min wall per subject-field with search. **Watch item added:** mention-
  stage under-answer policy (warn-only hold + floor-scan flag today; retry/
  split is the same USER-OWNED family as grounding's).
  **HOW TO SEE THE FOLD (notebook `mfg_extraction_test.ipynb`):** build the
  orchestrator with `stage_toggles=StageToggles().stop_after(PipelineStage.mention_collection)`
  (the old `stop_after(PipelineStage.relationship)` also stops before the v2
  tail, but name the real stage). `prepare_manufacturer` as today for a fresh
  run; dumps land in `packages/logs/extraction_dumps/<ts>/<subject>__<field>__partial.json`,
  per chunk under `"fold"`. **To iterate on the fold without re-spending:**
  keep the stored batch requests (skip the scoped delete, or scope it to
  `PipelineStage.mention_collection, and_downstream=True` to redo mentions
  only) — custom ids are unchanged, so search + mention replay from Mongo and
  the partial dump recomputes the fold with the CURRENT rules. A run WITHOUT
  toggles reaches the v2 grounding tail and stops with the named context
  error (expected until 3.3).
- **2.3 DONE 2026-08-21 (user approved the proposed plan A–E; built as
  proposed plus three refinements found while building):**
  `core/utils/aggregation_fold.py` + 25 tests
  (`tests/test_utils/test_aggregation_fold.py`); core suite 455 green; pyright
  and ruff clean. As built — (A) locate each collector snippet by exact
  substring, one candidate per position; a non-verbatim snippet is reported
  `unlocated` and anchors nothing. (B) RE-ATTRIBUTE: the OWNERS of a window are
  its tier-1 floor-scan hits after longest-match containment (D8); a located
  snippet is a mention of every owner inside its span, whatever form the
  collector filed it under (casing repaired without a reconciler; a sentence
  holding several forms is a mention of each). (C) **D19 SETTLED:** dedup key
  (group key, occurrence span); the longer snippet is kept. (D) bundles by
  `normalize()` key over the union of every window's sent forms (global scope,
  `verb_fold` dial), `group_id = hash(key)` as the synthesis `record_id`,
  per-mention `record_id = record_id_for_phrase(form)`, mentions in LOCKED
  order (window index, offset), bundles in first-mention order with empties
  last by key, page CODE-DERIVED from the occurrence offset (inherited
  `preceding_page` honoured), advisory location carried as colour;
  **empty-bundle fate SETTLED: kept, status `no_mentions`, skipped by
  `FoldResult.synthesis_records()`, dump-visible.** (E) tier-1 obligations =
  the owners of B (the same containment rule, so fuller-span forms raise no
  phantom discrepancies for sub-forms); uncovered obligations = the window's
  `unaccounted` record. **Refinements vs the prototype (flagged for the gate
  review):** (i) attribution reads the SCAN — a snippet's mentions are the
  owners inside its span — instead of re-matching forms inside the snippet
  string: the word-boundary guard stays honest at snippet edges (a snippet cut
  at "un|lead alloys" no longer passes), and B and E become one computation;
  (ii) containment is applied at WINDOW level across all snippets, not per
  snippet — a fragment snippet "Lead Time" cannot claim a spot inside a "Sample
  Lead Time" the window holds; (iii) among equal-length snippets of one spot
  the collector's OWN filing is preferred, then lexical order (never answer
  order) — `reported_form` reads truthfully. Surfaces for Phase 3/4:
  `fold_window` → `WindowFold` (kept mentions, `scan`, per-form
  `obligations`/`unaccounted` + `has_discrepancy`, `unlocated` / `unanchored`
  (verbatim but anchors no owner — anaphora, fragments) / `rekeyed` reports,
  raw `candidates` count); `fold_document` → `FoldResult` (`bundles` of
  `MentionBundle{group_id, key, sorted forms, mentions, status}`,
  `synthesis_records()` → `SynthesisRecordInput`s that render through
  `render_synthesis_record_blocks`, `windows`, `verb_fold`,
  `normalizer_version`). Sent forms are window-local; blank forms ignored.
  Golden test = walkthrough §6 (14 candidates → 7 mentions, obligations
  Lead=1 / Lead Time=0, zero unaccounted). Properties: order-independence
  (sent order, answer keys, mention lists, ± verb_fold), idempotence on its own
  output, anchoring (snippet ⊂ window, form at its span, span ⊂ snippet, page
  == `page_at`, record_id), no kept span strictly inside any tier-1 hit,
  obligations = tier-1 minus contained, unaccounted == uncovered, bundles
  partition the sent forms by key, unique ids, synthesis count == non-empty.
- **2.2 DONE 2026-08-21:** `core/utils/floor_scan.py` + 27 tests
  (`tests/test_utils/test_floor_scan.py`), core suite 430 green, pyright clean.
  The word-boundary matcher for technical terms: `find_form_occurrences` guards
  an edge with `(?<!\w)` / `(?!\w)` ONLY when that edge of the form is a word
  char — `6061-T6` matches in "6061-T6 aluminum" not "16061-T6", `CNC` in
  "CNC/Manual", `C++` needs no right guard, `Lead` hits "Lead Time"/"Lead-free"
  never "Leader"/"leading" (the measured substring bug); Unicode-aware `\w`.
  Scan domain: `mask_page_headers` blanks URL lines and the scraper's `#`
  separator lines with spaces, LENGTH-PRESERVING (every offset stays an offset
  into the window); `page_spans` tiles the window into pages (a page starts at
  its URL line; text before the first URL is `None` unless the caller passes the
  inherited `preceding_page`), `page_at`/`FloorScan.page_of` attribute offsets.
  Two tiers: `floor_scan(window, forms)` → tier1 exact-case hits per form (the
  hold), tier2 case-insensitive (discovery), tier2 ⊇ tier1; **short-form policy
  settled: `SHORT_FORM_MAX_LENGTH = 3`** — `Al`/`SS`/`CNC` stay case-sensitive in
  tier 2 and are listed in `FloorScan.short_forms`. Same engine serves D8's
  re-attribution in 2.3 (which sent forms occur exactly in a snippet). Hypothesis
  properties: every hit is a whole-word exact substring; tier2 ⊇ tier1 and
  casefold-equal; hits sorted, non-overlapping; a form surrounded by spaces is
  always found; masking is length-preserving and pages tile the window.
- **Phase 1 REVIEW gate PASSED 2026-08-21** (user: "yes, everything sounds
  good") — 24 statics + wire contracts signed off. **Publish DEFERRED to Phase
  3.1 by decision:** publishing the reworked search/recursive statics now would
  flip the LIVE search stage to v3 semantics under v2's relationship/grounding
  (hybrid runs from the notebook in the interim); nothing in Phase 2 needs the
  S3 objects, and unpublished drift does not break runtime (pins are read at
  init; disk drift is `check`'s job). Statics publish + pins + PromptService
  registration land together with the nodes that consume them.
- **2.1 DONE 2026-08-21:** `core/utils/form_normalizer.py` + 80 tests
  (`tests/test_utils/test_form_normalizer.py`), full core suite 403 green,
  pyright clean. **Lemmatizer decided by measurement (appendix E): lemminflect
  0.2.3, DICTIONARY mode only** — pinned exactly in `packages/core/pyproject.toml`
  and asserted at import (`NormalizerEnvironmentError`); `hypothesis` added as a
  dev dep. Layers: L0 (casefold, strip ™®©, hyphens/dashes/slashes→space,
  `&`→`and`, edge punctuation, whitespace) → L1 (NOUN dictionary lemma; guard:
  digits / internal capitals / capitalized-OOV, with acronym plurals `CNCs`
  exempt; then ONE guarded OOV fallback: ≥5 letters, alphabetic, ends `s` not
  `ss/us/is/ous`, unknown under every POS) → L2 (VERB dictionary lemma applied
  to the L1 result — after, not instead, so `bearing`/`bearings` share a key)
  as a `verb_fold` bool. The field→dial mapping (process/material on) lives
  APP-SIDE at Phase 3 — the field enums are in `data_etl_app.models.types_and_
  enums`, not core. Ids: `group_id_for_key` = `g` + 7 base36 of sha256 (mirrors
  record_id_util, different prefix so the two keys can't be confused),
  `assign_group_ids` with collision error. `NORMALIZER_VERSION = "1"`; not
  salted into group_id; the golden-corpus digest test is the tripwire (recorded
  l1/l2 digests under version "1"). Property tests (hypothesis): idempotence,
  key invariants, lowercase≡uppercase for plain tokens — hypothesis caught two
  Unicode edges on first runs (dotless i; letters with no uppercase), both
  properties of Unicode, test alphabet restricted accordingly.
- **1.4 DONE 2026-08-21** (unreviewed): wire contracts built, tests green,
  pyright clean. New `packages/core/src/core/models/extraction_schemas/
  mention_collection.py` (wire `FormMentionsResponse{forms: [{form, mentions:
  [{location, snippet}]}]}`, strict response_format, `parse_mention_collection_
  response → dict[form, list[Mention]]`, dummy content) and `synthesis.py`
  (request-side `SynthesisRecordInput{record_id, entries}`, wire
  `SynthesesResponse{syntheses: [{record_id, synthesis}]}`, strict
  response_format, `parse_synthesis_response → dict[group_id, str]`, dummy).
  `phrase_blocks_contract.py` v3 section: `hold_response_to_sent_forms` (EXACT —
  the v1 casefold reconciler would fuse v3's case-variant forms; unknown keys
  drop+warn, missing warn and stay ABSENT so dumps keep never-answered ≠
  answered-empty), `render_records_array_block` + `render_synthesis_record_
  blocks` (RECORD_IDS + ARRAY records, one per line, ensure_ascii=False, dup-id
  raise), and `hold_response_to_sent_record_ids` generalized to read ids off an
  array payload (`_record_ids_of`) — one hold serves v2 grounding and v3
  synthesis. **Wire-driven prompt amendment:** mention Output became an ARRAY
  `{"forms": [{"form", "mentions"}]}` because the response_format is OpenAI
  STRICT mode, which cannot express an object keyed by arbitrary form strings
  (v2 hit the same wall: records are arrays carrying `record_id` as a field);
  six mention statics re-propagated byte-identical. Tests: `test_mention_
  collection_wire.py`, `test_synthesis_wire.py` (strict-schema acceptance,
  prompt examples decode, dup/empty/extra-key rejection, exact hold keeps case
  variants distinct, array-payload round trip incl. fence forgery, drift
  detection). **Deliberately NOT done (interpretation flagged for the gate):**
  no `PromptService.STAGED_PROMPT_FILE_PATHS` entries (registering before the S3
  object exists breaks every bot's init — follows publish), no `PipelineStage`
  members (Phase 3 node identity), NO catalogs/skeletons — the new prompts are
  field-agnostic and byte-identical, so catalog assembly has nothing to vary;
  they take the static path (`load_static_prompts` auto-discovers them; pins +
  provenance stamp at publish).
- **1.3 AMENDED 2026-08-21 (user review in chat — D14/D15 amended, D16/D18
  noted):** dispositions and split-flag REMOVED; synthesis = faithful aggregation
  of all entries; the model is blind to forms/groups. Wire: RECORD_IDS + RECORDS
  ARRAY blocks, record = opaque `record_id` + `entries: [{location, snippet}]`;
  output array mirrors input. Vocabulary = **record / entry / snippet / location**
  (entry replaces excerpt to kill the excerpt≈snippet synonymy; snippet/location
  stay byte-identical to the mention-stage field names — one name per concept
  across code/dumps/GT; alternatives on record in chat: citation, excerpt). Kept,
  all non-judgment: the manufacturer lens — reworded after user review to the
  closed catch-all **"whatever they show it does, has, or is, and whatever is
  said of it"** (action / possession / state + third-party attribution; the old
  "does, makes, offers, works with" list missed certificates, industries,
  equipment) — repeat-collapse, one deposition sentence, name-masking in output.
  ~~One convergence nudge sentence~~ REMOVED at user review ("they share a
  subject" invites predicting the subject; the synthesis just weaves entries).
  Six statics re-propagated byte-identical; tests green.
- **1.3 DONE 2026-08-21** (superseded in part by the amendment above): synthesis statics created at
  `final_texts/static/multi_stage/4_phrase_synthesis/` (six byte-identical,
  field-agnostic — synthesis reports what depositions support; field judgment
  stays downstream). Deposition framing: the model is told it does NOT see the
  website; every mention is a verbatim record by a reader who did; judgments rest
  on records alone (D15's add-text-later escape hatch unused). Input: manufacturer
  name + two-block <<<GROUP_KEYS / <<<GROUPS wire; bundle = member forms +
  numbered mentions {form, location, snippet}. Dispositions: one entry per
  mention, exactly once — {"mention": n, "use": "relied on"|"discounted", "why":
  short}; split-flag = optional machine-countable "different_thing": true (dumps +
  merge-proposer need a tally; prose-only flags can't be counted). Synthesis text
  rests only on relied-on mentions, attributes as the records attribute; the
  all-discounted case states plainly that the mentions show nothing (D14's
  polyseme resolution). NAME-MASKING RETURNS HERE per the 1.2 decision: v1
  court-reporter rule ported ('[the manufacturer]', derivatives, read-back check);
  input bundles arrive unmasked, output prose masks. Output:
  {"syntheses": {key: {dispositions, synthesis}}}, every key exactly once,
  drop-and-flag for unmatched keys. ~~Flagged for audit: the visible group key is the normalized key string~~
  SUPERSEDED by the amendment above — the key is an opaque `record_id`, because
  this is the one stage where blindness to the phrase is a design goal.
- **URL-domain cut 2026-08-21 (user decision; second 1.1/1.2 amendment — D2/D7
  noted):** URL lines are page separators, not text to harvest or match. Search +
  recursive statics drop "URL lines count as text too" and the heading gloss;
  mention statics drop the occurrence-inside-a-URL clause and the URL-line snippet
  case (orientation sentence kept everywhere; the heading gloss survives ONLY in
  the mention prompt, where it feeds location). The floor scan's domain becomes
  window text MINUS URL lines, both tiers (D7 note) — else phantom discrepancies.
  Measured basis in D7: 11 of 12 runs reported zero URL occurrences despite the v1
  instruction; the one compliant run flooded 183 sense-free URL mentions.
  Empty-bundle fate booked as a Phase 2.3 open item. All 18 statics re-verified
  (stage 2/3 byte-identical; stage 1 intros identical).
- **1.2 AMENDED 2026-08-21 (user review in chat — D5/D6 amended, D18 noted):**
  the mention wire drops record ids entirely — bare forms sent (existing
  `<<<PHRASES` fence), output keyed by the form string itself under a `"mentions"`
  key, exactly-once, empty array legal; mention = `{freehand location, verbatim
  snippet}` where location states a DEFAULT PREFERENCE (page by path or heading +
  governing heading/section or structural kind) in the model's own words, and the
  authoritative page is code-derived at the fold from snippet position; unmatched
  response keys drop and the tier-1 scan flags the hole; ~~URL-line occurrences
  kept~~ (SUPERSEDED by the URL-domain cut above — the phantom-discrepancy
  objection dissolved once the scan domain excludes URL lines too); example shows
  multiple mentions per form. Statics re-propagated,
  six byte-identical.
- **1.2 DONE 2026-08-21** (superseded in part by the amendment above): mention-collector statics created at
  `final_texts/static/multi_stage/3_phrase_mention_collection/` (six byte-identical
  files — FIELD-AGNOSTIC, since D4's sense-agnostic contract forbids field
  judgment; v1-relationship precedent for six copies per the (stage, field_type)
  lookup invariant). Contract: char-for-char occurrence matching incl. casing,
  whole-word ends, sense-agnosticism stated explicitly, mention = {page, snippet},
  output keyed by record_id via the existing <<<RECORD_IDS/<<<RECORDS two-block
  wire, every sent id exactly once, empty array legal (neutral wording — false
  empties are the tier-1 floor scan's catch), no form echo (D6's diagnostic arm
  deferred to Phase 5). Decided in-substep, flagged for audit: **snippet extent
  default = whole sentence, or whole line for non-sentence text** —
  whole-bullet-plus-heading REJECTED (non-contiguous snippet breaks the D18
  verbatim-fidelity floor), radius rejected as arbitrary; **NO name-masking in
  snippets** (deviation from the v2 relationship static — masking would break D8
  code re-attribution and the snippet⊂text check; subject-name handling moves to
  synthesis + the Phase 4.2 lint); one mention per occurrence, identical
  boilerplate repeats included (bounded by 5k windows; degeneration is what the
  floor scan flags — watch at Phase 5). Recursive statics reworked IN PLACE as
  form completion per D3: six byte-identical, field-agnostic (variant-hood is
  relative to the list, not the field — a field gloss would invite the 0/12
  find-more-entities behavior); variant = casing/spacing/punct/spelling/
  inflection/abbreviation↔expansion/fuller-or-shorter span of a listed form's
  designation; Rejection = no new entities; When-nothing-remains kept; file
  names + <<<ALREADY_EXTRACTED_PHRASES fence kept (renames are 1.4/Phase-3 wire
  churn). NOT published.
- **1.1 DONE 2026-08-21** (unreviewed — audit at the Phase 1 gate or sooner): all
  six stage-1 search statics reworked on disk for D2. Task states the exact-string
  surface-form contract; Extent flipped from split-into-shortest to fullest-span
  preference (designation-not-statement kept as the outer bound); the
  Deduplication section replaced by a Variants section that inverts it (every
  distinct wording incl. casing is its own entry; identical strings once; no
  folding). What-qualifies/Rejection untouched (field-relevance only). Shared
  sections md5-identical across the six; Task/Extent/Rejection template-matched
  modulo field gloss. NOT published — sha256 pins now drift from disk by design
  until the Phase 1 review gate. Interpretation flagged for audit: "fullest-span
  preference" rendered as "keep attached modifiers/qualifiers; prefer the whole
  designation over fragments inside it" (nested-form disambiguation is D8's
  code-side longest-match, not the prompt's job).
- **Phase 0 CLOSED by user decision in chat 2026-08-21: the v2 Phase 4 gate is
  ABANDONED and v3 proceeds at full scope.** Both gate subjects crashed at factory
  defaults inside the stages v3 replaces (run `20260822T012722`; full evidence in
  appendix D): steelcraft hit a gpt-4.1 repetition loop in the v2 relationship
  stage (burned the entire 20k completion cap, JSON truncated mid-string);
  alecmfg's freehand grounding answered 1 of 30 sent records (clean JSON) and the
  sent-records contract hard-failed. Rationale: completing the gate would mean
  repairing a stage scheduled for deletion; v2's relationship output demand is
  structurally unbounded (verbatim accounts for every mention of 30 phrases from
  a ~97KB window) while v3's 5k-window mention collector bounds it (D1/D5).
- **Baseline fallback (user-agreed 2026-08-21):** no v2 numbers will exist; v3's
  Phase 5 gate compares against the v1 hand-audit baselines (appendix C) only.
  D20 amended accordingly.
- **Carried into v3 (USER-OWNED):** the grounding under-answer failure mode
  survives the pivot — v3 grounding stays similar, and nothing forces one wire
  entry per sent record, so one lazy response kills the subject with no retry
  (appendix D, alecmfg). The user will look into the policy (re-ask missing
  record_ids / split-and-retry / schema-forced coverage) during v3.
- **Design:** fork ledger D1–D20 below is settled (chat review 2026-08-21; D20
  amended at the pivot); the evidence appendix holds the measurements the design
  rests on.
- **Blockers:** none.

*Update this block at every substep completion. Journal rows are append-only, at
the bottom of this file.*

---

## How this plan runs

1. **Every substep is a pause point**, sized to be reviewable in one sitting.
2. **Done means:** code written, `pytest` green, pyright clean on touched files,
   STATE updated, journal row appended.
3. **Every phase ends in a REVIEW gate** with a specific question for the user.
4. **Resumption protocol:** STATE → fork ledger → the substep named next → last
   journal row. No other context required.
5. Extraction runs happen via the user's notebook (`mfg_extraction_test.ipynb`) —
   ask them to run; never drive extraction directly.

---

## Fork ledger (settled 2026-08-21 — do not re-litigate without asking)

D-ids are v3's own numbering; no continuity with the v2 plan's F-ids.

| # | Fork | Decision |
|---|------|----------|
| D1 | Stage split | Relationship splits into mention collection (LLM, per 5k window) + synthesis (LLM, per group, deposition-only). Motivated by measured satisficing: 45% context coverage, ~1.7 mentions flat vs 34 contexts for `Aluminum`. |
| D2 | Search output | FLAT array of verbatim surface forms — every variant incl. casings, ~~fullest-span preference~~ **SHORT designations (Extent amended 2026-08-21, user decision, measured on run 20260822T061410 — see the 3.1 follow-up in STATE): the pre-v3 shortness rule is back in all 12 search/recursive statics — the designation not the statement, long spans split into qualifying verbatim sub-phrases, never across a line break, modifiers kept only when part of the name (the illustrative parenthetical "a grade, a series, a trade name, a standard's number" dropped the same day — user: it does not apply to all fields equally; let the model use its own judgement); recursive form-completion no longer asks for FULLER spans. Measured: forms ≥6 words were 15% of first-search output and 33% of recursive rounds (52% in process_caps; max 147 words; 9 forms crossed line breaks); 65% of ≥6-word process forms ended as empty bundles; "swallowed by a longer form" was the dominant rekey category in 11/12 field-subjects. Containment still resolves genuine polysemes whenever both spans are emitted; the long tail was claims, not designations.** Field-relevance only, NO entity binding, NO dedup by search. Window-local: each window's search covers its own text; payloads are never unioned across windows. **Domain note (2026-08-21, user decision):** URL lines are page separators, not harvest material — "URL lines count as text" dropped from search AND recursive statics. Slug-only forms were architecturally dead in v3 anyway: exact-case matching gives them no body anchor, so they either duplicate a body form's group or ride to an empty bundle. **Geometry (2026-08-22, user decision): chunk AND search sub-window overlap set to 0 (`wide.overlap = 0`; one ratio drives both). In v3 overlap only duplicated work: search is window-local and 97% verbatim, the fold runs per chunk and synthesis per group, reconcile merges groups across chunks by key. Measured cost of overlap on run 20260822T061410: 297 duplicate occurrences across overlapping sub-windows (5.8% of mentions) and 87 occurrences folded/synthesized in both chunks. The v1/v2 reasons (weaker search, relationship context at edges) no longer apply. Side effect: with `max_chunks=2` the two 20k chunks now cover ~40k tokens of the subject instead of ~37k.** **Measured on run 20260822T195947 (2026-08-22): shortness restored — ≥6-word forms 731 → 374 (max 147 → 25 words, 0 across line breaks), ≥6-word empty forms 197 → 21, swallowed rekeys 1,081 → 451; overlap duplicates 297/87 → 0/0. Side effect found: the sub-window divider leaves REMAINDER windows (386 / 775 chars → 12 search requests, 10 mention dummies) — merge proposal in the journal.** **2026-08-22 (P5, user-accepted): `derive_search_sub_bounds` merges a trailing remainder sub-window shorter than 20% of its predecessor into it (`merge_trailing_remainder`).** |
| D3 | Recursive search | Repurpose candidate: form completion ("here are the found forms, find variants we missed") replacing v1's find-more-entities round (measured 0/12 in-boundary at highest cost). Per-field toggle; final call at the Phase 5 measurement gate. **VERDICT 2026-08-22 (user decision on measurement, run 20260822T061410): OFF — `DEFAULT_RECURSIVE_SEARCH_MAX_ROUNDS = 0` for every pipeline (keyword pipelines already were). Measured: recursive output 65% verbatim (22% nowhere in its window, 12% casing drift; process_caps 41% non-verbatim) vs first search 97%; it ran on empty lists and returned page menus (2 windows, 42 phrases); of its 770 window-new phrases, 36% were not in the text, 24% longer spans, 24% "unrelated new" (its Rejection rule forbids exactly that), and its intended form-completion role (casings 11, inflections 8, shorter spans 105) carried ~235 occurrences against the 2,217 the first-search forms already carried (~10%). Casing variants are recoverable mechanically from the tier-2 scan (casing rescue at prefill — PROPOSED, not built); shorter spans are what the restored shortness Extent asks search to emit directly. The node stays in the chain as a pass-through; the statics stay published. **Confirmed on run 20260822T195947 (2026-08-22): zero-exact-hit sent forms 406 (11%) → 103 (4%), nowhere-in-subject 230 → 32, loops 16 → 0, tokens −30% in; what recursive had supplied is the casing-rescue hole (70 casing-variant zero-hit forms, 222 casing-only unanchored, 453 discovered casings) — still PROPOSED.** |
| D4 | Mention collector is LLM | Kept LLM (not code) because mention-EXTENT rules will evolve and prompts are cheaper to evolve than tokenizers. The anaphora argument is measured-weak (see appendix: 0.8% of records lack a string-anchorable mention) — flexibility, not anaphora, is the justification. Contract: sense-agnostic occurrence reporting, stated EXPLICITLY in the prompt ("report occurrences; do not judge whether it's really the material"), exact string + casing matching. **AMENDED 2026-08-22 (user decision, measured): the collector is CODE — `core.utils.aggregation_fold.collect_window` (casing-expanded whole-word scan, longest-span containment, sentence/line clip, one wire item per distinct snippet). Why: the fold already discarded any LLM mention whose snippet lacked the form verbatim, so the LLM collector's accepted output was a strict SUBSET of the scan (run 20260822T195947: LLM kept 3,636 vs scan 4,907 occurrences; 19% unaccounted, deterministic listing-title skips). The LLM's one remaining job is LOCATION: the semantic place of each passage, in its own words and extent (user: not restrictive). Code can no longer lose a mention; the model can only fail to colour it.** |
| D5 | Mention payload | ~~Flat `{record_id, form}` pairs~~ **AMENDED 2026-08-21 (user decision): bare form strings on the wire — no ids sent (see D6)**; window-local, NO payload dedup / variant logic — the LLM is deliberately dumb about variants (all casings ride as separate items; exact-case matching makes each occurrence match exactly one). Batching splits freely along forms (no judgment spans two forms). **AMENDED 2026-08-22: the wire carries the window's DISTINCT SNIPPETS as two blocks — `<<<MENTION_IDS` bare array + `<<<MENTIONS` array of `{mention_id, mention}` (`mention_id = hash(snippet)`, prefix `m`) — and NO forms (user decision: a passage's role does not depend on which word in it we care about). Groups of `max_mentions_per_request` = 50 (measured: median 23 items/window, p90 81, max 125).** |
| D6 | Mention output | ~~Keyed by `record_id` (key-masking); mention = `{page, verbatim snippet}`; optional form-echo diagnostic arm~~ **AMENDED 2026-08-21 (user decision): keyed by the FORM STRING itself** — the id→form map is cognition load, and key-masking was built for judgment stages; the collector has no verdict to bias. Mention = `{freehand location, verbatim snippet}`: location is advisory color (stated default preference: page by path or heading + the governing heading/section or structural kind); the authoritative page is CODE-DERIVED at the fold from the snippet's position vs URL lines. Hold policy: a response key matching no sent form is DROPPED and the tier-1 floor scan flags the resulting unaccounted hits (raising would replay a temp-0 mis-echo to death — the alecmfg shape). `record_id = hash(form)` survives per D11, minted at the fold, never on this wire. If Phase 5 shows the echo channel bleeding, the id-keyed variant is the A/B fallback (the old form-echo arm, inverted). **Wire-shape note (1.4, 2026-08-21):** the output is an ARRAY `{"forms": [{form, mentions: [...]}]}`, not an object keyed by form — the response_format is OpenAI strict mode, which forbids objects with arbitrary keys; the form is echoed as a field, the parse rebuilds the map and raises on duplicates. **AMENDED 2026-08-22: response = `{"mentions": [{mention_id, location}]}`; `location` is the LLM's own wording and extent (semantic role: testimonial / job posting / listing title / company claim / menu …; describe only, once per recurring line). Page, repeat count and nearest-heading facts are CODE's. Hold = warn-only both ways on ids; an undescribed snippet keeps its mention under `DEFAULT_LOCATION` (`location_source=none`).** |
| D7 | Mechanical floor scan, two tiers | Tier 1 (HOLD): exact-case word-boundary scan per sent form; every hit must be accounted for in the LLM's mentions or the window flags a discrepancy — the satisficing tripwire, zero GT cost. Tier 2 (DISCOVERY): case-insensitive scan; uncovered casings/near-forms feed the missed-form surface. Word-boundary ALWAYS (substring matching is the measured `Lead` 52-hits-0-real bug); short forms (≤3 chars, e.g. `Al`, `SS`) match case-sensitively or flagged. The standing brute word-boundary TODO graduates to load-bearing core. **Domain note (2026-08-21, follows the URL cut):** both tiers sweep window text MINUS URL lines — the scan domain must match the mention contract or every path hit is a phantom discrepancy. Measured basis (12 runs, 10,661 mention segments): 11 runs produced ZERO URL-occurrence mentions despite the v1 instruction; the one compliant run (20260814T203731) flooded 183 rows of sense-free URL mentions — dead weight either way. **AMENDED 2026-08-22: the scan IS the collector — tier 2's casings are collected as mentions of their family group (casing rescue, the mechanical replacement for recursive's one legitimate role; forms ≤3 chars stay exact). The hold moved from occurrences to LOCATIONS (`described`/`not_described` per window). Scan domain also minus EXCLUDED PAGES (P3, user-accepted): URL path matching privacy|cookie|terms|legal|disclaimer|gdpr|imprint|impressum — blanked for the scan, omitted from the search AND location wire (`floor_scan.wire_window_text`); measured 80k chars of legal text across alecmfg + steelcraft, 4% of collected spans.** |
| D8 | Code re-attribution | Aggregation re-keys EVERY mention from its verbatim snippet (checks which sent forms exactly occur in it); the LLM's attribution is advisory, the fold's is authoritative — model case-sloppiness becomes harmless. The longest-match containment rule lives HERE, in code, not in the prompt (`Sample Lead Time` attributed to sent form `Lead Time`, never also to `Lead`). **2026-08-22: with code collecting there is nothing to re-attribute; longest-span containment lives in `collect_window._owning_hits` over tier-2 hits, one mention per distinct span (two sent casings of one string yield one mention).** |
| D9 | Grouping | Pure code, runs at aggregation (AFTER mention collection — user preference: mention batching stays form-based, prompts stay flat/unnested), global scope (all windows, whole document). Normalization-KEY bucketing — groups are equivalence classes of `normalize()`, a dict, not union-find. Union-find machinery only becomes necessary for the future co-listing enhancement. |
| D10 | `normalize()` | L0 (casefold, strip ™®©, unify hyphens/dashes/slashes→space, collapse whitespace) + L1 (per-token plural lemma) baseline for ALL fields; L2 (verb/participle fold) as per-field dial for process/material fields only. Code-token guard: never lemmatize tokens with digits, internal capitals, or OOV-capitalized (protects invented product names; `AccuGrip`/`AccuGrips` stay separate). Real lemmatizer, never naive suffix-stripping (`Brass`→`bras`). Normalizer VERSIONED like a prompt catalog — an upgrade is a loud event. Measured on 9,577 real phrases: zero wrong merges (appendix). |
| D11 | Identity | Per-form `record_id = hash(form)` — mention provenance + mention-GT anchor, cross-run stable. `group_id = hash(normalized key)` — the wire key for everything downstream of aggregation; one key per group by construction, NO leader election, stable across chunks AND runs while grouping stays key-derived. The co-listing enhancement would break the one-key property and reopens the id question — that cost is booked against the enhancement, not v3. |
| D12 | LLM grouper REJECTED | Settled firmly. Blind semantic judgment (strings, no text); its over-merges are the measured M2 world-knowledge instinct ("X is a kind of Y", wrong when confident) landing upstream of every check; nondeterminism churns group_ids/digests and destroys A/B attribution; new GT surface. Escalation path if disjoint-form under-merge ever measures painful: LLM merge-PROPOSER — advisory pairs, logged, applied at concept-level reconcile where a wrong proposal cannot touch synthesis bundles. |
| D13 | Under-merge is the accepted steady state | Redundant groups cost one extra synthesis request and heal at reconcile (both ground to the same concept; screening's one-qualifying-record-suffices resolves split verdicts toward recall). A missed merge cannot cost a result. Every attempt to squeeze redundancy out with smarter form-matching converts cheap failures into expensive ones. |
| D14 | Polysemes | No search-side conditional disambiguation (can't see cross-window conflicts; brute bypasses prompts; mention stage re-merges anyway). A bare polyseme is ONE group with mixed mentions; ~~synthesis dispositions resolve it~~ **AMENDED 2026-08-21 (user decision, with D15): no synthesis dispositions — a mixed group yields a FAITHFUL mixed synthesis, and resolution moves to SCREENING, where field rule catalogs already live (D13's one-qualifying-record-suffices resolves toward recall). Side effect: the over-discount failure mode — dispositions discarding genuine evidence before screening saw it — is gone.** |
| D15 | Synthesis | Deposition-only: mention bundles, never source text (escape hatch: text can be ADDED to its input later if evidence demands). ~~Per-mention DISPOSITION slot; SPLIT-FLAG clause~~ **AMENDED 2026-08-21 (user decision): both REMOVED. Synthesis is a FAITHFUL AGGREGATION of all entries — the court-reporter rule applied to itself: discounting is adding judgment. The model is BLIND to forms and groups: a record is an opaque `record_id` + `entries: [{location, snippet}]`, sent as RECORD_IDS + RECORDS array blocks; output is an array mirroring the input (exactly-once by count). Non-judgment scaffolding kept: the organizing lens, worded as the closed catch-all "whatever the entries show the manufacturer does, has, or is, and whatever is said of it" (action / possession / state + attribution — covers certificates, industries, equipment that a verb list misses); NO convergence nudge (removed at user review — naming a shared subject invites predicting it; convergence is left to the structural fact that every entry contains a form of the group key), repeat-collapse (say once, note recurrence), one deposition sentence (you do not see the site), name-masking in output. Sense burden → screening (D14). The disposition design survives only as a Phase 5 A/B arm if screening measurably struggles with mixed syntheses. Missing-record policy on this wire = the USER-OWNED under-answer policy (same family as grounding's).** Batching: soft cutoff by total mention count; groups NEVER split; a monster group gets its own request; mentions-per-request recorded in dump stats (the satisficing A/B variable). **2026-08-22 NOTE (pending 3.2): the user intends to send synthesis ONE FOCAL FORM of the group and ask it to concentrate on that entity and shed noise — this REVERSES the court-reporter rule above; to be recorded as a decision when 3.2 builds it.** **DECIDED 2026-08-22 (user, this session): synthesis receives ONE FOCAL FORM per record (a representative member form of the group, chosen in code) and its Task becomes 'describe the focal entity using these entries as evidence' — the faithful-aggregation framing is SUPERSEDED for the Task; kept: attribution in the entries' own words, repeat-collapse, the one deposition sentence, name-masking in output, the blind record wire. `location` on the wire becomes an A/B ARM (with / without), run as two arms at 3.2.** |
| D16 | Downstream re-key | Grounding → OOV → screening → descent → reconcile consume per-group records keyed `group_id`. Upstream-content digests (`|ud=`) extend through the new chain: mention requests digest forms, synthesis digests bundles, grounding digests group records. **Naming note (2026-08-21):** the synthesis wire label `record_id` carries the group_id VALUE; code, dumps and GT keep the name `group_id` — the per-form `record_id = hash(form)` is a different key and must not be conflated. |
| D17 | No text preprocessing | No ASCII fold / character substitution (skipped as not straightforward; the curated-fold idea is shelved unless evidence demands). `ensure_ascii=False` STAYS — reverting was proposed and withdrawn against the measured evidence (escapes caused 17/17 echo corruption; real characters fixed it; on ASCII-clean text the flag is a no-op anyway). Normalize-at-comparison wherever a comparison crosses surfaces. |
| D18 | GT v3 | Mention-level GT is near-mechanical (verbatim fidelity + location), anchored `(record_id, window, mention_index)` — survives any grouping change. **Location note (2026-08-21, follows the D6 amendment):** the auditable location is the fold's code-derived page; the freehand location string is unaudited color. **Synthesis note (2026-08-21, follows the D15 amendment):** the synthesis audit is faithfulness-to-entries only — no disposition audit, no split-flag tally; wrong-merge visibility is the Phase 4.1 dump's member-forms column. The missed-MENTION surface becomes a missed-FORM surface per 5k window (tractable for an annotator; tier-2 scan feeds it). Synthesis audits key on `group_id` (run-scoped addressing accepted; cross-run joins via member-form overlap). Synthesis is auditable AGAINST ITS BUNDLE without opening source text. Annotator budget concentrates on synthesis. Audit primitives (TextFieldAudit/EntityFieldAudit, rule_tree, fold, inflation pattern) carry over. |
| D19 | Same-occurrence dedup | Cross-casing duplicates are impossible by construction (exact-case matching, D5). Containment overlaps resolved by longest-match in code (D8). Residual rule for two forms of one group reporting the same spot: keyed on (group, page, snippet overlap) at aggregation — exact rule OPEN, decide in Phase 2. **SETTLED 2.3 (2026-08-21, user-approved, built):** after re-attribution the unit is the OCCURRENCE — dedup key (group, occurrence span in the window); the longer snippet is kept; among equal snippets the collector's own filing wins, then lexical order (never answer order). `core/utils/aggregation_fold.py`; PIPELINE_V3_WALKTHROUGH_2_2.md §6 C. **2026-08-22: the occurrence is the unit by construction (code collects each span once); synthesis entries additionally dedupe by SNIPPET within a bundle (a repeated line reaches synthesis once, the per-occurrence mentions stay for the dump/GT).** |
| D20 | Measurement gates | v3 proceeds at full scope; the Phase 5 gate compares v3 against the v1 hand-audit baselines only (56% boundary precision, 45% context coverage — appendix C). There is no v2 comparison arm: the v2 gate was abandoned 2026-08-21 after both subjects crashed inside the stages v3 replaced (appendix D, condensed). |

**Open items (small, decide in-phase):** ~~D19's exact residual rule~~ (settled 2.3); ~~snippet extent
defaults~~ (settled in 1.2: sentence, or whole line for non-sentence text); the
soft-cutoff value for mentions-per-request; ~~short-form length threshold~~
(settled 2.2: ≤3 chars stay case-sensitive in tier 2, flagged);
~~lemmatizer selection + version pinning mechanics~~ (settled 2.1: lemminflect
0.2.3 dictionary-only, exact pin + import assertion + golden-digest tripwire);
group-aware dump row format;
~~**empty-bundle fate (Phase 2.3)**~~ (settled 2.3: a form with zero mentions
after the fold — search false positive, or swallowed by containment — keeps its
bundle with status `no_mentions`, is skipped by `synthesis_records()`, and stays
dump-visible).

**Standing watch items:** M2 match rules still owed; descent-attribution leak
channel (measure at gates); search-divisor recall/quality A/Bs still unrun;
**under-answer policy (USER-OWNED, appendix D; now also OBSERVED in the mention stage — run 20260822T061410, 2 of 30 forms answered, caught by the floor scan, warn-only):** a grounding response can validly answer a fraction of its sent
records and today that aborts the whole subject via `MissingResponseRecords`;
decide retry/split/schema policy before v3's grounding re-key (Phase 3.3).

---

## Phases

### Phase 0 — Predecessors (CLOSED 2026-08-21)
- **0.1** v2 flip review signed off; **0.2** the v2 measurement gate abandoned
  by user decision (both subjects crashed inside stages v3 replaced — appendix
  D, condensed; D20). **Gate resolved by decision, not numbers.**
- **0.3 Evaluation protocol — RETAINED for v3's Phase 5 gate:** dumps land in
  `packages/logs/extraction_dumps/<run_id>/` and are readable directly;
  baselines are appendix C; scoring methodology is the hand-audit write-up at
  `apps/data_etl_app/src/data_etl_app/services/ground_truth/MATERIAL_CAPS_STAGE_AUDIT_20260816.md`
  (boundary judgment, distinct-context clustering for coverage, per-stage
  attribution). **The USER supplies the arm mapping** per run id — subject,
  batching knobs, OOV on/off, model/temp/seed if non-default (usual: gpt-4.1,
  temp 0, seed 12345) — plus any notebook-side errors/aborts; dumps do not
  reliably self-describe these knobs.

### Phase 1 — Contracts on paper
- **1.1** Search statics reworked: flat form emission, fullest-span, all casings,
  no entity binding (D2). Propagate per the md5 discipline.
- **1.2** Mention-collector prompt: sense-agnostic occurrence contract, exact-case
  matching, extent rules, id-keyed output (D4–D6). Recursive-as-form-completion
  draft (D3).
- **1.3** Synthesis prompt: deposition framing, dispositions, split-flag (D15).
- **1.4** Wire shapes for all three + catalog/skeleton work in the established
  rule-catalog pattern.
- **REVIEW gate:** user signs off rendered prompts + wire shapes.

### Phase 2 — Pure core (all pure functions, property-tested)
- **2.1** `normalize()` per D10 + versioning; group_id/record_id derivations (D11).
- **2.2** Word-boundary matcher (technical-term tokenization, short-form policy) —
  the floor-scan engine (D7).
- **2.3** Aggregation fold: re-attribution, longest-match, same-occurrence dedup
  (D8, D19), bucketing grouper, bundle assembly with locked mention order.
- **REVIEW gate:** property tests (order-independence, normalization edges,
  short-form bridges) + a dry grouping run over the appendix corpus.

### Phase 3 — Node services + DAG
- **3.1** Mention node: flat payloads, floor-scan hold (tier 1), discovery stats
  (tier 2).
- **3.2** Synthesis node: soft-cutoff batching, groups whole, ~~dispositions parse~~ one synthesis per record with a FOCAL FORM (D15 as amended 2026-08-22), `location` as an A/B arm, snippet-radius knob on the collector. **DONE 2026-08-22 (journal row '3.2 BUILT'; retry pass included).**
- **3.3** Downstream re-key onto group_id; digests through the new chain (D16);
  orchestration order search → recursive → mentions → [fold] → synthesis →
  grounding → OOV → screening → descent → reconcile.
- **REVIEW gate:** end-to-end dry run on a small subject.

### Phase 4 — Observability
- **4.1** Group-aware dumps: one row per group, member forms visible inline (a
  wrong merge must be human-visible at a glance); floor-scan discrepancy stats;
  mentions-per-request stats; never-asked vs asked-found-nothing preserved.
- **4.2** Subject-name lint over snippets and synthesis.
- **REVIEW gate:** user reads a dump.

### Phase 5 — Measurement gate (before GT)
v3 vs the v1 baselines (56% boundary, 45% coverage — appendix C; no v2 arm
exists, D20 amended) on alecmfg + steelcraft via the user's notebook. Floor-scan
tripwire rates. Recursive-as-form-completion A/B (D3 final call). Soft-cutoff
sweep (D15). If loops resurface in the mention collector, A/B the mention output
with/without `page` here (the sub-URL hypothesis from appendix D). If v3 loses,
iterate here — GT waits.

### Phase 6 — GT rebuild (absorbs v2 plan Phase 5)
Stage blocks v3 per D18: form-coverage surface per window, mention audits
(near-mechanical), synthesis audits (dispositions + faithfulness-to-bundle), run
identity, inflation, submission validation, fold, routes, fresh API test plan.

---

## Evidence appendix (measurements this design rests on, both 2026-08-21)

### A. Anaphora layer in v1 relationship evidence
Run `20260816T030947` (alecmfg, gpt-4.1, the hand-audited run), all 7 phrase
fields: 1,535 phrase-records, 1,804 `Mention:` segments. Does the phrase occur in
its own quoted evidence (casefold, word-boundary, dash/™-normalized)?
- Mention-level: **93.5% verbatim**, 2.2% head-token-only (variant form),
  **4.3% absent**. Record-level (best mention): **98.3% / 0.9% / 0.8%**.
- The absent bucket is mostly INFLECTION, not anaphora (`Anodizing` anchored at
  "Clear anodized (Type II)", `prototyping` at "initial prototype") — exactly what
  form discovery covers. True anaphora (`aluminum parts` evidenced by "The parts
  look clean…") is a handful of cases in 1,804.
- Caveats: measures what the satisficing v1 collector CHOSE to write, not a census;
  products/contract_products share records (double-counted; percentages unmoved).
- Consequence: D4's justification is extent-flexibility, not anaphora.
- Script: `anaphora_measure.py` (session scratchpad; methodology above suffices to
  recreate).

### B. Normalization-key grouping on real search output
All 11 dump runs, 8 subjects, **9,577 unique (field, phrase) pairs**, bucketed
within (field, subject) — the grouper's real scope — under three strengths:
- Token distribution: 10% single-token, 24% 2-tok, **37% 3-tok (mode)**, 30% 4+.
  Compounds are the norm; per-token folding is the main case.
- **L0 (case/punct/™): 204 multi-member groups** (210 phrases folded) — the bulk of
  real variance (`Fire Rated`/`fire rated`/`fire-rated`, `GRAINTECH`/`GRAINTECH™`).
- **L1 (+plural): 247 groups** (261) — adds real pairs: `electrical code(s)`,
  `steering column(s)`, `batch delivery/deliveries`, `assemblies/assembly`.
- **L2 (+verb fold): only 6 additional groups**, all process-flavored and
  defensible (`CNC milled/milling`, `Polished/Polishing`, `tested/testing …`) —
  confirms L2 as per-field dial, near-zero cost if dropped.
- **Wrong merges: ZERO** at every level, under rule-based folds CRUDER than a real
  lemmatizer (over-merging relative to production — conservative validation).
  Closest call: `metal stamping`/`metal stampings` (process vs its output parts) —
  defensibly one group; grounding judges downstream.
- Incidental: Spanish phrases exist in production data (austinelectricservices
  process_caps) — English lemmatizer passes them through = safe under-merge;
  supports D17. Boundary noise is visible in groups (`BEARING` as material) —
  grouping merges variants of junk like variants of good; screening owns boundary.
- Script: `normalize_groups.py` (session scratchpad).

### C. Standing v1 baselines (from the material_caps hand audit, run 20260816T030947)
56% boundary precision · 45.2% distinct-context coverage (137/303) · mentions flat
at ~1.7 regardless of context count (100% coverage at 1 context, 9.8% at 10+) ·
`Lead` brute-substring 52 hits/0 real · recursive round 2: 0/12 in-boundary at
highest input cost · relationship windows complementary not redundant (21.9%
quote overlap, union 1.39× best window) · 5k-window simulation: −27% input tokens,
output ~2.2× if coverage reached all contexts.

### D. v2 Phase-4 gate crash (run 20260822T012722, 2026-08-21) — condensed
Why there is no v2 comparison arm (D20). Both gate subjects failed at factory
defaults (gpt-4.1, temp 0, seed 12345, group size 30), inside stages v3 replaced:
- **steelcraft — relationship repetition loop:** one identical mention block
  repeated to the 20k completion cap, JSON truncated mid-string. v2's output
  demand was proportional to every mention of 30 phrases over a ~21k-token
  window — unbounded; v3 bounds it (5k windows, D1/D5) and D7's floor scan
  catches degeneration mechanically. Cheap Phase 5 A/B if loops resurface in
  the collector: mention output with/without the page/location field.
- **alecmfg — grounding UNDER-ANSWER:** a clean, short response answering 1 of
  30 sent records (`finish_reason='stop'`) → `MissingResponseRecords` killed
  the subject. **Survives the pivot** (the wire schema doesn't force one entry
  per sent record); policy is USER-OWNED — see standing watch items.
- Stored bad responses replay identically; the notebook's scoped delete is the
  remedy. Log: `apps/data_etl_app/src/data_etl_app/scripts/latest_extraction_error.txt`;
  dump dir `packages/logs/extraction_dumps/20260822T012722/` (single-stage fields only).

### E. Normalizer dry run (2026-08-21, substep 2.1) — lemmatizer selection
Every distinct phrase in all 12 dump runs, bucketed within field: **4,157
(field, phrase) pairs, 7 fields, 1,888 distinct alphabetic tokens.** Three
candidates measured first per token, then per phrase (multi-member groups each
layer creates, listed in full so a wrong merge is visible — the appendix-B
method). **Saved in-repo:** the tool `pipeline_v3_evidence/normalize_dry_run.py`
(runs the PRODUCTION normalizer over the dump corpus) and the full per-layer
listing `pipeline_v3_evidence/2026-08-21_normalize_dry_run_output.txt`.
- **simplemma 2.0.0 — REJECTED:** restores case (`texas`→`Texas`) and folds
  inconsistently (`bearing`→`bear` but `bearings`→`bearing`, so singular and
  plural land in different keys); no POS dial, so the verb fold cannot be
  confined to process/material.
- **lemminflect 0.2.3 OOV rules — REJECTED:** naive suffix stripping in
  disguise (`continuous`→`continuou`, `abs`→`ab`, `as`→`a`, `kansas`→`kansa`,
  Spanish mangled), 381 token changes vs 245 for dictionary mode — D10's
  "never naive suffix-stripping" failure exactly.
- **lemminflect 0.2.3 DICTIONARY mode — CHOSEN:** clean on the traps (`brass`
  stays, `stainless`/`series`/`chassis` untouched, `bearing`/`bearings`
  consistent); misses domain plurals the dictionary lacks (`stampings`,
  `forgings`, `weldments`, `counterbores`), recovered by ONE guarded fallback
  (≥5 letters, alphabetic, ends `s` not `ss/us/is/ous`, unknown under every
  POS) that changed 67 tokens: English domain plurals, Spanish plurals (mostly
  correct Spanish; the wrong ones collide with nothing), three proper nouns
  (`douglas/kansas/texas` — the capitalized-OOV guard covers their real
  spellings).
- **Phrase-level groups:** L0 **162** → +L1 dictionary **189** (+79, all
  legitimate: `Plastic/Plastics/plastic/plastics`, `assemblies/assembly`,
  `medical device(s)`) → +fallback **191** (+9: `metal stamping(s)` — appendix
  B's closest call — `baseplates`, `stamping(s)`) → +L2 on process/material
  **198** (+21: `CNC milled/milling/mills`, `Polished/Polishing`, `surface
  finish(ing)`, `3D Printing`→`3d print`). **Wrong merges: zero at
  L0/L1/fallback; one debatable at L2** — `injection molds` ↔ `Injection
  Molding` in process_caps (a mixed group synthesis + screening can resolve,
  not a wrong key). L2 off elsewhere because it only uglifies noun modifiers
  (`mounting brackets`→`mount bracket`).

---

## Journal (append-only)

| Date | Row |
|---|---|
| 2026-08-21 | Plan created after multi-session design review of pipeline_v3.txt. Ledger D1–D20 locked in chat. Evidence appendix measurements A (anaphora 93.5/2.2/4.3) and B (normalization grouping, 0 wrong merges on 9,577 phrases) run this day. V2 flip signed off (three deviations approved); v2 Phase 4 gate handed to the user's notebook — its numbers are v3's Phase 0 precondition and comparison baseline. |
| 2026-08-21 | **Phase 0 CLOSED — v2 Phase-4 gate ABANDONED by user decision; PIVOT TO V3 CONFIRMED.** The gate run (20260822T012722) crashed on both subjects inside stages v3 replaces: steelcraft — 20k-token repetition loop in v2 relationship (identical mention block repeated to the cap, JSON truncated); alecmfg — freehand grounding answered 1/30 records → MissingResponseRecords (appendix D). Rationale: don't repair a stage v3 deletes; v2's relationship output demand is structurally unbounded vs v3's bounded 5k windows. D20 amended — Phase 5 compares v3 vs v1 baselines only (no v2 arm will exist; user agreed). Grounding under-answer carried into v3 as USER-OWNED watch item (it survives the pivot). V2 plan STATE closed with pointer here. Next: Phase 1.1. |
| 2026-08-21 | **1.1 built:** six stage-1 search statics reworked for D2 (surface-form contract in Task, fullest-span Extent, Dedup→Variants inversion; What-qualifies/Rejection untouched). Propagated industry→other five, section-level md5 verified (intro/Variants/Output identical; Task/Extent/Rejection match modulo gloss). Prompt tests green (246). Unpublished — pins drift until the Phase 1 review gate. Open for user audit: the fullest-span rendering; stage-2 recursive statics still carry the old variant-folding dedup until 1.2 rewrites them as form completion. Next: 1.2. |
| 2026-08-21 | **1.2 built:** mention-collector statics created (new dir `3_phrase_mention_collection/`, six byte-identical, field-agnostic) on the <<<RECORD_IDS/<<<RECORDS wire with {page, snippet} mentions, id-keyed total output, no form echo; recursive statics reworked in place as form completion (six byte-identical, field-agnostic, no-new-entities Rejection, fences/names kept). Prompt tests green (246). Decided + flagged: snippet extent = sentence-or-line (whole-bullet-plus-heading rejected for breaking contiguous verbatim); no name-masking in snippets (masking moves to synthesis + Phase 4.2 lint); boilerplate repeats each get a mention (loop-degeneration watch at Phase 5). Unpublished. Next: 1.3. |
| 2026-08-21 | **1.2 amended after user review (D5/D6 amended, D18 noted):** record ids off the mention wire (bare forms sent, form-string keys returned under "mentions"; ids survive code-side per D11, minted at the fold); mention = {freehand location, verbatim snippet} — location is advisory with a stated default preference (page + governing heading/section), authoritative page is code-derived from snippet position; unmatched keys drop + tier-1 scan flags; URL-line occurrences kept with the URL line as snippet; example shows multi-mention arrays. Six statics re-propagated byte-identical; prompt tests green. Rationale recorded in D6: key-masking was built for judgment stages; the floor scan is the loss catcher v2 lacked. Next: 1.3. |
| 2026-08-21 | **URL-domain cut (user decision, D2/D7 noted):** URL lines out of the harvest/match domain across all 18 statics — search+recursive drop "count as text too"+heading gloss, mention statics drop the URL-occurrence clause and URL-line snippet case; heading gloss survives only in the mention prompt (feeds location); floor scan domain = text minus URL lines, both tiers. Measured: 11/12 runs zero URL-occurrence mentions despite instruction; the compliant run (20260814T203731) flooded 183 sense-free URL mentions; and v3 exact-case matching dead-ends slug forms regardless. Empty-bundle fate → Phase 2.3 open item. Tests green (234). Next: 1.3. |
| 2026-08-21 | **1.3 built:** synthesis statics created (new dir `4_phrase_synthesis/`, six byte-identical, field-agnostic). Deposition-only framing (model never sees the site); bundles on a <<<GROUP_KEYS/<<<GROUPS two-block wire keyed by the NORMALIZED KEY STRING (group_id stays code-side — mirrors the D6 amendment; flagged for audit vs D16's wording); per-mention dispositions {mention, use: relied on/discounted, why} with optional machine-countable "different_thing": true as the split-flag; all-discounted case states the mentions show nothing (D14 polyseme resolution); v1 name-masking ported into synthesis output per the 1.2 decision (input unmasked, output masks). Prompt tests green (234). Phase 1 prompt drafting complete — next: 1.4 wire shapes + catalogs, then the Phase 1 review gate. |
| 2026-08-21 | **1.3 amended after user review (D14/D15 amended, D16/D18 noted):** dispositions + split-flag REMOVED — synthesis is a faithful aggregation of all entries (court-reporter rule applied to itself); sense burden moves to screening; model blind to forms/groups: record = opaque record_id (group_id value; code keeps the name) + entries [{location, snippet}], RECORD_IDS + RECORDS array blocks, output array mirrors input. Vocabulary record/entry/snippet/location (entry replaces excerpt; snippet/location stay = mention-stage names). Kept as non-judgment scaffolding: activity lens, one convergence nudge, repeat-collapse, deposition sentence, output name-masking. Dispositions = Phase 5 A/B arm only if screening struggles. Six statics byte-identical; tests green (234). Phase 1 prompt drafting complete — next: 1.4. |
| 2026-08-21 | **1.3 lens + nudge fix (user review):** synthesis lens reworded from the open verb list ("does, makes, offers, works with, or is said to" — missed certificates/industries/equipment) to the closed catch-all "whatever they show it does, has, or is, and whatever is said of it"; the convergence-nudge sentence removed (naming a shared subject invites predicting it — the synthesis just weaves entries). Six statics byte-identical; tests green (234). Phase 1 prompt drafting complete — next: 1.4. |
| 2026-08-21 | **1.4 built:** wire contracts for mention collection + synthesis — new `extraction_schemas/mention_collection.py` and `synthesis.py` (strict response_formats, parsers, dummies); `phrase_blocks_contract.py` v3 section (EXACT `hold_response_to_sent_forms`, `render_synthesis_record_blocks` = RECORD_IDS + ARRAY records, record-id hold generalized to array payloads). Wire-driven prompt amendment: mention Output → array `{"forms": [{form, mentions}]}` because strict mode forbids objects keyed by arbitrary strings; six statics re-propagated. Two new test files; the drift test caught a real bug in the consistency-error message (fixed). Full suites green (core 323, app 402), pyright clean. Not done by design: PromptService registration (post-publish), PipelineStage members (Phase 3), catalogs (field-agnostic statics take the static path). **Phase 1 substeps complete — next: the Phase 1 REVIEW gate.** |
| 2026-08-21 | **Phase 1 REVIEW gate PASSED** (user sign-off on 24 statics + wire contracts). Publish DEFERRED to Phase 3.1 by decision: publishing the reworked search/recursive statics now would flip the live search stage under v2's downstream; nothing in Phase 2 needs S3; drift is runtime-safe. |
| 2026-08-21 | **2.1 built:** `core/utils/form_normalizer.py` + 80 tests; core suite 403 green; pyright clean. Lemmatizer decided by measurement on 4,157 real (field, phrase) pairs (appendix E): lemminflect 0.2.3 DICTIONARY-only (simplemma and lemminflect's OOV rules rejected), exact pin + import assertion, one guarded OOV-plural fallback, code-token guard with acronym-plural exemption, L2 verb fold as a bool dial applied after L1 (field mapping app-side at Phase 3), `group_id` = g+7 base36, NORMALIZER_VERSION "1" with golden-digest tripwire, hypothesis properties (caught two Unicode edges in the TEST alphabet, not the code). Groups 162→189→191→198, zero wrong merges at L0/L1/fallback, one debatable at L2. `hypothesis` added as core dev dep. Next: 2.2. |
| 2026-08-21 | **2.2 built:** `core/utils/floor_scan.py` + 27 tests; core suite 430 green; pyright clean. Word-boundary matcher for technical terms (edge guards only where the form's edge is a word char — 6061-T6, CNC/Manual, C++, the Lead/Leader case), length-preserving page-header masking as the scan domain (URL + `#` separator lines, per the URL cut), `page_spans`/`page_at` for code-derived page attribution, two tiers (exact = hold, case-insensitive = discovery), short-form threshold settled at 3 (stay case-sensitive, flagged). Hypothesis properties: whole-word exact hits, tier2 ⊇ tier1, sorted/non-overlapping, inserted-form-always-found, mask length + page tiling. Next: 2.3 fold. |
| 2026-08-21 | **Resume aids saved (user request):** `PIPELINE_V3_WALKTHROUGH_2_2.md` (2.2 mechanics + the 2.3 design with real outputs) and `pipeline_v3_evidence/` (normalize_dry_run.py + 2026-08-21 output = appendix E tool; fold_prototype.py = the 2.3 seed). 2.3 design recorded in STATE as PROPOSED (A–E: locate, re-attribute with longest-match, D19 dedup keyed on occurrence span with longer snippet kept, bundles in locked order with empty bundles kept, tier-1 obligations under the same containment rule) — awaiting the user's go-ahead. Committed 2.2 + these files on top of ca9a82e. |
| 2026-08-21 | **2.3 built (user go-ahead on the proposed A–E):** `core/utils/aggregation_fold.py` + 25 tests; core suite 455 green; pyright + ruff clean. The walkthrough §6 scenario reproduces exactly and is the golden test. Three refinements found while building, all recorded in STATE for the gate review: attribution reads the tier-1 scan (owners inside the located snippet) instead of re-matching inside the snippet string; containment applied window-wide across snippets; D19 tie prefers the collector's own filing. D19 SETTLED; empty-bundle fate SETTLED (kept, `no_mentions`, skipped by synthesis, dump-visible). Next: Phase 2 REVIEW gate (question in STATE), then 3.1. |
| 2026-08-21 | **Phase 2 gate PASSED; 3.1 built (user: replace relationship now; per-chunk fold scope; brute casings as forms; fold identity in metadata; I publish).** Mention node + per-chunk fold in all 7 phrase chains, relationship out (v2 tail unreachable behind a named context error until 3.3); `PipelineStage.mention_collection`; `BatchedMentionCollectionNodeMetadata` + `AggregationFoldMetadata` (Optional on the v2 metadata model); `llm_phrase_mention_req_ids` per sub-window; `brute_by_sub_bounds` at prefill; service (window-local forms, groups, fenced context, exact warn-only hold, `get_chunk_fold`); partial dump `fold` block + request witness; factory/PromptService wiring. 24 statics published + pinned (search/recursive reworked, mention, synthesis); `check` clean. Core 472 / app 406 green; pyright delta = pre-existing families only; ruff clean. 2.1 finding: `normalize` idempotent only for un-guarded forms (documented boundary; no code change). v2 doc noise trimmed in this plan. Next: 3.2 synthesis node. |
| 2026-08-21 | **First v3 run analyzed (20260822T061410) + D2 Extent reverted.** Both subjects, 7 phrase fields, `stop_after(mention_collection)`, zero errors; fold blocks + witness in every dump. Findings in STATE (3.1 follow-up): long forms from "fullest span" (15% / 33% ≥6 words first/recursive; max 147 words; 9 across line breaks; 65% of ≥6-word process forms empty; swallowed sub-forms dominate rekeys) → shortness Extent restored + line-break ban in all 12 search/recursive statics, recursive no longer asks for fuller spans; published + pinned. Satisficing 4–35% unaccounted; one mention-stage under-answer (2/30) caught by the floor scan; 16 recursive loops salvaged. Next: user re-runs; 3.2 synthesis node. |
| 2026-08-21 | **Extent parenthetical dropped (user review of the revert):** "(a grade, a series, a trade name, a standard's number)" removed from the modifier clause in all 12 search/recursive statics — not field-neutral; the model judges what is part of the name. Published + pinned; `check` clean. Same review raised: (1) the 2/30 under-answer is a detection win, not an output win — under-answer policy still user-owned, options tabled in chat (mechanical backfill from tier-1 hits vs LLM retry of unanswered forms); (2) the collector SEES URL lines (masking is the scan domain only, D7) and names the page by URL in 66% of `location` strings, 13% of which disagree with the code-derived `page` (repeated boilerplate) — proposal: `location` = the spot on the page only, `page` stays code's; (3) synthesis statics should re-focus on the field with its catch verbs (3.2 input). |
| 2026-08-22 | **User review round 2 — measured (evidence in `pipeline_v3_evidence/2026-08-22_mention_stage_underanswer/`).** (1) Claim "closely resembling forms cause eager stops": TESTED, NOT SUPPORTED as the cause. Over the 183 real mention requests, 91% of forms with an exact hit got ≥1 mention; only 2/183 answers returned fewer forms than sent (steelcraft process_caps 102672:126084 g0 = 2/30; steelcraft industries 0:23757 g0 = 29/30). Skip ratio correlates +0.42 with same-normalize-key pairs but the controlled replays break the link: the 2/30 request replayed EXACTLY (same body, temperature 0, seed 12345) returns 30/30 forms, 27/27 with-hit forms answered, 39 mentions, 2,701 tokens — the original 170-token answer was a NON-DETERMINISTIC eager stop (seen once more: one 15-form arm returned 10/15 on its first call and 15/15 on its second). Reversing the forms, collapsing near-duplicates, or halving the group changes nothing material (25–27/27). The other bad group (alecmfg material_caps 82700:98612 g0 = 3/21) is DETERMINISTIC (replay identical, near-dup removal identical, capitalized-only 2/13): the model skips occurrences that sit in repeated blog-listing/heading lines ("Precision Manufacturing Case Study: Aluminum Alloy Sheet Metal Structural Housing …" on three index pages) and answers only the case-study prose — a retry cannot fix it, only backfill can. (2) Instrumentation gap: `ChatCompletionChoice.finish_reason` is not persisted (commented out in `gpt_batch_response_blob.py`), so stop-vs-length cannot be told from Mongo; replays all report `stop`. (3) Backfill prototyped on the real 2/30 window: 35 unaccounted hits → 35 code mentions (snippet = the sentence-clipped line holding the hit, median 121 chars vs the collector's 226; page = code's; location = a fixed marker; `source=floor_scan`). (4) Retry design tabled: groups already exist (`split_into_form_groups`, `group>{i}` in the custom id); a retry is a second embed→create→dispatch pass in the mention node modelled on `BaseLLMRecursiveExtractionNode.execute`, keyed `…group>{i}>retry>1…`, stored alongside the group ids in `llm_phrase_mention_req_ids[sub_bounds]`, triggered when a group's answer returns fewer forms than sent OR leaves >80% of its with-hit forms unanswered; fold treats retry answers as one more candidate source. (5) Mention statics: URL-path-as-heading hint and "prefer to name the page by its path" removed (user: unnecessary); `location` now borrows the relationship static's how-it-sits list (heading, menu item, list entry, table cell, prose, testimonial, blog item, job posting, footer, boilerplate). Published + pinned; `check` clean; app tests 406 green. (6) Field-focused synthesis (#4) PARKED until the current synthesis static has been seen in action — user concern: overlap with grounding. |
| 2026-08-22 | **Mention-stage broad audit (user ask; scripts + outputs in `pipeline_v3_evidence/2026-08-22_mention_stage_underanswer/`).** Run 20260822T061410, 183 real requests, 3,789 sent forms, 4,587 collector mentions, 1.03M prompt / 345k completion tokens. FINDINGS: (a) **Recursive round 0 runs even when the window's search found nothing** (`embed_request_ids` seeds round 0 for every sub-window) — 2 windows this run, fence `[]`, the model returned 42 phrases of page-menu junk (alecmfg conformity: 37 materials/processes → 2 mention groups, 40+ junk conformity groups in the fold). DEFECT, fix = dummy-complete the round when the listed set is empty. (b) **Recursive output is only 65% verbatim** (22% occurs nowhere in its window, 12% casing variants; process_caps 41% non-verbatim: "CNC is ideal for projects", "repeatable") vs search 97% exact — D3's Phase-5 data; 11% of all sent forms (406) have zero exact hits in their window (230 nowhere in the subject text, 160 casing variants, 10 elsewhere in the chunk), and the collector gave 192 of them mentions anyway (189 loose-casing, 79 snippets not holding the form) — all discarded by the fold, pure waste. Proposed: mechanical prefilter at prefill (a form with no tier-2 hit in the window is not sent) + casing rescue (send the tier-2 casing instead). (c) **Satisficing is frequency-shaped**: recall of exact hits 93% for single-hit forms, 57% for 2–4 hits, 39% for 5–9, 47% for 10–24 — backfill's case. (d) **Window re-send cost**: 46/105 windows need >1 group (max 8 — alecmfg process_caps 0:24296 with 225 forms); the window text re-sent for extra groups is ≈443k prompt tokens, 43% of the stage's input. (e) **Snippet extent**: 16% of snippets are multi-line (menus copied whole; max 5,196 chars), 13% of located snippets run beyond the occurrence's line; output chars are 60% snippets / 32% locations / 8% echoes (locations median 91 chars). Proposed: fold-side clip of every located snippet to the sentence/line of the occurrence (same clipper as backfill) — shrinks synthesis input and the dump, zero LLM cost. (f) **Double counting in overlaps**: sub-windows overlap (360k chars run-wide) and chunks overlap (220k) — the same occurrence is an obligation in both and lands twice: 297 duplicate occurrences within chunk folds (overlapping sub-windows; 5.8% of mentions) and 87 occurrences present in BOTH chunks' folds (chunk overlap) — the two chunk folds are synthesized separately, so those 87 groups' occurrences are synthesized twice (products/contract_products carry the most). Fold dedups per window only, synthesis is per chunk → duplicate mentions reach synthesis and across chunks the same group is synthesized twice. Needs a decision (dedup by absolute span at chunk level; overlap ownership rule across chunks). (g) Clean: 0 echo mismatches, 0 parse errors, 48 'unknown' locations all genuinely mid-page, true duplicate snippets only 33 (636 identical repeats are real distinct occurrences), 146 out-of-order events, bare-form snippets 276 (9 inside a sentence). |
| 2026-08-22 | **User decisions after the audit: recursive search OFF, overlap 0; next run requested.** `DEFAULT_RECURSIVE_SEARCH_MAX_ROUNDS = 0` (factory) and `wide.overlap = 0` (chunking_strat) — D3 verdict and D2 geometry amended with the measurements. Two more measurements behind the decisions: (i) of the recursive round's 770 window-new phrases only ~235 occurrences (10% of what search already carried) came from its intended form-completion role; (ii) the collector does NOT build a unique representation of a frequent form — for forms with ≥3 exact occurrences it covers 53% (3–4 hits) / 36% (5–9) / 33% (10+) of the DISTINCT contexts (distinct sentence/line texts), while 29–37% of what it reports repeats a context it already reported; backfill covers the misses, the fold's clip + synthesis's "say once that it recurs" handle the repeats. Suites 924 green, pyright clean. Mention identities change (`|ud=` digests differ without recursive forms; window bounds differ with overlap 0) — the next run re-searches and re-collects; search requests for the new bounds are new too. |

| 2026-08-22 | **Second v3 run analyzed — 20260822T195947 (recursive OFF, overlap 0, shortness Extent, reworked mention location) vs 20260822T061410; evidence `pipeline_v3_evidence/2026-08-22_run_195947_vs_061410/` (README has the full table).** Identity: every search/mention request carries a new `pv=` id — nothing replayed; Mongo holds only the new run (the old one was scoped-deleted), the old request-level baseline survives only in the outputs. WHAT THE DECISIONS BOUGHT (14 dumps, prev → new): unique forms 3,582 → 3,241; ≥6-word forms 731 → 374 (max 147 → 25 words; 9 → 0 across line breaks); ≥6-word EMPTY forms 197 → 21; empty groups 657 (18%) → 289 (9%); rekeyed 2,157 → 981 (swallowed 1,081 → 451, casing 533 → 279); zero-exact-hit sent forms 406 (11%) → 103 (4%) (nowhere-in-subject 230 → 32, casing variants 160 → 70); loose/hallucinated mentions on them 192 → 82; overlap duplicates 297 within-chunk / 87 across-chunk → 0 / 0; recursive loops 16 → 0, search loops 0, unparsed 5 → 0; tokens (dump witnesses) in 2.31M → 1.62M (−30%), out 492k → 419k (−15%), wall 2,454 → 2,005 s; mention requests 183 → 143 real (+10 dummies), re-sent window text 443k (43%) → 328k (40%); search 97% → 96% exact (72 casing-only, 32 not-in-window, 124 within-window duplicate phrases — harmless). Mentions flat (5,137 → 5,108) on MORE obligations (5,928 → 6,130); snippets better (median 147 → 138 chars, max 5,196 → 1,973, multi-line 16.0 → 11.6%, beyond-line 13 → 9%, true duplicates 33 → 14). WHAT GOT WORSE: (1) **unaccounted 791 (13%) → 1,022 (17%)** (satisficing 615 → 790, fully-missed 176 → 232); alecmfg material_caps 35% → 60%, equipments 15 → 28%, process_caps 18 → 25%, steelcraft material_caps 26 → 36% (steelcraft process_caps 24 → 14%, conformity 4 → 2%). Measured cause: the shortness rule yields short generic names (Aluminum, Copper, Alloy, Steel, Titanium) whose exact hits sit in repeated case-study LISTING TITLES ("🇫🇷 French Rail Equipment Manufacturer | Custom Aluminum Machining …") and the collector answers the specific forms and skips those lines — miss rate by line type, alecmfg material_caps: repeated heading/list 53% → 86%, prose 26% → 66%; all fields: prose 12 → 17%, repeated heading 18 → 25%. NO eager stop this run (143/143 answers returned every form; with-hit forms answered 91 → 93%) — the deterministic listing-skip family is what remains, plus legitimate polyseme declines the floor counts as misses (`Lead`/`lead` = lead time, brute). Frequency shape unchanged: recall 95/61/41/39% for 1/2–4/5–9/10–24 hits; context coverage 56/36/26% of distinct contexts. (2) `unanchored` 564 → 1,065 positions (excl. contract_products) is NOT a regression: distinct (form, snippet) pairs fell 200 → 156; the top-5 pairs cover 567 — a bare `Steel` snippet located at every `Steel` substring incl. `Steelcraft` (×343, boundary-rejected correctly; note a bare-form snippet anchors EVERY exact-case occurrence in the window — a wildcard), three footer-junk snippets filed under `steel` × 56 footer repeats (168), `Products ` menu ×56; casing-only (Title-Case occurrence filed under the lowercase form) 315 → 222. The counter multiplies by line repetition. (3) **NEW JUNK SOURCE — steelcraft's privacy-policy page** (offsets 63,831–98,888, 35k chars ≈ 18% of its covered text): forms whose mentions sit ONLY on legal/careers/cart pages 63 → 103 (process_caps 10 → 62: 'verifying', 'processing', 'delivering orders', 'Research and development'; products 3 → 21: 'orders', 'cookies', 'API') — shortness split 10 long legal claims into 62 generic words; the two windows fully inside it cost ≈ 80k in / 12k out tokens (mention 5%, search 8%) for nothing. (4) Overlap-0 side effect: the sub-window divider leaves REMAINDER windows (alecmfg 386 chars, steelcraft 775) → 12 search requests (9.4k prompt tokens, 0–2 phrases) + 10 mention dummies. (5) Mention `location` after the rework: median 91 → 106 chars, 39% of output chars (was 32%), 69% carry the page URL the fold derives anyway, 390 start "On the page". PROPOSALS (none built; user decision): P1 backfill (prototyped) — now the dominant loss; P2 casing rescue at prefill (222 casing-only unanchored + 70 casing zero-hit forms + 453 discovered casings); P3 legal/privacy PAGE exclusion — mask the page like URL lines (URL pattern + content cue; mechanical) rather than a prompt rule; P4 drop the page from `location` (code-derived) — ~25% of output tokens; P5 merge a trailing remainder sub-window below ~20% of the target size into its predecessor; P6 dump `unanchored` by distinct (form, snippet) pair with counts; P7 fold-side snippet clip (still open). Next: decisions on P1–P5, then 3.2. |
| 2026-08-22 | **User proposal: mechanical mention collection + LLM Location stage (Search → code: casing-expand, Ctrl+F, clip, dedupe → LLM Location [semantic role per mention, same 5k window, wire `[{mention_id, mention}]`, no forms] → Synthesis with a focal form).** Rationale agreed in chat: the fold already discards any LLM mention whose snippet lacks the form, so the collector's accepted output is a SUBSET of the exact-match scan — the LLM only adds location/extent/declines; satisficing (17% unaccounted) cannot lose data once code collects. Split agreed: page + repeat count + nearest heading are CODE; the LLM describes only the semantic role (testimonial / job posting / listing title / claim…). MEASURED on run 20260822T195947, 86 windows (`pipeline_v3_evidence/2026-08-22_run_195947_vs_061410/mech_collect_measure.py`): sent forms 2,904 (33 zero-hit, dropped free); LLM raw mentions 3,851 / fold kept 3,636 (unaccounted 840 = 19%); mechanical tier-2 distinct spans 4,907 (100% of occurrences) → 3,989 after (casing family, clipped snippet) dedupe → 2,880 distinct-snippet wire items; clipped snippet median 112 chars, max 623 (LLM 138 / 1,973); items/window median 23, p90 81, max 125; downstream snippet chars 748k → 487k (−35%) while covering +1,271 occurrences; Location-stage estimate at 50 items/request: 108 requests, ≈690k prompt (85% of today) / ≈115k completion (41%) at 40 out-tok/item; at 100: 70% / 41%; privacy-page spans 203/4,907 (4%). Flags raised: (i) the focal-form + drop-noise ask in synthesis REVERSES D14's court-reporter rule — record it as a decision if taken; (ii) Location stage is the speculative half (3.2 synthesis not yet seen running); mechanical collection is justified by measurement regardless; (iii) P3/P5 matter more, not less, since code collects faithfully. Subsumes P1, P2, P7; P4 moot (page is code's). DECISION PENDING. |
| 2026-08-22 | **BUILT — mechanical mention collection + LLM Location stage (user decision; D4/D5/D6/D7/D8/D19 amended above, D2 P5, D15 note). Suites 902 green (core + app), pyright delta clean, six Location statics published + pinned (`check` clean).** Code: `core/utils/floor_scan.py` (excluded pages: `is_excluded_page`, `excluded_page_spans`, `mask_excluded_pages`, `scan_domain`, `omit_excluded_pages`, `wire_window_text`; scan domain minus excluded pages); `core/utils/aggregation_fold.py` REWRITTEN (`collect_window` → `WindowCollection{mentions, items, discovered_casings, excluded_pages}`, `CollectedMention`, `fold_window` attaches `locations_by_mention_id` with `DEFAULT_LOCATION`/`location_source`, `WindowFold{collection, described, not_described}`, `FoldedMention{mention_id, location_source, sent_form}`, `MentionBundle.synthesis_entries()` distinct by snippet, groups include discovered casings); `core/utils/record_id_util.py` (`mention_id_for_snippet`, prefix `m`, `MentionIdCollisionError`); `extraction_schemas/mention_collection.py` REWRITTEN (`MentionWireItem`, `MentionLocationEntry`, `MentionLocationsResponse`, schema name `phrase_mention_location`, dummy `{"mentions": []}`, `parse_mention_location_response` → `LocationsByMentionId`); `phrase_blocks_contract.py` (`render_mention_blocks`, `sent_mention_ids_from_user_message`, `sent_mentions_from_user_message`, `hold_response_to_sent_mention_ids` warn-only; readers anchor on the LAST fence so a fence line inside scraped text cannot hijack them; `hold_response_to_sent_forms` removed); `llm_phrase_mention_collection_node_service.py` REWRITTEN (`collect_sub_window`, `split_into_item_groups`, `group_digest_payload`, `render_mention_location_context`, `create_mention_location_gpt_request`, dummy, `create_missing_mention_collection_requests` reads `bundle.llm_phrase_mention_sent_forms` — no search maps, `parse_mention_group_result`, `get_window_locations`, `get_chunk_fold`, `stored_window_forms`); `llm_phrase_mention_collection_node.py` (embed collects from `PipelineContext.subject_text`, stores forms on the bundle, ids digest the group's items); `base_node.PipelineContext.subject_text` + orchestrator sets it; `deferred_phrase_extraction_requests.LLMPhraseExtractionRequestBundle.llm_phrase_mention_sent_forms`; `BatchedMentionCollectionNodeMetadata.max_mentions_per_request` (was `max_forms_per_request` — old deferred docs will not load; re-defer); factory `DEFAULT_MENTION_COLLECTION_MAX_MENTIONS_PER_REQUEST = 50`, kwarg `max_mention_collection_mentions_per_request`; `chunking_strat.merge_trailing_remainder` (+ `REMAINDER_MERGE_RATIO = 0.2`); search service sends `wire_window_text`; `fold_dump_util.py` REWRITTEN (mention rows carry `mention_id`/`location_source`/`sent_form`; window rows carry `forms_with_hits`/`zero_hit_forms`/`distinct_snippets`/`described`/`not_described`/`excluded_pages`); contract-product node signature `group_items`. Statics: six byte-identical `3_phrase_mention_collection/*` rewritten as the LOCATION prompt (stage name, request token and prompt names KEPT — the stage still collects mentions, code does it now). Tests: fold/dump/wire/service rewritten, floor_scan exclusion + chunking remainder tests added, renames in custom_id_segments/factory_equipment/wiring. NOT built: the focal-form synthesis ask (3.2); P4/P6/P7 subsumed. Next: the user re-runs (fresh deferred docs), then 3.2. |
| 2026-08-22 | **Third v3 run analyzed — 20260822T223715 (mechanical collection + LLM Location stage, P3, P5) vs 20260822T195947; evidence `pipeline_v3_evidence/2026-08-22_run_223715_location_stage/` (README has the table; `wire_verify.py` verifies on the exact wire texts from Mongo — the local sample / S3 texts differ from the pipeline's alecmfg text by 44 chars).** Zero errors; Mongo holds exactly this run (106 Location + 84 search docs; search re-ran, no stale replay). WHAT THE RESTRUCTURE BOUGHT: coverage verified 100% — 2,650 spans exact, 4,726/4,726 snippets in-window, 0 uncovered occurrences of any collected form (was 81%), 0 `Steel`-in-`Steelcraft`, 0 mentions on excluded pages, 0 multi-line snippets; mentions 3,636 → 4,726 (+30%), wire items 2,755 (estimate 2,880), snippet median 138 → 103 chars (max 1,973 → 382); empty groups 227 → 72 (63 swallowed under D8, 9 zero-hit); zero-hit sent forms 4% → 0.4%; casing rescue live (308 discovered casings, 461 rescued mentions); P3 excluded 7 legal URLs, the privacy window now a dummy (6 fields), 0 junk from it; P5 left no remainder windows; tokens mention-stage in 814k → 584k (72%; est. 85%) / out 283k → 179k (63%; est. 41%), search in 462k → 363k, wall 1,804 → 1,601 s; hold: 2,755 sent ids, 2,708 returned, 1 unknown, 48 missing, 0 dups/unparseable; locations by eye name the role and speaker accurately. WHAT WENT WRONG: (1) DEFECT — one request (alecmfg industries 98612:123329, 47 items) answered with the request NONCE (`uuid4().hex` that `create_base_gpt_batch_request` prepends to every user message) as the mention_id and 'there is no mention with the id …' → 0/47 described (defaulted, no data lost); the window starts mid-page so the nonce sat directly above prose; +1 benign skip (35/36). (2) Location VERBOSITY is the new dominant cost: median 218 chars (was 106), 70 completion tok/item (est. 40), location chars 2.13× snippet chars, URLs 18% of location chars (55% carry one), prefix 6%, restatement verbs 27%; synthesis entry payload 902k → 1,234k chars (+37%; snippets −28%, locations +160%; locations = 65%). (3) 72 locations (2.7%) refer to OTHER mentions of the request ('following the previous mention'). (4) 273 window-head items (page known only to code): 59 hedged, 148 named a page anyway, sometimes not code's. (5) Remaining loss channel = window-local sent forms: 1,144 occurrences of a chunk's own forms uncollected in windows where search didn't list them (491 distinct lines ≈ +18% wire items if chunk-wide; top `doors` 77, `frames` 55, `steel doors and frames` 44, `steel` 39, `cnc machining` 30). (6) Polyseme import as predicted: alecmfg `lead` 17 mentions = 12 'Lead Time' + 5 roles/verbs, 0 metal. (7) alecmfg cookie policy sits under the homepage URL (P3 miss, 1 junk mention). PROPOSALS (none built): N1 label/move the nonce or prepend the inherited page URL line as the window header (fixes 1 and 4); N2 Location under-answer policy (unknown-id / missing > X% → retry); N3 sent-form scope window-local vs chunk-wide vs document-wide; N4 location cost — synthesis A/B first, or strip URLs mechanically (−18%) / 'stand alone, no URL, no restatement'; N5 `unknown_ids` on the dump window row; N6 content-cue legal exclusion. Next: decisions, then 3.2. |
| 2026-08-22 | **User review of run 223715 — DECISIONS + two measurements (evidence README addendum in `pipeline_v3_evidence/2026-08-22_run_223715_location_stage/`).** DECISIONS: (a) the user-message nonce is cache-busting → MOVE TO THE END of the user message (`create_base_gpt_batch_request`), all stages; (b) Location under-answer policy: BUILD (retry); (c) sent-form scope: collect forms across the chunk's sub-windows and send each sub-window only the forms that OCCUR in it (chunk-wide, occurrence-filtered) — search stays responsible for per-window recall; (d) Location static: descriptions must stand alone (no 'the previous mention' / 'the same list' references); (e) location verbosity ACCEPTED — value measured at 3.2; (f) page-aligned chunking = user's work in another chat; (g) polyseme import waits for synthesis; (h) cookie text under the homepage URL accepted. MEASUREMENTS: provenance resolved — `subject_text` is the S3 object after `normalize_scraped_text` at load (NFC + canonicalization), so offsets must be checked against the normalized text (raw S3 / sample files differ: alecmfg 142,826 vs 142,870). Q3 metric — search lists its own document-wide vocabulary in 74% of the windows where a family occurs (10,279 occurrences: 5,385 listed, 2,188 inside a longer listed form, 2,706 missed; industries 58%, steelcraft equipments 53%; chunk-scope 1,144 missed / 491 lines); caveat: inflated by generic families listed once and by wrong-field listings (`Doors` under equipments). Search self-evaluation WITHOUT the prompts (4 products windows, 422 forms judged by Claude on the plain meaning; `search_self_eval.py`): precision (valid+borderline) 37% — alecmfg 10%, steelcraft 70% — the alecmfg mass is processes 43, materials 33, client's products 18, specs/documents/equipment/facility 90, generic 23; recall of my own product designations 79/84 = 94%. Verdict: products search over-generates, it does not miss. Next: build plan for (a)–(d) presented for go-ahead; then 3.2. |
| 2026-08-22 | **BUILT (user go-ahead) — Location retry, chunk-wide occurrence-filtered forms, stand-alone Location descriptions; the NONCE is NOT touched (another agent owns it).** Code: `llm_phrase_mention_collection_node_service.py` — `get_chunk_forms` (pool of every sub-window's search ∪ recursive ∪ brute casings), `forms_occurring_in_window` (tier-2 filter on the window's scan domain, inherited page honoured), `retry_items_of_window`, retry request creation inside `create_missing_mention_collection_requests` (stored retry ids grouped like the first pass, drift raises), `parse_mention_group_result` → `(sent_ids, held, unknown_ids)`, `get_window_locations` → `WindowAnswer{sent_ids, locations, unknown_answer_ids, retried_mention_ids, missing_ids}` reading groups then retry (`include_retry=False` = assessment view), `get_chunk_fold` passes the diagnostics into the fold; `llm_phrase_mention_collection_node.py` — now a `BaseLLMRecursiveExtractionNode`: pass 1 embeds group ids from the pooled+filtered forms, pass 2 (all requests complete) assesses every window once (`llm_phrase_mention_retry_mention_ids[sub]` = missing ids, `[]` = assessed clean) and embeds ONE retry set per window with missing ids (`…>sub>{s}>retry>1>group>{j}>…|ud=`), third entry adds nothing; `get_embedded_request_ids` includes retry ids; `get_request_custom_id(..., retry_index=None)` (first-pass ids byte-identical); contract-product override passes `retry_index`; bundle gains `llm_phrase_mention_retry_mention_ids` + `llm_phrase_mention_retry_req_ids` (defaults; old docs load); `aggregation_fold.WindowInput/WindowFold` carry `retried_mention_ids`/`unknown_answer_ids`; `fold_dump_util` window rows gain `retried`, `unknown_answer_ids`, summary gains `retried`, `windows_retried`, `unknown_answer_ids`; `extraction_dump_util` witness gains `llm_phrase_mention_collection_retry` per sub-window; `deferred_manufacturer.schema.json` regenerated (NOTE: the generator also pulled in the user's WIP `align_to_page_headers` chunking field; `manufacturer.schema.json` / `binary_ground_truth.schema.json` were restored to the user's working copies). Statics: six byte-identical Location prompts + the stand-alone sentence, PUBLISHED + pinned, `check` clean (new pv → the Location stage re-runs next time). Tests: core 509 / app 406 green (new: `test_mention_collection_node_passes.py` two-pass node test; service tests for pooling, filtering, retry merge/creation/drift, fold diagnostics; wiring retry-id shape); pyright delta clean (node file 7 pre-existing 'overrides incompatibly' vs 9 before; dump util 14 = 14 baseline), ruff clean, py_compile clean. Expected on the next run: ≈+18% wire items from pooling, 0 zero-hit forms, retries only where a window came back short. Next: the user's re-run (re-defer: deferred docs predate the retry fields? they load — defaults — but a re-defer is the protocol), then 3.2. |
| 2026-08-22 | **N1 BUILT — page-aligned chunking + continued-page wire header + Location statics republished (page-alignment chat; user go-ahead on the plan as presented; evidence `pipeline_v3_evidence/2026-08-22_run_223715_location_stage/n1_*`).** PROBLEM: the chunker cut by token budget at line boundaries with no notion of a page, so every window opened mid-page — measured on the 7 tokenizable sample subjects under `wide` (20k÷4, overlap 0, max 2 chunks): 39/39 windows started mid-page (window 0 included: the file opens on the separator, URL on line 2), the headless head typically 200–1,500 tok = 5–30% of a window; on run 223715 that was the 273 window-head items and the nonce-as-mention-id request. FEASIBILITY: pages are small — median 28 (alecmfg) to 1,548 (taylordunn) tok; 1 of 307 sample pages > 5k (steelcraft privacy, 6,585 tok, excluded under P3), acimachine 1 of 1,430 (0.1%). BUILT (1) `llm_providers.utils.chunk_util` `get_chunks_respecting_line_boundaries[_sync](break_before=)`: on overflow the chunk closes BEFORE the last preferred-break line it holds (never its first line → no empty chunk) and carries that partial page into the next chunk; no such line → the old line split (an oversize page still splits; only its continuations open mid-page); overlap lines, when any, precede the carried header (alignment is an overlap-0 guarantee). (2) `ChunkingStrategy.align_to_page_headers` (default False; `wide` True — the single-shot strategies would only lose a tail page) + `chunk_break_predicate()`; predicate `floor_scan.is_page_header_line` = the `##########` separator line; threaded into the macro chunking of the keyword / concept / single-stage prefill nodes AND `derive_search_sub_bounds`, so chunk 2's first window is aligned too. (3) `floor_scan`: a page now STARTS AT ITS SEPARATOR when one directly precedes the URL line (`_page_headers`; a bare URL line still opens a page; a separator not followed by a URL is body) — found while building: the separator a window opens on belonged to the PREVIOUS page, so after an excluded page `omit_excluded_pages` would have emitted a spurious marker line at the top of a good window; `omit_excluded_pages` keeps separator + URL of an excluded page; `wire_window_text` prepends `continued_page_header(url)` = separator + inherited URL + `CONTINUED_PAGE_MARKER` ('[this page began before the text shown; its opening is not included]') when a window has a non-blank head inherited from a non-excluded page — search and Location both go through it; the scan and the fold never see it. (4) Six byte-identical Location statics: intro explains the continued-page line; Task sentence now 'say what the text shows and no more' — PUBLISHED + pinned, `check` clean (on top of the other chat's 'stand alone' sentence, already on disk). (5) Nonce: `NONCE_LABEL` = 'request nonce (ignore): ' ahead of the hex, FIRST line kept (user: it must miss the prefix cache before the context is read; note: OpenAI documents the cache as reusing prefix computation, not answers — position does not change outputs and, since every context differs, costs nothing either way). SIMULATED COST (`n1_page_packer_sim.py`): windows 1/7/6/3/6/8/8 → 1/8/6/4/7/8/8 (+3 of 39, ~+8%), fill median 4.2–4.7k tok, one 766-tok runt (strict alignment accepted — a minimum-fill guard would recreate mid-page windows); coverage under `max_chunks=2`: steelcraft 189k → 157k chars (the privacy page eats a window + packing slack), taylordunn 182k → 176k — user: the 40k cap is a document cap for now, they may raise `max_chunks`. TESTS: +6 chunk_util (close-before-header, oversize fallback, no empty chunk, unchanged without predicate, max_chunks, async threading), +5 in new `test_chunking_page_alignment.py` (both levels aligned, off by default, oversize continuation), floor_scan +3 (no spurious marker after an excluded page, mid-page head announced, no header when unknown/excluded/blank) and TWO PINS UPDATED (separator attribution: `page_at(spans, 0)` is now the first page; the inherited head ends at the separator). SUITES: llm_providers 52, app 406 green; core 507 green + 2 failures in the OTHER chat's in-flight files (`test_mention_collection_node_passes.py`, `test_forms_occurring_in_window…` — green on rerun); pyright delta 0 (the 3 `next_node` override errors pre-exist); ruff clean on the lines written here (pre-existing E501/E741/B905 left). CONSEQUENCES: new bounds → new custom ids → search + Location replay fresh next run; deferred docs from 223715 won't resume (re-defer); `chunk_strat` stored on results loads old docs with the flag's default. NOT BUILT: excluded pages at zero budget (steelcraft's privacy page still costs a window + 2 dummy windows — rare); a runt guard; search statics untouched (the continued-page line is plain text to them, and they never harvested URL lines). Next: the user's run, then 3.2. |
| 2026-08-22/23 | **FOURTH RUN ANALYZED — 20260823T034518 (Location retry + chunk-wide occurrence-filtered forms + 'stand alone' sentence + N1 page-aligned chunking, all first-run here; evidence `pipeline_v3_evidence/2026-08-23_run_034518_retry_chunkwide_pagealigned/`; log `apps/data_etl_app/src/data_etl_app/scripts/latest_extraction_logs.txt`; Mongo = exactly this run, 105 docs).** WRONG: (1) steelcraft CRASHED 26.6 s in (products mention stage): `ValueError: Cannot dispatch GPT batch request …>sub>63780:90445>group>0…: built for model 'no_model' but dispatch was handed 'gpt-4.1'` — the node is now a `BaseLLMRecursiveExtractionNode` whose eager path (`base_llm_recursive_extraction_node.py` ~L128) dispatches every request it just created, whereas `BaseLLMExtractionNode` (~L370-379) dispatches only `find_incomplete_gpt_batch_requests_by_custom_ids`; a zero-mention window's `create_dummy_completed_mention_location_request` (NO_MODEL, pre-answered) therefore reaches the model guard. Trigger: two steelcraft sub-windows were ENTIRE legal pages blanked by P3 (63780:90445 privacy head 26,665 chars; 136253:157472 terms 21,219 chars; wire 180 chars, search `[]` at ~765 prompt tokens). Run 223715 survived the same dummies on the base class; alecmfg had no empty window. Steelcraft lost all 7 phrase fields. Fix (not applied): filter completed requests before dispatch in the recursive base / skip NO_MODEL bodies; test an empty window on the recursive node. (2) Steelcraft coverage regression, exact (tiktoken o200k, sample text 45,596 tok, legal 12,661): OLD 0:97071+97071:192276 = 39,048 used, legal 12,661, real 26,387 = 80% of real content; NEW 0:63780 (13,292, no legal) + 63780:157472 (18,682, legal 10,906, real 7,776) = 31,974 used, idle 8,026, real 21,068 = 64%. The oversize privacy page (6,585 tok) did not fit chunk 1 → close-before-header idled 33% of chunk 1; it then headed chunk 2, split across two sub-windows, blanked anyway; terms-of-use filled a whole sub-window, blanked. 47% of the budget produced nothing → 'excluded pages at zero budget' (N1 row: NOT BUILT/rare) is the dominant lever and also removes the slack. (3) Location verbosity up: distinct locations 1,242 → 1,407; median products 215 → 246, industries 207 → 262; URL-bearing industries 25% → 83% (192/248 cite the code-derived page exactly), conformity 51% → 71%, material 48% → 64%; URL chars industries 9% → 27%; 'This passage…' prefix products 13% → 34%, industries 2% → 25% (every window now opens on a URL line + the republished intro talks about URLs). Synthesis payload (6 fields) 619k → 694k chars (+12%), locations 68%; biggest record equipments `machining` 37 entries 13.4k chars. (4) Search unstable under re-chunking: distinct phrase sets share ~55% (Jaccard 0.45–0.58; products 432 → 384, process_caps 316 → 274, conformity 42 → 56) — confounds every old-vs-new mention comparison. (5) Tokens alecmfg mention stage in 296,051 → 317,797 (+7%), out 91,107 → 108,363 (+19%), out/item +3–12%, in/item flat; search flat; tiny last groups still re-send the window (1-item requests at 6,351 / 5,666 prompt tokens). RIGHT: Location described 1,560/1,560 (prev 1,371/1,419), unknown ids 0, retry sets 0 (armed, never needed), nonce labelled first in all 105 requests with 0 nonce-as-id answers; strict relative refs 21 (1.7%) → 0; chunk-wide forms: forms_with_hits == sent in every window, zero-hit 6 → 0, discovered casings 165 → 251, mentions 2,374 → 2,723 (+15%), items 1,419 → 1,560 (+10%), empty groups 47 → 67 all but 1 = D8 containment; N1: all 13 windows open on separator+URL, 0 continued headers needed (the one mid-page head inherits the excluded privacy page → correctly none), no runts; P3 4 alecmfg legal URLs, P5 no remainders; alecmfg 10 fields 133 s wall. Next: fix the dummy dispatch (user go-ahead), decide zero-budget exclusion, re-run steelcraft, then 3.2. |
| 2026-08-23 | **BUILT — dummy dispatch fix, legal pages dropped before chunking, no-address Location prompt (user decisions 2026-08-23: drop the word URL, tell the model to name a page in its own words; single-stage fields keep the full text; knob on `ChunkingStrategy`; scoped delete + re-run by the user).** (1) `base_llm_recursive_extraction_node.py`: the eager loop now builds `requests_to_dispatch = [req for req in batch_requests if req.response is None]` and dispatches/records only those — the mention stage's pre-answered NO_MODEL dummy (one per zero-mention window) is stored and never sent; log line counts the pre-answered ones. Test `test_recursive_node_eager_dispatch.py` (fake recursive node creating one answered + one unanswered request; asserts upsert sees both, dispatch and record see only the real one). (2) `floor_scan.py`: `PAGE_EXCLUSION_VERSION = "1"`, `DroppedPage`, `PageExclusion{version, text, dropped, chars_before; chars_removed/chars_after; to_dump()}`, `drop_excluded_pages(text)` — whole page removed (separator + URL + body, no marker), pre-header text kept, pure/deterministic, idempotent on its own output (+3 tests). `chunking_strat.py`: `ChunkingStrategy.drop_excluded_pages: bool = False`, `page_exclusion_version: Optional[str]` (on requires version; version requires on), `wide` = on + pinned version → stored on every phrase metadata's `chunk_strat` so a rule change is drift (old docs load with the defaults and differ → re-defer). `base_prefill_node.PrefillNode.apply_page_exclusion(file, ctx)`: on every execute (fresh AND resume — bounds are offsets into the trimmed text), refuses a version mismatch, `model_copy(update={'text': …})` of the frozen scraped-text object (S3 identity + its own `num_tokens` kept), sets `PipelineContext.subject_text` and `PipelineContext.page_exclusion`; called first thing in the keyword and concept prefill `execute` and the trimmed file is what flows down the chain (search, mention, fold, dumps); the single-stage prefill is untouched. `base_node.PipelineContext.page_exclusion`; `extraction_dump_util.build_run_provenance(page_exclusion=)` → `run.scraped_text.excluded_pages {version, chars_before, chars_removed, chars_after, pages[{url,start,end,chars}]}`; passed from `partial_run_dump` and the keyword/concept reconcile nodes. Tests: `test_page_exclusion_before_chunking.py` (6: wide knob + version; validation + old-doc drift; prefill trims/records/idempotent on resume; off = untouched; version mismatch refused; provenance block). The wire-level masking (P3) stays in code as belt-and-braces. NOT changed: the cookie-under-homepage text (accepted), oversize REAL pages at a boundary (rare). (3) Six byte-identical Location statics: the user's rewrite (continued-page explanation, 'reader who cannot see the text' framing and rationale clauses removed; stand-alone + recurrence rules kept) + 'its address' for 'its URL' in the intro + new sentence 'Name a page by what it is about, in your own words — never by copying its address, or any part of it, into a description.' — PUBLISHED + pinned (`assemble_prompts publish`, `check` clean; new pv per field). The continued-page marker line still appears on the wire for an oversize real page's continuation (plain English, left unexplained by design). Checks: core 518 passed (+ known `test_normalize_is_idempotent` deselected), app 406, llm_providers 52; pyright on all edited files = the 23 baseline errors only (dump-util `object` family 14, `get_result` 5, prefill `next_node` 2, reconcile Mapping 2); ruff clean on written lines; py_compile clean; `generate_db_schemas.py` re-run — `deferred_manufacturer`, `manufacturer`, `binary_ground_truth` schemas gained the two chunk_strat fields, diff purely additive (user WIP intact; backup in the session scratchpad). Expected next run: steelcraft chunks pack real pages only (≈100% of its 32.9k real tokens within the 40k budget vs 64%), no legal-only windows, no dummies from P3, new bounds → new custom ids everywhere; alecmfg bounds shift slightly (its legal pages are small). Next: the user's scoped delete + re-run, then 3.2. |
| 2026-08-22 | **FIFTH RUN ANALYZED — 20260823T044500 (dummy-dispatch fix + legal pages dropped before chunking + no-address Location prompt, all first-run here; evidence `pipeline_v3_evidence/2026-08-23_run_044500_pages_dropped_no_address/` — README, `dump_compare.py`, `pass2.py` + outputs; log `apps/data_etl_app/src/data_etl_app/scripts/latest_extraction_logs.txt`, 2,451 lines, 227 dispatched, 0 errors). Compared with 034518 (alecmfg) and 223715 (steelcraft's last completed run).** RIGHT: (1) no crash — the two zero-form windows (`alecmfg/conformity_attestations 0:23771`, `steelcraft/equipments 154347:158790`) produced NO_MODEL dummies that were created and never dispatched; both subjects finished all 10 fields (steelcraft 118.1 s, alecmfg 105.7 s). (2) Real-content coverage 100% on both (tiktoken o200k): steelcraft 224,094 ch / 45,596 tok raw → privacy 35,057 + terms 21,219 + cookie 9,028 ch dropped (29%) → 158,790 ch / 32,935 tok all chunked (19,077 + 13,858 of a 40,000 budget; was 64% with 8,026 idle + 10,906 blanked); alecmfg 4 legal pages / 15,134 ch dropped; exclusion list = exactly legal pages, no over-removal. (3) steelcraft yield follows the budget: mentions 2,352 → 3,579 (+52%), items 1,336 → 1,888 (+41%), groups 1,123 → 1,297, for +31% in / +13% out tokens (products 543→811, process 204→311, material 218→302, conformity 124→207, industries 97→107, equipments 150→150). (4) No-address prompt: 0 of 6,085 locations contain a URL (prev per field 39–83%); 85% name the page in words; median length 246 → 208 ch (alecmfg products), 262 → 200 (industries), 216 → 169 (steelcraft products), run-wide 194 ch. (5) Synthesis payload alecmfg 674,557 → 561,259 ch (−17%), location share 68% → 65%; steelcraft 562,517 → 643,583 ch on +32% entries, share 65% → 60%. (6) Retry pass earned its keep: alecmfg/process_caps 45436:67366 echoed `m2iv4vtt` for `m2iv4ltt`; hold caught it, retry (5,078 in / 82 out) described it; run-wide 3,345 sent / 3,345 described / 0 not_described / 1 unknown id / 1 retry. (7) Tier-2 collection: 722 of 6,085 mentions (11.9%) harvested under a casing search never returned; `discovered_casings` 547 sightings / 311 distinct, 394 appear as harvested forms and 153 are D8-swallowed spans — a record of what the scan added, not misses. (8) `forms_with_hits == sent_forms` in every window (0 zero-hit forms); 115/115 empty groups = D8 containment; every mention `location_source: llm`. WRONG / OPEN: (1) search unstable under re-chunking — distinct phrase sets share Jaccard 0.43–0.64 with the previous run; alecmfg's −8% mentions / −7% items (equipments 47 → 33 phrases, conformity 56 → 41) is drift not loss; no old-vs-new comparison on a re-chunked subject measures anything but search variance — Phase 5 A/Bs must hold bounds fixed. (2) 26% of mention-stage input tokens = the window re-sent for every 2nd+ group (182,720 of 700,193; 126 requests, median 24.5 items; 30 requests <10 items, 9 <5; worst 1 item at 5,491 tok). (3) Locations 60–65% of the synthesis payload, median 194 ch vs snippet 106 ch; 5,848/6,085 open 'This passage/sentence/phrase/line/word…'. (4) 19 client-level OpenAI retries (8.4%), transparent; real latency median 3.3 s, p90 10.8 s (`client_latency_ms`). (5) `time_span.seconds` measures elapsed since `deferred_at`, not work duration — use `client_latency_ms`. COST whole run 1,340,287 in / 203,110 out ≈ $4.31 (mention stage 700,193 / 182,239). **USER DECISIONS (same day, this session):** focal-form synthesis ('describe the focal entity using these entries as evidence'; D15 amended), two synthesis arms with/without `location`, a snippet-RADIUS knob on the mechanical collector, COMMIT the tree, cost levers deferred until v3 runs end-to-end ('execute the whole pipeline first, then adjust knobs'), idempotence-test fix pending. Next: 3.2 build plan in chat → build. |
| 2026-08-22 | **3.2 BUILT — synthesis node + location A/B arm + retry + snippet-radius knob + idempotence test (c) (user go-ahead 2026-08-22: 'Build the retry as well. If no other questions, proceed.').** FILE-BY-FILE. core: `utils/aggregation_fold.py` — `_Lines.clip(…, radius)` (legacy path untouched at 0; `_Unit`/`units()`/`_clip_with_radius` for r > 0: sentence units across lines, `floor_scan.is_page_barrier_line` blocks), `collect_window(snippet_radius=)`, `fold_window`/`fold_document(snippet_radius=)`, `FoldResult.snippet_radius`, `MentionBundle.focal_form` (Counter by `m.form`, ties → earliest first mention), `synthesis_entries/record(include_location=)`, `FoldResult.synthesis_records(include_location=)`; `utils/floor_scan.py` — `is_page_barrier_line`; `extraction_schemas/synthesis.py` — `SynthesisEntry.location: Optional`, `SynthesisRecordInput.focal_form` + `wire_dict()` (exclude_none); `pipeline_nodes/base/pipeline_stage.py` — `PipelineStage.synthesis` rank 4 (relationship 5 … reconcile 10), token `llm_phrase_synthesis`; `extraction_results/llm_phrase_extraction_results.py` — `BatchedMentionCollectionNodeMetadata.snippet_radius=0` (`|rad=r` only when r > 0) + NEW `BatchedSynthesisNodeMetadata{max_entries_per_request, include_location}` (`|gs=N|loc=0/1`); `…_v2.py` — `llm_phrase_synthesis: Optional`; `deferred_phrase_extraction_requests.py` — `llm_phrase_synthesis_req_ids`, `llm_phrase_synthesis_retry_record_ids: Optional[list]` (None = unassessed), `llm_phrase_synthesis_retry_req_ids`; `services/…/llm_phrase_mention_collection_node_service.py` — radius threaded (`collect_sub_window`, `create_missing…`, `get_chunk_fold`) + `fold_snippet_radius_of`; NEW `services/…/llm_phrase_synthesis_node_service.py` — `require_synthesis_metadata`, `synthesis_include_location_of`, `chunk_fold`, `pack_records` (soft cap, never split, monster alone, `[[]]` for none), `retry_records_of_chunk` (raises on unknown), `group_digest_payload`, `render_synthesis_context` ('the name of the manufacturer in question: …' + the two record blocks), `create_synthesis_gpt_request` / `create_dummy_completed_synthesis_request` / `create_missing_synthesis_requests`, `parse_synthesis_group_result` (exact hold: unknown dropped + reported, missing left), `ChunkAnswer` + `get_chunk_syntheses(include_retry=)`, `ChunkSynthesisResult` + `get_chunk_synthesis_result` (raises if sent ids ≠ fold records); NEW `pipeline_nodes/multi_stage/base/llm_phrase_synthesis_node.py` (`BaseLLMRecursiveExtractionNode`, two passes, abstract `get_upstream_mention_collection_map`, the ONE new `get_request_custom_id` builder the token tripwire counts), NEW `concept/concept_synthesis_node.py`, NEW `keyword/keyword_synthesis_node.py`; mention nodes' `next_node` annotations → the synthesis nodes; both prefill nodes take `llm_phrase_synthesis_metadata`; `partial_run_dump.py` — radius passed to the fold, NEW `synthesis` block (`utils/synthesis_dump_util.build_synthesis_dump`); `pipeline_nodes/__init__.py` exports. app: leaves `pure_product_synthesis_node.py` / `contract_product_synthesis_node.py` (id as `products`) / `equipment_synthesis_node.py` + exports; `extraction_pipeline_factory.py` — `DEFAULT_MENTION_COLLECTION_SNIPPET_RADIUS=0`, `DEFAULT_SYNTHESIS_MAX_ENTRIES_PER_REQUEST=50`, `DEFAULT_SYNTHESIS_INCLUDE_LOCATION=True`, `_batched_synthesis_metadata`, `_require_synthesis_prompt`, `phrase_synthesis_prompt` + the three knobs on all four `create_*` and on `create_pipelines`, chain mention → synthesis → v2 tail; `prompt_service.py` — six `*_phrase_synthesis` paths + properties; six statics REWRITTEN (focal-form Task, 'where given' location wording serves both arms) → `assemble_prompts.py publish` (6 new pv in `static_prompt_pins.config.json`), `check` clean. TESTS: core +27 (fold radius golden/units/lines/barriers/spanning occurrence/negative/hypothesis invariants, focal form + ties + empty, location arm; synthesis service: packing, retry ids, context on both arms, strict schema + dummy, exact hold, merged answer groups→retry, no-requests guard; node passes: pass 1 packing/idempotence/arm+cap ids, radius reaches mention ids + digest, pass 2 assess/retry/result/dump, empty-chunk dummy; dump util; id segments `|gs|loc` + `|rad` only when set; stage token scope), app +4 (prompts registered/on disk/byte-identical, concept chain + knobs as identity, prompt required, pure chain + shared contract identity incl. retry); three existing app tests gained `phrase_synthesis_prompt=`; `test_form_normalizer.py` — property at verb_fold=False + `test_verb_fold_is_single_pass_not_a_fixed_point` (Grounded→ground→grind pinned); `test_synthesis_wire.py` — focal_form + no-location-arm test. Suites core 546 / app 410 / llm 52. pyright delta 8 = pre-existing families only; ruff clean on written lines. NOT built: 3.3 re-key; synthesis stats in the full-run dump (reconcile is unreachable until 3.3). NEXT: user re-run with `stop_after(PipelineStage.synthesis)` → read the `synthesis` block → location A/B → 3.3. |
| 2026-08-23 | **SIXTH RUN ANALYZED — 20260823T200044 vs 20260823T195031, a clean synthesis-prompt A/B (evidence `pipeline_v3_evidence/2026-08-23_run_200044_synthesis/` — README, `synthesis_ab.py`/`_output.txt`, `thin_evidence_records.json`).** Both runs replay the 04:45:00 mention state: same 95 request ids, every `ud=` digest identical, only `pv=` differs. COUNTING TRAP: `products` and `contract_products` share the synthesis request (`ContractProductSynthesisNode` mints the `products` id — deliberate, saves 39 of 134), so their dumps are byte-identical apart from `field_type` and any folder-wide sum overstates by ~32%; true totals **2,133 records / 95 requests**. RIGHT: 2,133/2,133 synthesized in BOTH arms, `not_synthesized`/`retried`/`retry_requests`/`unknown_answer_ids` all 0 (the 3.2 retry path has still never fired); empty-group invariant exact (3,309 − 148 = 3,161); max output 4,174 tok vs a 20,000 cap; 1 of 6,085 mention entries lacked a location; 10/2,133 records merge forms beyond case/plural and all 10 are legitimate verb-fold merges. THE REWRITE'S WINS: alecmfg own-name 47 → **0** (all 47 were 'Alec Model' in `industries`); set-aside floor 2 → **48** records; third-party attribution 6.5% → **9.1%**. WRONG: (1) ONE entity silently renamed — `steelcraft/equipments`, focal and sole snippet both `FE Series Double-Egress Frames`, synthesis wrote `DE Series Double-Egress Frames` twice; new (correct in 195031), stochastic (the same record in `products` is right), the only genuine invention in a sweep of every `<X> Series` name in 1,250 syntheses; the hold validates `record_id` echoes, never the entity. (2) +28% output for the same records (645,553 → 829,318 ch; ≈$2.13 → $2.42 for two subjects), uniform across every evidence size = the extra mandated sentence, not richer coverage. (3) The own-name ban is unsatisfiable on steelcraft (139 → 126 hits): **46% of hits are records whose focal_form CONTAINS the name**, against a rule that says to use the focal form; 12.8% of its mention locations and 13.5% of its snippets carry the name (mention collection is not under the ban); `[the manufacturer]` was used 7 times in 1,250. **AUDIT CORRECTION (do not repeat the first reading):** the 1% → 45% jump in asserted dealings on the 393 thin-evidence records is REAL but is NOT a defect rate — checked against `knowledge/sample_scraped_texts/`, 71 sit on own product pages, 43 on alecmfg case studies, 17 services, 16 service-centre, 9 applications, 5 home/support, 15 downloads. `Micro-blasting` is a Step/Parameters/Result row in a DELIVERED project (`alecmfg.com.txt` L1049) — the assertion is correct; `4-axis machining` came from a **Client Requirements** table and the model hedged correctly ('is expected to provide'); it also produced a NEGATIVE dealing correctly ('does not offer factory finish paint for frames'). **Genuine defects: 2** — `LEED Credits` and `CalGreen Building Standards`, whose location said 'More from Allegion' (parent; `steelcraft.com.txt` L146–151) and where the new text dropped the qualifier the OLD text kept; both in `conformity_attestations`, so they would ground as the company's own claims. Of all 87 records whose location names a third party, 32 dropped ≥1 name and 15 still credited the manufacturer, but 13 of those dropped only 'Falcon' (a Steelcraft sub-brand). Side effect, not a defect: 33 records read as 'provides this as a resource' because their only chunk occurrence is a downloads link label — 23 are true document titles (helpful; screening can drop them on the word 'resource'), 10 are real product lines used as link labels, but GRAINTECH/FT Series are covered by 9 and 5 other records and the two that are not got the floor phrase in the other chunk — **no net loss found**. WITHDRAWN: an earlier suggestion of mine to add a 'listed, therefore probably offered' floor — the audit killed it. MEASURED FOR THE TWO OPEN FORKS: locations are **63% of the evidence payload** (952,386 ch vs 567,678 snippet; median 191 vs 112) and **89% of syntheses name a page/section/heading**, and **27% of location text is a boilerplate opener** (333,450 of 1,229,700 ch) — so trim the opener in the MENTION static (≈17% off the whole synthesis input, no information lost) rather than flipping the arm off; the `loc=0` arm is still unrun. Next: build the fixes. |
| 2026-08-23 | **BUILT (user go-ahead: 'implement the next step then') — own-name ban RETIRED from synthesis, party-preservation rule added, focal-form lint added. NOT PUBLISHED (user's explicit choice).** (1) Six byte-identical `4_phrase_synthesis/*.txt` statics, three edits: the Task sentence drops "and to the manufacturer as 'the manufacturer'"; the masking paragraph and the read-back check are DELETED (the "other companies' names are quoted as written" sentence is rescued out of it) and replaced by a new section **## The manufacturer's own name** — *the name is given above the records; use it to tell what belongs to the manufacturer and what does not; where the focal entity is the manufacturer itself, or one of its brands, product lines, divisions, plants, or former names, say so plainly; write names as the entries write them; you need not avoid the manufacturer's name*; and **Rules on reporting** gains *where an entry — its snippet or its location — places the item with a party other than the manufacturer, such as a parent company, a publication, or a customer, name that party in your synthesis; do not drop it* (the Allegion fix). RATIONALE: the ban was unsatisfiable (46% of steelcraft hits are the focal form itself), the pipeline supplies the name itself through its own location text (12.8%), the `[the manufacturer]` workaround was dead (7 uses in 1,250), and the lint changes nothing downstream by design. The bracket convention is retired from SYNTHESIS only — the seven screening statics still name it, harmlessly, until 3.3. (2) NEW `core/utils/focal_form_lint.py` — `is_entity_shaped` (2–6 tokens with a capital or digit past the first: a proper name, not a lifted bullet) + `focal_form_absent(synthesis, focal_form, forms)`; any MEMBER form satisfies it. The naive check flags 24/2,133 (1.1%), nearly all correct re-inflections of clause-shaped focal forms; the entity-shaped band flags **3 (0 real) in 195031 and 2 (1 real = FE→DE) in 200044** out of 700 entity-shaped records per run — verified by replaying the shipped module over both dumps. Deliberately a dump lint, NOT a retry trigger, and NOT a hard failure (same posture as `subject_name_lint`). (3) `synthesis_dump_util.py`: per-record `focal_form_absent` (emitted only when true, like `own_name_hits_in_synthesis`) and summary `focal_form_absent_records`; docstring names both lints. TESTS: new `packages/core/tests/test_utils/test_focal_form_lint.py` (9 — the measured FE→DE swap, a correct synthesis, member-form satisfaction, punctuation/case, the clause-shaped and single-token exclusions, the shape predicate, empty input). CHECKS: core+app **968 passed** (1 deselected — the known idempotence property), llm_providers 52; `assemble_prompts render` = 0 written, `check` reports exactly the 6 synthesis statics UNPUBLISHED and the published ids it cites are 200044's, confirming nothing shipped; pyright **0 errors** on `focal_form_lint.py`, `synthesis_dump_util.py`, `test_focal_form_lint.py`; `ruff check` clean (`ruff format` is NOT enforced here — `synthesis_dump_util.py` was already non-conformant at HEAD, so its pre-existing lines were left untouched). NOT DONE ON PURPOSE: the mention-stage location rewrite (changes `ud=` → both stages re-run → would destroy A/B attribution against 200044; it gets its own run) and the seven screening statics + `subject_name` into `create_record_screening_batch_request` (rides with 3.3). EXPECTED NEXT RUN: new `pv` on the six statics → search and mention replay from Mongo, synthesis alone re-runs (`ud=` unchanged) ≈$2.40; watch alecmfg own-name (0 → non-zero is EXPECTED and fine now), the two Allegion records, `focal_form_absent_records`, and whether output length falls now that the read-back check is gone. |
| 2026-08-23/24 | **SEVENTH + EIGHTH RUNS ANALYZED (backfilled row — this session ran ahead of the journal; full detail in the STATE bullets and the evidence READMEs).** SEVENTH — `20260824T002404` vs `200044`, the six synthesis statics as PUBLISHED (evidence `pipeline_v3_evidence/2026-08-24_run_002404_published_synthesis/`): same 95 request ids, every `ud=` identical, six new `pv=`; FE→DE swap read as REPAIRED (later retired by the tenth run), both Allegion records name Steelcraft as publisher + surface 'More from Allegion'; delivery flat, cost $2.42, output −2.6% (the +28% did NOT belong to the read-back check); `own_name_hits` 0 → 3,896 is identification, correct post-ban; `focal_form_absent` 2 → 13 ALL FALSE (lint substring bug). EIGHTH — `20260824T010654` vs `002404`, the document-listing rule (evidence `pipeline_v3_evidence/2026-08-24_run_010654_document_rule/`): unscoped 'provides this resource' 23 → 0, document-scoped claim 1 → 45 (58%); the feared conformity overreach did not materialise (165 records, 0 newly withheld) — the rule STAYS on all six statics, fork closed; delivery flat, $2.43. Lint possessive bug found and fixed same pass (`Steelcraft's` → bare `s` token); uniform-lint series 1/1/1/0. THEN BUILT (not published): the mention-stage Location rewrite, compact-sentence form. Commits `b957c9e` (+ `a274975` recording it). |
| 2026-08-24 | **NINTH RUN ANALYZED + THREE THINGS BUILT (backfilled row; STATE bullet has full detail).** `20260824T012721`, the Location rewrite: 17 OF 20 DUMPS — `alecmfg/industries`, `steelcraft/products`, `steelcraft/contract_products` vanished silently (773 records, 36%), traced to the failed run `20260824T012354` (duplicate `record_id` parse raise; 429 no-credits) + THE REAL DEFECT: the recursive base dispatched only requests it had just CREATED, so `record_response_parse_error`'s nulled rows were never re-asked — **the parse-error retry had never worked for ANY recursive stage** — and `execute` returned silently with the field incomplete. The rewrite itself worked on the 17 shared fields: opener 92.7% → 0.0%, 'whose words' 12% → 100%, locations −24.6%, 0 URLs/cross-refs/empties; ONE regression — 7 records lost a party name incl. the two Allegion records (heading paraphrase), which begat the heading-verbatim fix. BUILT: the heading-verbatim statics change, the resume fix (converge over UNANSWERED requests, `get_incomplete_req_ids`, raise on unproductive passes, `MAX_UNPRODUCTIVE_PASSES = 3`; tests 1 → 5), Defect A left alone on purpose. Suites 1,033 green. Commits `c8d04fb` (+ `167537d`); step 0 added `4c7a1fe`; the user's publish (six Location statics, 02:03:41Z) recorded and step 0 closed in `e32ef0a`. |
| 2026-08-24 | **TENTH RUN ANALYZED (backfilled row; RESUME block + `pipeline_v3_evidence/2026-08-24_run_020729_heading_verbatim/README.md` are the record).** `20260824T020729` = the heading-verbatim fix, and it LANDED: both Allegion records read "the 'More from Allegion' section" again in location AND synthesis; everything the rewrite won held (opener 0%, whose-words 100%, 0 URLs); price +6.9% location text; delivery 2,133/2,133 clean, $5.25. A clean A/B despite a scoped pre-run delete (search replayed from `04:45`; 93 shared mention ids, `ud=` identical). THE FE→DE SWAP RECURRED — retires `002404`'s REPAIRED verdict: right/wrong/right/right/wrong across five observations, stochastic and unfixed; the twins' locations are byte-identical in both runs, and the model emitted the same synthesis string for both. Commit `dad8891`. |
| 2026-08-24 | **DEEP SUPPLEMENT + JOURNAL BACKFILL (this row's commit).** Second pass over `020729` (`deep_supplement.py` + output beside the README; pyright 0, ruff clean): corrected 37 → **93** shared mention request ids and the pairing (242 group_ids straddle both chunks; (subject, field, group_id) keys dropped 114 records and hid **7 real party-name losses — all steelcraft/conformity_attestations, all the mild Falcon page-enumeration class**; full-pairing survival 47% → 66%, Allegion class still fixed). NEW: identical-synthesis collapse census 12 / 23 / 1 / 14 over the last four runs (standing behavior, concentrated in `steelcraft/products`; FE→DE the only wrong-for-one pair; a same-request identical-string tripwire would catch what the focal-form lint cannot — 13 of 14); twin census **130** confusable same-request pairs (93 in `steelcraft/products`); own-name hits 2,154 → 2,477, identification steady; document rule held (19 → 20 scoped, 0 unscoped); mention delivery 3,345/3,345 clean; latency p50 8–10 s p90 ~16 s; paired synthesis +1.9%; thin single-entry records 69% → 70%. Journal rows for the seventh through tenth runs backfilled (the append-only journal had stopped at the sixth); `dad8891` recorded in the Tree line; the four unlisted dump folders accounted for (`170602` pre-v3 smoke; `063104`/`063147`/`063808` deferred-run polls collected by `195031`; `012354` the failed run). MEMORY.md and the v3 memory note reconciled (both still said 'one thing pending' / eight or ten runs with the stale party claim). Next: **3.3**, then the twin-record fix. |
