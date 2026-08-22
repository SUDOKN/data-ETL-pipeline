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

- **Phase:** 2 — pure core, ALL THREE SUBSTEPS BUILT (2.3 landed 2026-08-21
  on the user's go-ahead). **Next: the Phase 2 REVIEW gate** (below), then
  Phase 3.1 (mention node). **RESUME HERE:** the 2.3 module is
  `core/utils/aggregation_fold.py` — its docstring is the design A–E as built;
  [PIPELINE_V3_WALKTHROUGH_2_2.md](PIPELINE_V3_WALKTHROUGH_2_2.md) §6 walks the
  same scenario, which is now the module's golden test. Commits on
  `new-ground-truth-v2`: `ca9a82e` = Phase 1 + 2.1; `e729a81` = 2.2 +
  walkthrough + evidence; `c1209a1` = STATE pointers; 2.3 committed
  2026-08-21 on top (hash recorded at the next commit). **Correction
  (2026-08-21):** the 12 modified statics under `3_phrase_mention_collection/`
  and `4_phrase_synthesis/` were NOT v2-flip work — they are the Phase
  1.2–1.4 amendments (array-shaped mention output per the strict-mode note in
  D6; the amended synthesis lens per D15) that never got committed after
  `ca9a82e`; they ride with the 2.3 commit. Only the `ontology` submodule's
  dirty content is not v3.
- **Phase 2 REVIEW gate — prepared 2026-08-21, awaiting the user.** What to
  read: (1) the property tests of all three substeps
  (`tests/test_utils/test_form_normalizer.py`, `test_floor_scan.py`,
  `test_aggregation_fold.py`: order-independence, normalization edges,
  short-form bridge, anchoring/containment/hold invariants, idempotence);
  (2) the dry grouping run over the appendix corpus —
  `pipeline_v3_evidence/2026-08-21_normalize_dry_run_output.txt` (appendix E:
  4,157 real pairs, 162→189→191→198 groups, zero wrong merges at
  L0/L1/fallback). **Question for the user:** does the fold's rule set (2.3
  bullet below — plan A–E plus three build-time refinements) match what you
  approved, and is the Phase 2 evidence enough to start building the nodes
  (3.1)?
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
| D2 | Search output | FLAT array of verbatim surface forms — every variant incl. casings, fullest-span preference, field-relevance only, NO entity binding, NO dedup by search. Window-local: each window's search covers its own text; payloads are never unioned across windows. **Domain note (2026-08-21, user decision):** URL lines are page separators, not harvest material — "URL lines count as text" dropped from search AND recursive statics. Slug-only forms were architecturally dead in v3 anyway: exact-case matching gives them no body anchor, so they either duplicate a body form's group or ride to an empty bundle. |
| D3 | Recursive search | Repurpose candidate: form completion ("here are the found forms, find variants we missed") replacing v1's find-more-entities round (measured 0/12 in-boundary at highest cost). Per-field toggle; final call at the Phase 5 measurement gate. |
| D4 | Mention collector is LLM | Kept LLM (not code) because mention-EXTENT rules will evolve and prompts are cheaper to evolve than tokenizers. The anaphora argument is measured-weak (see appendix: 0.8% of records lack a string-anchorable mention) — flexibility, not anaphora, is the justification. Contract: sense-agnostic occurrence reporting, stated EXPLICITLY in the prompt ("report occurrences; do not judge whether it's really the material"), exact string + casing matching. |
| D5 | Mention payload | ~~Flat `{record_id, form}` pairs~~ **AMENDED 2026-08-21 (user decision): bare form strings on the wire — no ids sent (see D6)**; window-local, NO payload dedup / variant logic — the LLM is deliberately dumb about variants (all casings ride as separate items; exact-case matching makes each occurrence match exactly one). Batching splits freely along forms (no judgment spans two forms). |
| D6 | Mention output | ~~Keyed by `record_id` (key-masking); mention = `{page, verbatim snippet}`; optional form-echo diagnostic arm~~ **AMENDED 2026-08-21 (user decision): keyed by the FORM STRING itself** — the id→form map is cognition load, and key-masking was built for judgment stages; the collector has no verdict to bias. Mention = `{freehand location, verbatim snippet}`: location is advisory color (stated default preference: page by path or heading + the governing heading/section or structural kind); the authoritative page is CODE-DERIVED at the fold from the snippet's position vs URL lines. Hold policy: a response key matching no sent form is DROPPED and the tier-1 floor scan flags the resulting unaccounted hits (raising would replay a temp-0 mis-echo to death — the alecmfg shape). `record_id = hash(form)` survives per D11, minted at the fold, never on this wire. If Phase 5 shows the echo channel bleeding, the id-keyed variant is the A/B fallback (the old form-echo arm, inverted). **Wire-shape note (1.4, 2026-08-21):** the output is an ARRAY `{"forms": [{form, mentions: [...]}]}`, not an object keyed by form — the response_format is OpenAI strict mode, which forbids objects with arbitrary keys; the form is echoed as a field, the parse rebuilds the map and raises on duplicates. |
| D7 | Mechanical floor scan, two tiers | Tier 1 (HOLD): exact-case word-boundary scan per sent form; every hit must be accounted for in the LLM's mentions or the window flags a discrepancy — the satisficing tripwire, zero GT cost. Tier 2 (DISCOVERY): case-insensitive scan; uncovered casings/near-forms feed the missed-form surface. Word-boundary ALWAYS (substring matching is the measured `Lead` 52-hits-0-real bug); short forms (≤3 chars, e.g. `Al`, `SS`) match case-sensitively or flagged. The standing brute word-boundary TODO graduates to load-bearing core. **Domain note (2026-08-21, follows the URL cut):** both tiers sweep window text MINUS URL lines — the scan domain must match the mention contract or every path hit is a phantom discrepancy. Measured basis (12 runs, 10,661 mention segments): 11 runs produced ZERO URL-occurrence mentions despite the v1 instruction; the one compliant run (20260814T203731) flooded 183 rows of sense-free URL mentions — dead weight either way. |
| D8 | Code re-attribution | Aggregation re-keys EVERY mention from its verbatim snippet (checks which sent forms exactly occur in it); the LLM's attribution is advisory, the fold's is authoritative — model case-sloppiness becomes harmless. The longest-match containment rule lives HERE, in code, not in the prompt (`Sample Lead Time` attributed to sent form `Lead Time`, never also to `Lead`). |
| D9 | Grouping | Pure code, runs at aggregation (AFTER mention collection — user preference: mention batching stays form-based, prompts stay flat/unnested), global scope (all windows, whole document). Normalization-KEY bucketing — groups are equivalence classes of `normalize()`, a dict, not union-find. Union-find machinery only becomes necessary for the future co-listing enhancement. |
| D10 | `normalize()` | L0 (casefold, strip ™®©, unify hyphens/dashes/slashes→space, collapse whitespace) + L1 (per-token plural lemma) baseline for ALL fields; L2 (verb/participle fold) as per-field dial for process/material fields only. Code-token guard: never lemmatize tokens with digits, internal capitals, or OOV-capitalized (protects invented product names; `AccuGrip`/`AccuGrips` stay separate). Real lemmatizer, never naive suffix-stripping (`Brass`→`bras`). Normalizer VERSIONED like a prompt catalog — an upgrade is a loud event. Measured on 9,577 real phrases: zero wrong merges (appendix). |
| D11 | Identity | Per-form `record_id = hash(form)` — mention provenance + mention-GT anchor, cross-run stable. `group_id = hash(normalized key)` — the wire key for everything downstream of aggregation; one key per group by construction, NO leader election, stable across chunks AND runs while grouping stays key-derived. The co-listing enhancement would break the one-key property and reopens the id question — that cost is booked against the enhancement, not v3. |
| D12 | LLM grouper REJECTED | Settled firmly. Blind semantic judgment (strings, no text); its over-merges are the measured M2 world-knowledge instinct ("X is a kind of Y", wrong when confident) landing upstream of every check; nondeterminism churns group_ids/digests and destroys A/B attribution; new GT surface. Escalation path if disjoint-form under-merge ever measures painful: LLM merge-PROPOSER — advisory pairs, logged, applied at concept-level reconcile where a wrong proposal cannot touch synthesis bundles. |
| D13 | Under-merge is the accepted steady state | Redundant groups cost one extra synthesis request and heal at reconcile (both ground to the same concept; screening's one-qualifying-record-suffices resolves split verdicts toward recall). A missed merge cannot cost a result. Every attempt to squeeze redundancy out with smarter form-matching converts cheap failures into expensive ones. |
| D14 | Polysemes | No search-side conditional disambiguation (can't see cross-window conflicts; brute bypasses prompts; mention stage re-merges anyway). A bare polyseme is ONE group with mixed mentions; ~~synthesis dispositions resolve it~~ **AMENDED 2026-08-21 (user decision, with D15): no synthesis dispositions — a mixed group yields a FAITHFUL mixed synthesis, and resolution moves to SCREENING, where field rule catalogs already live (D13's one-qualifying-record-suffices resolves toward recall). Side effect: the over-discount failure mode — dispositions discarding genuine evidence before screening saw it — is gone.** |
| D15 | Synthesis | Deposition-only: mention bundles, never source text (escape hatch: text can be ADDED to its input later if evidence demands). ~~Per-mention DISPOSITION slot; SPLIT-FLAG clause~~ **AMENDED 2026-08-21 (user decision): both REMOVED. Synthesis is a FAITHFUL AGGREGATION of all entries — the court-reporter rule applied to itself: discounting is adding judgment. The model is BLIND to forms and groups: a record is an opaque `record_id` + `entries: [{location, snippet}]`, sent as RECORD_IDS + RECORDS array blocks; output is an array mirroring the input (exactly-once by count). Non-judgment scaffolding kept: the organizing lens, worded as the closed catch-all "whatever the entries show the manufacturer does, has, or is, and whatever is said of it" (action / possession / state + attribution — covers certificates, industries, equipment that a verb list misses); NO convergence nudge (removed at user review — naming a shared subject invites predicting it; convergence is left to the structural fact that every entry contains a form of the group key), repeat-collapse (say once, note recurrence), one deposition sentence (you do not see the site), name-masking in output. Sense burden → screening (D14). The disposition design survives only as a Phase 5 A/B arm if screening measurably struggles with mixed syntheses. Missing-record policy on this wire = the USER-OWNED under-answer policy (same family as grounding's).** Batching: soft cutoff by total mention count; groups NEVER split; a monster group gets its own request; mentions-per-request recorded in dump stats (the satisficing A/B variable). |
| D16 | Downstream re-key | Grounding → OOV → screening → descent → reconcile consume per-group records keyed `group_id`. Upstream-content digests (`|ud=`) extend through the new chain: mention requests digest forms, synthesis digests bundles, grounding digests group records. **Naming note (2026-08-21):** the synthesis wire label `record_id` carries the group_id VALUE; code, dumps and GT keep the name `group_id` — the per-form `record_id = hash(form)` is a different key and must not be conflated. |
| D17 | No text preprocessing | No ASCII fold / character substitution (skipped as not straightforward; the curated-fold idea is shelved unless evidence demands). `ensure_ascii=False` STAYS — reverting was proposed and withdrawn against the measured evidence (escapes caused 17/17 echo corruption; real characters fixed it; on ASCII-clean text the flag is a no-op anyway). Normalize-at-comparison wherever a comparison crosses surfaces. |
| D18 | GT v3 | Mention-level GT is near-mechanical (verbatim fidelity + location), anchored `(record_id, window, mention_index)` — survives any grouping change. **Location note (2026-08-21, follows the D6 amendment):** the auditable location is the fold's code-derived page; the freehand location string is unaudited color. **Synthesis note (2026-08-21, follows the D15 amendment):** the synthesis audit is faithfulness-to-entries only — no disposition audit, no split-flag tally; wrong-merge visibility is the Phase 4.1 dump's member-forms column. The missed-MENTION surface becomes a missed-FORM surface per 5k window (tractable for an annotator; tier-2 scan feeds it). Synthesis audits key on `group_id` (run-scoped addressing accepted; cross-run joins via member-form overlap). Synthesis is auditable AGAINST ITS BUNDLE without opening source text. Annotator budget concentrates on synthesis. Audit primitives (TextFieldAudit/EntityFieldAudit, rule_tree, fold, inflation pattern) carry over. |
| D19 | Same-occurrence dedup | Cross-casing duplicates are impossible by construction (exact-case matching, D5). Containment overlaps resolved by longest-match in code (D8). Residual rule for two forms of one group reporting the same spot: keyed on (group, page, snippet overlap) at aggregation — exact rule OPEN, decide in Phase 2. **SETTLED 2.3 (2026-08-21, user-approved, built):** after re-attribution the unit is the OCCURRENCE — dedup key (group, occurrence span in the window); the longer snippet is kept; among equal snippets the collector's own filing wins, then lexical order (never answer order). `core/utils/aggregation_fold.py`; PIPELINE_V3_WALKTHROUGH_2_2.md §6 C. |
| D20 | Measurement gates | ~~v2's Phase 4 numbers are v3's precondition AND baseline~~ **AMENDED 2026-08-21 (user decision):** the v2 gate is ABANDONED — both subjects crashed inside v2's relationship/grounding at production settings (appendix D), and repairing a stage v3 deletes just to measure it isn't worth it. v3 proceeds at full scope; the Phase 5 gate compares v3 against the v1 hand-audit baselines only (56% boundary precision, 45% context coverage — appendix C). There is no v2 comparison arm. |

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

**Standing watch items inherited from v2:** M2 match rules still owed; descent-
attribution leak channel (measure at gates); search-divisor recall/quality A/Bs
still unrun; **grounding under-answer policy (USER-OWNED, from the gate crash —
appendix D):** a grounding response can validly answer a fraction of its sent
records and today that aborts the whole subject via `MissingResponseRecords`;
decide retry/split/schema policy before v3's grounding re-key (Phase 3.3).

---

## Phases

### Phase 0 — Predecessors (CLOSED 2026-08-21)
- **0.1** V2 flip review sign-off. **DONE 2026-08-21** (chat; three deviations approved).
- **0.2** v2 Phase 4 measurement gate: **ABANDONED 2026-08-21 (user decision).**
  The gate run (alecmfg + steelcraft at factory defaults,
  `max_phrases_per_request=30`) crashed on BOTH subjects inside stages v3
  replaces — no coverage numbers exist and none will be produced. Crash detail in
  appendix D.
- **0.3 Evaluation protocol:** moot for v2, but RETAINED for v3's Phase 5 gate —
  dumps land in `packages/logs/extraction_dumps/<run_id>/` and are readable
  directly; baselines are appendix C; scoring methodology is the hand-audit
  write-up at
  `apps/data_etl_app/src/data_etl_app/services/ground_truth/MATERIAL_CAPS_STAGE_AUDIT_20260816.md`
  (boundary judgment, distinct-context clustering for coverage, per-stage
  attribution). **The USER must supply the arm mapping** — per run id: subject,
  batching knobs, OOV on/off, model/temp/seed if non-default (usual: gpt-4.1,
  temp 0, seed 12345) — plus any notebook-side errors/aborts; dumps do not
  reliably self-describe these knobs.
- **REVIEW gate:** resolved by decision, not numbers — v3 proceeds at full scope
  against the v1 baselines (D20 amended).

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
- **3.2** Synthesis node: soft-cutoff batching, groups whole, dispositions parse.
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

### D. v2 Phase-4 gate crash (run 20260822T012722, 2026-08-21) — why the gate was abandoned
Sweep of alecmfg + steelcraft at factory defaults (gpt-4.1, temp 0, seed 12345;
relationship AND grounding group size 30 —
`ExtractionPipelineFactory.DEFAULT_*_PER_REQUEST`). Both subjects FAILED; no
coverage numbers exist.
- **steelcraft — repetition loop → truncated JSON.** Relationship request
  `group>5>chunk>0:97071` (candidates incl. `borrowed lights`, `configured
  Doors`, `doors and frames`): the model locked into emitting one identical
  mention block ({form, page, account} — same page, same boilerplate account
  sentence) over and over until it spent exactly 20,000 completion tokens
  (`finish_reason='length'`); JSON cut mid-string at line 842 → pydantic
  `json_invalid` → ValueError at `parse_llm_phrase_relationship_records`. Same
  failure family as the 2026-08-16 search repetition loops; the relationship
  stage has no truncation salvage, and salvage is structurally blocked by the
  all-records-must-return contract. Root read: ~21k-token prompt (97KB macro
  window), output demand proportional to every mention of 30 phrases — v2 is
  unbounded on the output side; v3's 5k-window mention collector bounds it
  (D1/D5), and D7's floor scan catches degeneration mechanically.
- **alecmfg — grounding under-answer.** Freehand grounding `group>1>chunk>0:98612`
  answered ONLY the first record (`r59v4t75`) of 30 sent — clean JSON,
  `finish_reason='stop'`, 244 completion tokens; `hold_response_to_sent_record_ids`
  raised `MissingResponseRecords` and killed the subject. Under-generation, NOT
  truncation. **This failure mode SURVIVES the pivot** (v3 grounding is similar):
  the wire schema doesn't force one entry per sent record. Policy owed —
  USER-OWNED, see standing watch items.
- **Sub-URL hypothesis (user, thinking aloud):** that demanding page paths causes
  the loops. The crash evidence points elsewhere — the loop's repeated unit was
  dominated by the long verbatim `account` text and repeated the SAME page. Note
  v3's D6 mention output keeps `page`, so if the hypothesis were right v3 would
  inherit the problem; testable as a cheap Phase 5 A/B.
- **Replay hazard:** both bad responses are RECORDED in the batch-request store —
  resuming these runs replays the stored responses and fails identically; delete
  the stored requests first if these subjects are rerun on v2.
- Artifacts: full log (transient, overwritten per run) at
  `apps/data_etl_app/src/data_etl_app/scripts/latest_extraction_error.txt`; dump
  dir `packages/logs/extraction_dumps/20260822T012722/` holds only steelcraft's
  five single-stage fields (both runs died mid-products, before trail dumps).

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
