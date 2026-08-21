# Pipeline v2 — master plan & journal

**Branch `new-ground-truth-v2` · this file is the single source of truth for progress.**
The user's proposal is [pipeline_v2.txt](pipeline_v2.txt); this README is the plan, the
fork ledger, and the append-only journal. A fresh session resumes by reading: (1) the
STATE block, (2) the fork ledger, (3) the phase it says is next.

**The inversion:** v1 screens one identified entity per phrase, then grounding fans out
to tags that never face relationship scrutiny. v2 grounds first — candidate
enumeration under candidate-only rules — then consolidated screening vets EVERY
candidate's relationship, and recursive descent (post-screening, in-vocab survivors)
is not re-screened. Rule-catalog locked decisions #19/#20 anticipated exactly this
("entity section deletable as a unit if screening ever carries all qualifying
entities forward").

---

## STATE

- **Phase:** 0 — contracts on paper. 0.1 + 0.2 DONE (user-approved 2026-08-20).
  **0.3 industry pattern BUILT as INERT DRAFTS (2026-08-20), awaiting user
  review:** 4 v2 catalogs in `knowledge/prompts/rule_catalog_v2/` + 4 v2
  skeletons in `knowledge/prompts/skeletons_v2/` (neither dir is globbed by the
  assembler), rendered previews in `final_texts/assembled_v2/` (gitignored)
  via a scratchpad harness carrying the draft v2 example builders. Suite 1146
  green untouched.
- **COUPLING DISCOVERY (2026-08-20), reshapes 0.3/0.4 cutover:**
  `test_applied_rule_parse_validation.py` + `test_prompt_assembly_service.py`
  bind catalog content, wire builders, and parse validation together (SCR-1
  slots, no_candidate branch, sentinel assertions, parametrized over live
  catalogs), and skeletons are per-stage so a stage's catalogs move atomically
  across all its fields. In-place catalog edits ahead of the 0.4 builders would
  break dozens of tests twice. **Revised strategy: catalogs/skeletons live as
  `_v2` drafts through Phase 0; cutover happens per-stage as vertical slices
  (catalog set + skeleton + example builder + wire builder + parse validation +
  that stage's tests together), merged into Phase 2's node-service rewrites.**
- **FAN-OUT BUILT (2026-08-20), awaiting user review:** 17 more draft catalogs
  (concepts: screening/IGR/OOV/RGR ×3 fields; keywords: screening ×3 +
  freehand-v2 ×2) + freehand v2 skeleton; all 21 previews rendered. Judgment
  calls flagged in the journal row: product freehand MERGED (one catalog, both
  product fields — attribution was the only difference); equipment's
  kind-guard moved from screening into grounding as FGR-Q1; equipment FGR-QC1
  removed (F17 extended beyond RGR); cross-screen notes (old 4c/3c) dropped
  per F15; keyword grounding wire unit = "candidate".
- **0.4 BUILT (2026-08-20), awaiting user review — PHASE 0 COMPLETE pending
  that review.** v2 wire builders in `catalog_wire_schema.py`
  (`build_screening_response_model_v2`, `build_record_grounding_response_model`,
  `response_model_for_v2` with its own cache, `STAGE_OOV_GROUNDING` constant,
  reserved names + record_id/candidate/candidates); v2 example builders in
  `prompt_assembly_service.py` (`EXAMPLE_BUILDER_BY_STAGE_V2`, unwired);
  49 tests in `test_v2_wire_schema.py` (strict-mode + example-decodes for all
  21 drafts, rejection shapes, flatten round-trips, cache separation).
  Previews re-rendered: populated grounding entries now show
  `"explanation": null` (strict mode requires every property; the example must
  teach the decodable shape). Suite 1195; pyright 0 (incl. one pre-existing
  binary-builder return-type fix).
- **PHASE 1 BUILT (2026-08-20), awaiting user review.** 1.1
  `core/utils/record_id_util.py` (pure `r`+7-base36 sha256, collision →
  `RecordIdCollisionError` naming both phrases, `assign_record_ids` /
  `mask_relationship_records` / `phrases_by_record_id`). 1.2
  `RecordToTagAndRulesMap` (grounding.py) + `CandidateScreeningVerdict` /
  `RecordScreeningResults` (screening.py). 1.3
  `llm_phrase_extraction_results_v2.py` — v2 stats/metadata standalone (shared
  stage fields changed type, no v1 base left to share); keywords on
  `ConceptsFound`; ONE stats-map name (`chunked_extraction_stats`) for both
  families; `llm_phrase_oov_grounding: Optional[...]` = run-config-as-identity
  (F6). **F12 moved to 2.9** — the digest rides request-id construction, which
  knows upstream results; node metadata is fixed before they exist. 1.4
  `PipelineStage.oov_grounding` member + rank; its request-id TOKEN arrives
  WITH the v2 node in phase 2 (the tripwire holds every token to a real
  builder). Suite 1207; pyright 0.
- **PHASE 2 STRATEGY (2026-08-20):** v2 node services land as SIBLINGS of v1
  (`*_v2.py`), mirroring v1 signatures for a drop-in flip; the single 2.9 flip
  re-points the nodes, deletes v1 paths, and migrates their tests — every v2
  path already unit-tested by then. Keeps the suite green at every substep,
  which per-stage in-place cutover could not (downstream v1 tests consume
  upstream types).
- **2.1 + 2.2 BUILT (2026-08-20), awaiting user review.** 2.2: record blocks
  in `phrase_blocks_contract.py` — `<<<RECORD_IDS` (one-line array) +
  `<<<RECORDS` (multi-line map, one top-level entry per line, lazy fenced
  reader; JSON-escaping makes fence forgery from record content impossible),
  rendered from one map, reader raises on divergence;
  `hold_response_to_sent_record_ids` is EXACT (no reconciler — an unsent id is
  fabrication and raises even under `drop`). 2.1:
  `llm_phrase_relationship_node_service_v2.py` — records parse (wire→stored),
  same phrase-axis hold as v1 (identity set here, masking AFTER the hold),
  `get_masked_phrase_relationship_result` (merge → mask),
  `records_with_mentions` (F9 filter; stored stats keep the empty records),
  v2 response schema strict-asserted, dummy = `{"records": []}`. 17 tests;
  suite 1224; pyright 0.
- **2.3–2.5 BUILT (2026-08-20), awaiting user review.** ONE shared service
  (`llm_grounding_node_service_v2.py`) for in-vocab/OOV/freehand — the stages
  differ only in what rides beside the records and whether labels are
  vocabulary-held. Parse: record-keyed wire → `RecordGroundingResults`;
  in-vocab membership per F4 (non-vocab label fails the response; casing drift
  repaired to the vocabulary's own spelling + warned); OOV/freehand labels
  minted (vocab re-route is reconcile's job); silent empty records fail
  ("must say why"); volunteered explanations beside units dropped with a
  warning; violations collected whole-response then raised once. **Model
  amendment (deviation from the sketch, flagged):** stored grounding entries
  are `RecordGroundingEntry {tags, explanation}` with a correlation validator
  — the sketch's bare `{record_id: {tag: rules}}` would have LOST declination
  explanations at storage, reopening the no-recorded-reason hole
  `no_candidate_explanation` closed in v1. Request side:
  `build_record_payloads` (mask holds — phrase never rides;
  `already_identified` sorted for the OOV pass), one generic creator taking a
  pre-rendered `options_section` (None = freehand), dummy =
  `{"groundings": []}` with empty blocks. 9 tests; suite 1233; pyright 0.
- **2.6 + 2.7/2.8 CORES BUILT (2026-08-20), awaiting user review.** 2.6:
  `llm_screening_node_service_v2.py` — per-candidate parse (`passed` derived
  by `passed_implied_by`, never reported), BOTH axes held exactly (id axis =
  shared record hold; candidate axis = `hold_candidates_to_sent_records`
  against the request's own records payload — unjudged candidate raises,
  fabricated verdict raises), deposition-only creator (records + candidates,
  never chunk text, F8), payload builder rejects candidates for unknown
  records. 2.7/2.8 pure halves: `pipeline_v2_derivations.py` —
  `candidates_for_screening` (in-vocab ∪ OOV, casefold-deduped preferring the
  in-vocab spelling), `passed_candidates_by_record`,
  `descent_seed_tagging_results` (passed in-vocab tags grouped tag-major, the
  shape the descent machinery consumes, record ids where phrases were),
  `candidates_that_passed` (one qualifying record suffices, as v1). 8 tests;
  suite 1241; pyright 0.
- **Next action:** user reviews; then 2.9 THE FLIP — node classes re-point to
  v2 services, bundles/embedding (incl. F12 upstream-content digest), the
  descent loop + reconcile nodes on the derivations, v2 orchestration order
  (search → relationship → in-vocab → OOV → screening → recursive), v1 paths
  deleted, v1 tests migrated, the `_v2` catalogs/skeletons/builders promoted
  to canonical, `oov_grounding` request-id token + node. Biggest remaining
  chunk; then Phase 3 observability and the Phase 4 measurement gate.
- **Blockers:** none.

*Update this block at every substep completion. Journal rows are append-only, at the
bottom of this file.*

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

## Fork ledger (settled 2026-08-20 — do not re-litigate without asking)

| # | Fork | Decision |
|---|------|----------|
| F1 | record_id format | Content-derived: `r` + lowercase base36 of sha256(phrase), collision → loud raise. NOT positional (positional + the stale-replay hazard = silent misattribution). Derived purely — no stored minting step; the stored `{record_id, phrase, record}` triple in relationship stats is the join. Gives cross-chunk phrase identity free. |
| F2 | Masking scope | KEY-masking only. Mention `form`s necessarily carry the phrase wording; the id replaces the output grouping key, nothing more. |
| F3 | Sentinel | Retired in v2 stages. Hold-to-sent-record-ids makes an empty `candidates` array an explicit declination; the empty branch carries an optional `explanation` slot (lesson of `no_candidate_explanation`). |
| F4 | In-vocab grounding contract | No escape hatch: vocab or nothing → hard membership check at parse, any non-vocab candidate raises and re-dispatches. Closes the fake-OOV option-axis hole for concepts. |
| F5 | OOV pass | Context = full vocab + pass-1 results (arm 1; the results-only arm is invalid — "outside pass-1 results" ≠ "outside vocab"). OOV candidates matching a vocab label re-route in-vocab, never raise. Serial after in-vocab for concepts. |
| F6 | OOV toggle | Run config + optional metadata node. NEVER a StageToggle (those are locked hard-stop; bypass semantics was declined). |
| F7 | Grounding rules | Attribution clauses removed ONLY; match strictness kept. v2 does NOT fix the M2 hypernym leak — screening inherits grounding's match frame by design; no match-sanity rule in screening. |
| F8 | Screening evidence | Deposition-only: records, never chunk text. Per-candidate rule slots (wire ≈ the option-grounding shape). SCR-1, `no_candidate` branch, `identified_entity` all dissolve. Held on BOTH axes: record ids and the supplied candidate set per record. |
| F9 | Empty-mentions records | Relationship's honest not-found branch skips grounding AND screening code-side, recorded as such. |
| F10 | Results semantics | Concept `in_vocab` = passed screening then deepened by descent (post-recursive); `out_of_vocab` = passed OOV candidates. Keywords adopt uniform `ConceptsFound` with `in_vocab` empty by construction. Descent entry: records with ≥1 passed in-vocab candidate. |
| F11 | GT | Pivot now — the v1 P3 gate run is abandoned; GT restarts against v2 shapes after the Phase 4 measurement gate. Audits relationship at synthesis AND per-mention, addressed `(record_id, mention_index)` with array order locked, plus a NEW missed-mention surface per record. Audit primitives carry over; stage blocks + run identity rebuild. |
| F12 | custom_id | v2's clean break folds in the upstream-content-digest fix for the stale-replay hazard. |
| F13 | Prompt assembly | Placeholders stripped from the relationship static; context-append kept (existing order already matches: name → scraped text → phrases fence, `llm_phrase_relationship_node_service.py`). |
| F14 | Mention key | The mention's text field is `account` (not `record` — overloaded with the phrase-level record; not `note` — collides with `TextFieldAudit.note`). Mention shape: `{form, page, account}`. |
| F15 | No downstream info in prompts (2026-08-20) | Prompts never describe what later stages do with the output. Releases are stated as bare job facts ("Whether it is the manufacturer itself or some other party … plays no part."), never justified by "a later stage screens…". Upstream input provenance ("the evidence a previous pass collected", "already identified from it") stays — it describes what the input IS. |
| F16 | Rule ids renumber from 1 (2026-08-20) | v2 catalogs number from 1 per prefix group (SCR-1/SCR-2/SCR-G1; RGR-E1/M1–M3); no id continuity with v1 (v1 SCR-2 ≡ v2 SCR-1). Joins go through catalog_version, but no tooling may assume cross-version id identity. |
| F17 | RGR granularity self-report removed (2026-08-20) | The over/under-descended `quality` kind is gone from the model-facing recursive catalog — granularity is the annotator's judgment in GT v2. Wire loses the slot; GT v2 drops the "quality filled" requirement from fresh derivations. |

**Standing watch items:** M2 match rules still owed; brute word-boundary TODO
(`brute_search_service.py`); mention-satisficing measurement (the ~8 cap is
permission, not pressure — test relationship `max_phrases_per_request` 20–30);
search-divisor recall/quality A/Bs still unrun; **descent-attribution leak
channel** (v2 drops RGR-Q1, so a record mixing manufacturer-attributed general
evidence with third-party-attributed specific evidence can descend a specific
the manufacturer does not own — accepted per the proposal, measure in Phase 4);
**`OUTCOME_GLOSS_BY_KIND`'s not_triggered example** ("when no {{entity_noun}}
was identified") is stale for v2 stages — reword to the chain-based example at
cutover.

---

## Phase 0 — Contracts on paper (prompts, catalogs, wire)

- **0.1 Prompt statics.** Copy-edit the three handwritten industry drafts (typos,
  dash consistency, `record`→`account` mention key, JSON example trailing comma,
  page-not-visible instruction, strip placeholders, trailing newline). Propagate
  industry → the other five per stage, md5-verified for relationship (byte-identical
  across fields); search/recursive get the same common-block edits with
  field-specific sections untouched.
- **0.2 Relationship wire + stored types.** `records: [{phrase, mentions: [{form,
  page, account}], synthesis}]`; stored phrase→record map + masked
  `{record_id → {phrase, record}}` variant.
- **0.3 Catalog rework.** Screening ×7 (SCR-1 + no-candidate deleted; conditions =
  relationship-is-real + is-the-manufacturer's; guards kept). Initial grounding:
  attribution out, match rules untouched, sentinel dropped. New OOV catalogs
  (catalogs are 1:1 with prompt names). Keyword grounding: freehand minus
  attribution, multi-candidate, no sentinel. Recursive: same treatment as initial.
- **0.4 Wire builders.** `catalog_wire_schema` extensions: id-keyed entries,
  empty-units-with-explanation, per-candidate screening. Pure-function-of-catalog
  tripwire stays.
- **REVIEW gate:** user signs off rendered prompts + catalog diffs before models.

## Phase 1 — Core models

- **1.1** record_id util (pure, collision-raise; determinism + cross-chunk tests).
- **1.2** Extraction schemas: masked relationship map, `RecordToTagAndRulesMap`,
  per-candidate screening verdicts, keywords → `ConceptsFound`.
- **1.3** Stats/metadata: grounding split `{in_vocab, out_of_vocab}` (OOV node
  optional), two grounding node metadatas for concepts / one for keywords,
  upstream-content digest into `custom_id` (F12).
- **1.4** `PipelineStage` / `StageToggles` updates + the ast tripwire test.
- **REVIEW gate:** model shapes vs pipeline_v2.txt sketch.

## Phase 2 — Node services + orchestration

- **2.1** Relationship node: new parse, masking at the parse boundary,
  empty-mentions classification (F9).
- **2.2** `phrase_blocks_contract` v2: render/hold by record_id, two-block structure
  kept, bijection tests rebuilt.
- **2.3** In-vocab grounding node: all non-empty records in, hard membership raise,
  empty candidates allowed.
- **2.4** OOV grounding node (new): serial after in-vocab; vocab + pass-1 results
  context; label-matching candidates re-route in-vocab.
- **2.5** Keyword grounding node: single-shot, multi-candidate.
- **2.6** Screening node v2: records with ≥1 candidate (in-vocab ∪ OOV),
  per-candidate judgments, both axes held.
- **2.7** Recursive grounding: entry per F10, concept-grouped descent, record_id
  keyed.
- **2.8** Reconcile: concept + keyword result assembly per F10;
  `dedupe_equivalent_keywords` moves to candidates.
- **2.9** Orchestration/prefill/embed: new DAG order, group-count derivations,
  dummies, resume guards.
- **REVIEW gate:** end-to-end dry run on a small subject.

## Phase 3 — Observability

- **3.1** Dumps + trail rows join via the stored triple; never-asked vs
  asked-found-nothing stays distinguishable; partial-run dump updated.
- **3.2** Subject-name lint over `form`, `account`, and synthesis.
- **REVIEW gate:** user reads a dump.

## Phase 4 — Measurement gate (before GT)

Run v2 on alecmfg + steelcraft via the user's notebook; compare against v1 baselines
(material_caps audit: 56% boundary precision, 45% context coverage, per-stage
verdicts). Test relationship `max_phrases_per_request` 20–30; OOV on vs off. If v2
loses to v1, iterate here — GT waits.

## Phase 5 — GT rebuild

Stage blocks v2 (structured relationship: synthesis + per-mention audits at
`(record_id, mention_index)`, missed-mention surface; per-candidate screening; split
grounding), run identity v2, inflation, submission validation, fold, routes, fresh
API test plan. Audit primitives (TextFieldAudit/EntityFieldAudit, rule_tree, fold,
inflation pattern) carry over unchanged.

**Deletions v2 earns:** sentinel machinery in these stages, `llm_declined`
special-casing (v2 GT), `no_candidate` branch, `identified_entity`.

---

## Journal (append-only)

| Date | Row |
|---|---|
| 2026-08-20 | Plan approved in chat after full design review; fork ledger F1–F14 locked; this file created. Phase 0.1 started. |
| 2026-08-20 | 0.1 built: industry statics copy-edited (dedup typo/dashes, mention key `record`→`account` per F14, page-unknown fallback, JSON example comma, placeholders stripped per F13 — context-append order already matches); propagated to all six files per stage, relationship md5-verified identical. Recursive-Task harmonization question left for the user. |
| 2026-08-20 | 0.1 user-approved; recursive-Task verbatim-span sentence propagated to all six recursive files. 0.2 built: wire `PhraseMentionEntry`/`PhraseRecordEntry`/`PhraseRecordsResponse` (extra=forbid, field order = generation order, empty mentions = the not-found branch) + stored `PhraseMention`/`PhraseRelationshipRecord`/`MaskedPhraseRelationshipRecord`, aliases `LLMPhraseRelationshipRecords` (phrase→record) and `MaskedLLMPhraseRelationshipResults` (record_id→{phrase, record}). Wire schema asserted strict-mode-supported (depth 3). 8 tests; suite 1146; pyright 0. |
| 2026-08-20 | 2.6 + 2.7/2.8 cores built. Screening v2 parses to per-candidate verdicts with derived `passed`; the candidate axis is held to the request document itself (sent_records payload), so "asked about" and "validated against" cannot diverge; candidate echo is exact-match (supplied strings; drift = corruption; repair would be silent misattribution). Derivations are pure functions over stored v2 shapes — descent seed reuses v1's TaggingResult tag-major shape so the descent machinery ports rather than rewrites; OOV-restating-vocab mints fold into the canonical label BEFORE screening so one concept gets one verdict. Remaining for 2.9: request-loop/embedding wiring, descent loop, reconcile nodes, orchestration order, v1 deletion + test migration, catalog/skeleton/builder promotion. 8 tests; suite 1241; pyright 0. |
| 2026-08-20 | 2.3–2.5 built as ONE shared grounding service (drift-proofing the three stages' contracts). RecordGroundingEntry {tags, explanation} replaces the sketch's bare map so declination reasons survive storage; validator pins tags-empty ⇔ explanation-present. In-vocab membership closes the fake-OOV hole at the decoder's altitude; two-pass parse (validate whole response, then construct) so a defective record can't trip the storage validator before the real error reports. Per-stage wiring (catalogs, allowed_labels, OOV's serial dependency, outline rendering, bundle req-id fields) deferred to the flip. 9 tests; suite 1233; pyright 0. |
| 2026-08-20 | 2.1+2.2 built under the siblings-then-flip strategy. Record blocks: ids fence one-line, records fence multi-line with a DOTALL-lazy reader — safe because record newlines are JSON-escaped so payload physical lines are exactly what json.dumps wrote; two-blocks-disagree raises as malformed-request. Exact hold: unsent response id raises under BOTH policies (fabrication, not echo drift); missing ids raise/thin per caller. Relationship v2: phrase fence and hold unchanged (identity is set here), mask applied after merge, `records_with_mentions` shapes downstream input while stats keep the not-found records (F9). 17 tests; suite 1224; pyright 0. |
| 2026-08-20 | Phase 1 built: record_id util (content-derived per F1; collision raise carries the raise-the-length remedy; duplicate inputs collapse, distinct-phrase collisions raise), v2 schemas (RecordToTagAndRulesMap, CandidateScreeningVerdict/RecordScreeningResults — no identified_entity, no no-candidate), v2 stats/metadata module (InitialGroundingStats in_vocab/out_of_vocab rounds; masked relationship rounds carry the id→phrase join; unified `chunked_extraction_stats` name — the v1 concept/keyword split bought nothing), PipelineStage.oov_grounding (rank in the v1-order table; reorder + token land with phase 2). F12 digest explicitly relocated to 2.9. 12 tests; suite 1207; pyright 0. |
| 2026-08-20 | 0.4 built: v2 wire schemas are record-keyed with structural declination (empty units + required-nullable entry `explanation` replaces the sentinel arm; screening = per-candidate units under `{record_id, candidates}`, no union, no `identified_entity`). One parametrized builder serves in-vocab/recursive (`option`/`options`) and OOV/freehand (`candidate`/`candidates`). Separate v2 cache so the two dispatchers can never trade shapes. v2 example builders moved into the repo (harness now imports them); examples show `"explanation": null` on populated entries — caught by the example-decodes-under-schema test before it shipped wrong. 49 tests; suite 1195; pyright 0. |
| 2026-08-20 | Fan-out built: 17 draft catalogs + freehand v2 skeleton, 21 previews rendered, suite 1146 green untouched. SCR-1a relocation map: material determinability / process operation-on-work / conformity name-number-body notes moved into IGR+OOV as E1b evidence notes; conformity's displayed-cert note stays in screening as SCR-1a; product "stated, not inferred" became merged-freehand E1b. Equipment: v1 kind guard G2(+notes) became grounding condition FGR-Q1(+notes); screening keeps sells-not-uses (G1) + aspirational (G2); access-mode note rides SCR-2b. Product freehand MERGED into `product_phrase_freehand_grounding` (field_types [products, contract_products], neutral rels "makes or works on") — resurrects the pre-split shared prompt; pure-vs-contract lives only in screening. Keyword grounding = conditions only (naming ladder M1/M2 + sentinel deleted); FGR-QC1 depth report removed (F17 extended), Q4's QC1 cross-reference sentence dropped. v1 industry-flavored 2b note NOT propagated (supplying-into-it logic is industry-only); anonymization note propagated to all 7 screening catalogs. |
| 2026-08-20 | User review of the industry pattern → three notes, locked as F15–F17: downstream-stage info scrubbed from all four skeletons + both grounding E1a notes (user's own preview edit was canonical — sources now reproduce it byte-for-byte); screening renumbered SCR-1/SCR-2(+2a/2b)/SCR-G1 with the cross-reference updated; RGR quality section + vocab deleted, RGR-Q2 renamed RGR-E1. Previews re-rendered; suite 1146 green. |
| 2026-08-20 | 0.2 user-approved. 0.3 industry pattern built as inert drafts after the coupling discovery (see STATE): screening v2 (SCR-1 + no_candidate gone, per-candidate judgment, SCR-2/SCR-3/SCR-G2 kept with record wording), IGR v2 (attribution → IGR-E1 evidence condition + E1a release note, ladder cut to M1/M2, sentinel gone, unmatched-candidates-unrecorded moved to skeleton prose), NEW OOV catalog (OGR-E1/K1/N1 conditions, "candidate" wire unit, stage `phrase_oov_grounding`), RGR v2 (RGR-Q1 attribution dropped — watch item; Q2/M1–M3/QC1 kept, M4 sentinel gone). 4 skeletons rewritten around records/mentions/synthesis with the relationship-release paragraph; previews rendered via scratch harness. Suite 1146 green untouched. |
